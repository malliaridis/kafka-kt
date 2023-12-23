/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.coordinator

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.TreeMap
import java.util.TreeSet
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.common.utils.Utils.mkString
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Node.Util.getTrogdorAgentPort
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.rest.RequestConflictException
import org.apache.kafka.trogdor.rest.TaskDone
import org.apache.kafka.trogdor.rest.TaskPending
import org.apache.kafka.trogdor.rest.TaskRequest
import org.apache.kafka.trogdor.rest.TaskRunning
import org.apache.kafka.trogdor.rest.TaskState
import org.apache.kafka.trogdor.rest.TaskStateType
import org.apache.kafka.trogdor.rest.TaskStopping
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.TasksResponse
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerReceiving
import org.apache.kafka.trogdor.rest.WorkerState
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.LoggerFactory
import kotlin.math.max

/**
 * The TaskManager is responsible for managing tasks inside the Trogdor coordinator.
 *
 * The task manager has a single thread, managed by the executor. We start, stop, and handle state changes
 * to tasks by adding requests to the executor queue. Because the executor is single threaded, no locks are needed
 * when accessing `TaskManager` data structures.
 *
 * The `TaskManager` maintains a state machine for each task. Tasks begin in the `PENDING` state,
 * waiting for their designated start time to arrive. When their time arrives, they transition to the `RUNNING` state.
 * In this state, the [NodeManager] will start them, and monitor them.
 *
 * The TaskManager does not handle communication with the agents. This is handled by the `NodeManager`s.
 * There is one NodeManager per node being managed. See [org.apache.kafka.trogdor.coordinator.NodeManager] for details.
 *
 * @property platform The platform.
 * @property scheduler The scheduler to use for this coordinator.
 * @property nextWorkerId The ID to use for the next worker. Only accessed by the state change thread.
 */
class TaskManager internal constructor(
    private val platform: Platform,
    private val scheduler: Scheduler,
    private var nextWorkerId: Long,
) {

    /**
     * The clock to use for this coordinator.
     */
    private val time: Time = scheduler.time()

    /**
     * A map of task IDs to Task objects.
     */
    private val tasks = mutableMapOf<String, ManagedTask>()

    /**
     * The executor used for handling Task state changes.
     */
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        createThreadFactory("TaskManagerStateThread", false)
    )

    /**
     * Maps node names to node managers.
     */
    private val nodeManagers = mutableMapOf<String, NodeManager>()

    /**
     * The states of all workers.
     */
    private val workerStates: MutableMap<Long, WorkerState> = HashMap()

    /**
     * `true` if the `TaskManager` is shut down.
     */
    private val shutdown = AtomicBoolean(false)

    init {
        for (node in platform.topology().nodes().values) {
            if (getTrogdorAgentPort(node) > 0)
                nodeManagers[node.name()] = NodeManager(node, this)
        }
        log.info("Created TaskManager for agent(s) on: {}", nodeManagers.keys.joinToString())
    }

    /**
     * @property id The task id.
     * @property originalSpec The original task specification as submitted when the task was created.
     * @property spec The effective task specification. The start time will be adjusted to reflect the time
     * when the task was submitted.
     * @property controller The task controller.
     * @property state The task state.
     */
    internal inner class ManagedTask(
        val id: String,
        val originalSpec: TaskSpec,
        val spec: TaskSpec,
        private val controller: TaskController?,
        var state: TaskStateType,
    ) {

        /**
         * The time when the task was started, or -1 if the task has not been started.
         */
        var startedMs: Long = -1

        /**
         * The time when the task was finished, or -1 if the task has not been finished.
         */
        var doneMs: Long = -1

        /**
         * True if the task was cancelled by a stop request.
         */
        var cancelled = false

        /**
         * If there is a task start scheduled, this is a future which can be used to cancel it.
         */
        var startFuture: Future<*>? = null

        /**
         * Maps node names to worker IDs.
         */
        var workerIds = TreeMap<String, Long>()

        /**
         * If this is non-empty, a message describing how this task failed.
         */
        var error = ""

        fun clearStartFuture() {
            startFuture?.let { future ->
                future.cancel(false)
                startFuture = null
            }
        }

        fun startDelayMs(now: Long): Long {
            return if (now > spec.startMs()) 0
            else spec.startMs() - now
        }

        fun findNodeNames(): TreeSet<String> {
            val nodeNames = controller!!.targetNodes(platform.topology())
            val validNodeNames = TreeSet<String>()
            val nonExistentNodeNames = TreeSet<String>()
            for (nodeName in nodeNames) {
                if (nodeManagers.containsKey(nodeName)) validNodeNames.add(nodeName)
                else nonExistentNodeNames.add(nodeName)
            }
            if (!nonExistentNodeNames.isEmpty())
                throw KafkaException("Unknown node names: ${nonExistentNodeNames.joinToString()}")
            if (validNodeNames.isEmpty()) throw KafkaException("No node names specified.")
            
            return validNodeNames
        }

        fun maybeSetError(newError: String) {
            if (error.isEmpty()) error = newError
        }

        fun taskState(): TaskState = when (state) {
            TaskStateType.PENDING -> TaskPending(spec)
            TaskStateType.RUNNING -> TaskRunning(spec, startedMs, getCombinedStatus())
            TaskStateType.STOPPING -> TaskStopping(spec, startedMs, getCombinedStatus())
            TaskStateType.DONE -> TaskDone(spec, startedMs, doneMs, error, cancelled, getCombinedStatus())
        }

        private fun getCombinedStatus(): JsonNode? {
            return if (workerIds.size == 1) workerStates[workerIds.values.first()]!!.status()
            else {
                val objectNode = ObjectNode(JsonNodeFactory.instance)
                for ((nodeName, workerId) in workerIds) {
                    val state = workerStates[workerId]
                    val node = state!!.status()
                    if (node != null) objectNode.set<JsonNode>(nodeName, node)
                }
                objectNode
            }
        }

        fun activeWorkerIds(): TreeMap<String, Long> {
            val activeWorkerIds = TreeMap<String, Long>()
            for ((nodeName, workerId) in workerIds) {
                val workerState = workerStates[workerId]
                if (!workerState!!.done()) activeWorkerIds[nodeName] = workerId
            }
            return activeWorkerIds
        }
    }

    /**
     * Create a task.
     *
     * @param id The ID of the task to create.
     * @param spec The specification of the task to create.
     * @throws RequestConflictException if a task with the same ID but different spec exists
     */
    @Throws(Throwable::class)
    fun createTask(id: String, spec: TaskSpec) {
        try {
            executor.submit(CreateTask(id, spec)).get()
        } catch (exception: ExecutionException) {
            log.info("createTask(id={}, spec={}) error", id, spec, exception)
            throw exception.cause!!
        } catch (exception: JsonProcessingException) {
            log.info("createTask(id={}, spec={}) error", id, spec, exception)
            throw exception.cause!!
        }
    }

    /**
     * Handles a request to create a new task. Processed by the state change thread.
     */
    internal inner class CreateTask(
        private val id: String,
        private val originalSpec: TaskSpec,
    ) : Callable<Unit> {
        
        private val spec: TaskSpec

        init {
            val node = JsonUtil.JSON_SERDE.valueToTree<ObjectNode>(originalSpec)
            node.set<JsonNode>("startMs", LongNode(time.milliseconds().coerceAtMost(originalSpec.startMs())))
            spec = JsonUtil.JSON_SERDE.treeToValue(node, TaskSpec::class.java)
        }

        @Throws(Exception::class)
        override fun call() {
            if (id.isEmpty()) throw InvalidRequestException("Invalid empty ID in createTask request.")
            
            var task = tasks[id]
            if (task != null) {
                if (task.originalSpec != originalSpec) throw RequestConflictException(
                    "Task ID $id already exists, and has a different spec ${task.originalSpec}"
                )
                log.info("Task {} already exists with spec {}", id, originalSpec)
                return
            }
            var controller: TaskController? = null
            var failure: String? = null
            try {
                controller = originalSpec.newController(id)
            } catch (throwable: Throwable) {
                failure = "Failed to create TaskController: " + throwable.message
            }
            if (failure != null) {
                log.info("Failed to create a new task {} with spec {}: {}", id, originalSpec, failure)
                task = ManagedTask(
                    id = id,
                    originalSpec = originalSpec,
                    spec = originalSpec,
                    controller = null,
                    state = TaskStateType.DONE,
                )
                task.doneMs = time.milliseconds()
                task.maybeSetError(failure)
                tasks[id] = task
                return
            }
            task = ManagedTask(id, originalSpec, originalSpec, controller, TaskStateType.PENDING)
            tasks[id] = task
            val delayMs = task.startDelayMs(time.milliseconds())
            task.startFuture = scheduler.schedule(executor, (RunTask(task)), delayMs)
            log.info(
                "Created a new task {} with spec {}, scheduled to start {} ms from now.",
                id, originalSpec, delayMs
            )
        }
    }

    /**
     * Handles starting a task. Processed by the state change thread.
     */
    internal inner class RunTask(private val task: ManagedTask) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            task.clearStartFuture()
            if (task.state != TaskStateType.PENDING) {
                log.info("Can't start task {}, because it is already in state {}.", task.id, task.state)
                return
            }

            val nodeNames: TreeSet<String>
            try {
                nodeNames = task.findNodeNames()
            } catch (e: Exception) {
                log.error("Unable to find nodes for task {}", task.id, e)
                task.doneMs = time.milliseconds()
                task.state = TaskStateType.DONE
                task.maybeSetError("Unable to find nodes for task: " + e.message)
                return
            }
            log.info("Running task {} on node(s): {}", task.id, nodeNames.joinToString())
            task.state = TaskStateType.RUNNING
            task.startedMs = time.milliseconds()
            for (workerName in nodeNames) {
                val workerId = nextWorkerId++
                task.workerIds[workerName] = workerId
                workerStates[workerId] = WorkerReceiving(task.id, task.spec)
                nodeManagers[workerName]!!.createWorker(workerId, task.id, task.spec)
            }
            return
        }
    }

    /**
     * Stop a task.
     *
     * @param id The ID of the task to stop.
     */
    @Throws(Throwable::class)
    fun stopTask(id: String) {
        try {
            executor.submit(CancelTask(id)).get()
        } catch (exception: ExecutionException) {
            log.info("stopTask(id={}) error", id, exception)
            throw exception.cause!!
        }
    }

    /**
     * Handles cancelling a task. Processed by the state change thread.
     */
    internal inner class CancelTask(private val id: String) : Callable<Unit> {
        @Throws(Exception::class)
        override fun call() {
            if (id.isEmpty()) throw InvalidRequestException("Invalid empty ID in stopTask request.")

            val task = tasks[id] ?: run {
                log.info("Can't cancel non-existent task {}.", id)
                return
            }
            when (task.state) {
                TaskStateType.PENDING -> {
                    task.cancelled = true
                    task.clearStartFuture()
                    task.doneMs = time.milliseconds()
                    task.state = TaskStateType.DONE
                    log.info("Stopped pending task {}.", id)
                }

                TaskStateType.RUNNING -> {
                    task.cancelled = true
                    val activeWorkerIds = task.activeWorkerIds()
                    if (activeWorkerIds.isEmpty()) {
                        if (task.error.isEmpty()) log.info("Task {} is now complete with no errors.", id)
                        else log.info("Task {} is now complete with error: {}", id, task.error)
                        task.doneMs = time.milliseconds()
                        task.state = TaskStateType.DONE
                    } else {
                        for ((nodeName, workerId) in activeWorkerIds) {
                            nodeManagers[nodeName]!!.stopWorker(workerId)
                        }
                        log.info(
                            "Cancelling task {} with worker(s) {}",
                            id, mkString(activeWorkerIds, "", "", " = ", ", ")
                        )
                        task.state = TaskStateType.STOPPING
                    }
                }

                TaskStateType.STOPPING -> log.info("Can't cancel task {} because it is already stopping.", id)
                TaskStateType.DONE -> log.info("Can't cancel task {} because it is already done.", id)
            }
        }
    }

    @Throws(Throwable::class)
    fun destroyTask(id: String) {
        try {
            executor.submit(DestroyTask(id)).get()
        } catch (exception: ExecutionException) {
            log.info("destroyTask(id={}) error", id, exception)
            throw exception.cause!!
        }
    }

    /**
     * Handles destroying a task. Processed by the state change thread.
     */
    internal inner class DestroyTask(private val id: String) : Callable<Unit> {
        @Throws(Exception::class)
        override fun call() {
            if (id.isEmpty()) throw InvalidRequestException("Invalid empty ID in destroyTask request.")

            val task = tasks.remove(id)
            if (task == null) {
                log.info("Can't destroy task {}: no such task found.", id)
                return
            }
            log.info("Destroying task {}.", id)
            task.clearStartFuture()
            for ((nodeName, workerId) in task.workerIds.entries) {
                workerStates.remove(workerId)
                nodeManagers[nodeName]!!.destroyWorker(workerId)
            }
        }
    }

    /**
     * Update the state of a particular agent's worker.
     *
     * @param nodeName The node where the agent is running.
     * @param workerId The worker ID.
     * @param state The worker state.
     */
    fun updateWorkerState(nodeName: String, workerId: Long, state: WorkerState) {
        executor.submit(UpdateWorkerState(nodeName, workerId, state))
    }

    /**
     * Updates the state of a worker. Process by the state change thread.
     */
    internal inner class UpdateWorkerState(
        private val nodeName: String,
        private val workerId: Long,
        private val nextState: WorkerState,
    ) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            try {
                val prevState = workerStates[workerId]
                    ?: throw RuntimeException("Unable to find workerId $workerId")
                val task = tasks[prevState.taskId()]
                    ?: throw RuntimeException("Unable to find taskId " + prevState.taskId())

                log.debug(
                    "Task {}: Updating worker state for {} on {} from {} to {}.",
                    task.id, workerId, nodeName, prevState, nextState
                )
                workerStates[workerId] = nextState
                if (nextState.done() && (!prevState.done())) handleWorkerCompletion(
                    task = task,
                    nodeName = nodeName,
                    state = nextState as WorkerDone
                )
            } catch (exception: Exception) {
                log.error("Error updating worker state for {} on {}. Stopping worker.", workerId, nodeName, exception)
                nodeManagers[nodeName]!!.stopWorker(workerId)
            }
        }
    }

    /**
     * Handle a worker being completed.
     *
     * @param task The task that owns the worker.
     * @param nodeName The name of the node on which the worker is running.
     * @param state The worker state.
     */
    private fun handleWorkerCompletion(task: ManagedTask, nodeName: String, state: WorkerDone) {
        if (state.error().isEmpty())
            log.info("{}: Worker {} finished with status '{}'", nodeName, task.id, toJsonString(state.status()))
        else {
            log.warn(
                "{}: Worker {} finished with error '{}' and status '{}'",
                nodeName, task.id, state.error(), toJsonString(state.status())
            )
            task.maybeSetError(state.error())
        }
        val activeWorkerIds = task.activeWorkerIds()
        if (activeWorkerIds.isEmpty()) {
            task.doneMs = time.milliseconds()
            task.state = TaskStateType.DONE
            log.info(
                "{}: Task {} is now complete on {} with error: {}",
                nodeName, task.id, task.workerIds.keys.joinToString(),
                task.error.ifEmpty { "(none)" }
            )
        } else if ((task.state == TaskStateType.RUNNING) && (task.error.isNotEmpty())) {
            log.info(
                "{}: task {} stopped with error {}. Stopping worker(s): {}",
                nodeName, task.id, task.error, mkString(
                    map = activeWorkerIds,
                    begin = "{",
                    end = "}",
                    keyValueSeparator = ": ",
                    elementSeparator = ", ",
                )
            )
            task.state = TaskStateType.STOPPING
            for ((name, workerId) in activeWorkerIds)
                nodeManagers[name]!!.stopWorker(workerId)
        }
    }

    /**
     * Get information about the tasks being managed.
     */
    @Throws(ExecutionException::class, InterruptedException::class)
    fun tasks(request: TasksRequest): TasksResponse = executor.submit(GetTasksResponse(request)).get()

    /**
     * Gets information about the tasks being managed. Processed by the state change thread.
     */
    internal inner class GetTasksResponse(private val request: TasksRequest) : Callable<TasksResponse> {

        @Throws(Exception::class)
        override fun call(): TasksResponse {
            val states = TreeMap<String, TaskState>()
            for (task in tasks.values) {
                if (request.matches(task.id, task.startedMs, task.doneMs, task.state))
                    states[task.id] = task.taskState()
            }
            return TasksResponse(states)
        }
    }

    /**
     * Get information about a single task being managed.
     *
     * Returns #`null` if the task does not exist
     */
    @Throws(ExecutionException::class, InterruptedException::class)
    fun task(request: TaskRequest): TaskState? = executor.submit(GetTaskState(request)).get()

    /**
     * Gets information about the tasks being managed. Processed by the state change thread.
     */
    internal inner class GetTaskState(private val request: TaskRequest) : Callable<TaskState> {

        @Throws(Exception::class)
        override fun call(): TaskState? {
            return  tasks[request.taskId()]?.taskState() ?: return null
        }
    }

    /**
     * Initiate shutdown, but do not wait for it to complete.
     */
    fun beginShutdown(stopAgents: Boolean) {
        if (shutdown.compareAndSet(false, true))
            executor.submit(Shutdown(stopAgents))
    }

    /**
     * Wait for shutdown to complete. May be called prior to beginShutdown.
     */
    @Throws(InterruptedException::class)
    fun waitForShutdown() {
        while (!executor.awaitTermination(1, TimeUnit.DAYS)) Unit
    }

    internal inner class Shutdown(private val stopAgents: Boolean) : Callable<Unit> {
        @Throws(Exception::class)
        override fun call() {
            log.info("Shutting down TaskManager{}.", if (stopAgents) " and agents" else "")

            for (nodeManager: NodeManager in nodeManagers.values)
                nodeManager.beginShutdown(stopAgents)
            for (nodeManager: NodeManager in nodeManagers.values)
                nodeManager.waitForShutdown()

            executor.shutdown()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(TaskManager::class.java)
    }
}
