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

import java.net.ConnectException
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.trogdor.agent.AgentClient
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Node.Util.getTrogdorAgentPort
import org.apache.kafka.trogdor.rest.AgentStatusResponse
import org.apache.kafka.trogdor.rest.CreateWorkerRequest
import org.apache.kafka.trogdor.rest.StopWorkerRequest
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerReceiving
import org.apache.kafka.trogdor.rest.WorkerRunning
import org.apache.kafka.trogdor.rest.WorkerStarting
import org.apache.kafka.trogdor.rest.WorkerState
import org.apache.kafka.trogdor.rest.WorkerStopping
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.LoggerFactory

/**
 * The NodeManager handles communicating with a specific agent node.
 * Each NodeManager has its own ExecutorService which runs in a dedicated thread.
 *
 * @property node The node which we are managing.
 */
class NodeManager internal constructor(
    private val node: Node,
    taskManager: TaskManager,
) {

    /**
     * The task manager.
     */
    private val taskManager: TaskManager

    /**
     * A client for the Node's Agent.
     */
    private val client: AgentClient

    /**
     * Maps task IDs to worker structures.
     */
    private val workers: MutableMap<Long, ManagedWorker>

    /**
     * An executor service which manages the thread dedicated to this node.
     */
    private val executor: ScheduledExecutorService

    /**
     * The heartbeat runnable.
     */
    private val heartbeat: NodeHeartbeat

    /**
     * A future which can be used to cancel the periodic hearbeat task.
     */
    private var heartbeatFuture: ScheduledFuture<*>? = null

    init {
        this.taskManager = taskManager
        client = AgentClient.Builder()
            .maxTries(1)
            .target(node.hostname(), getTrogdorAgentPort(node))
            .build()
        workers = HashMap()
        executor = Executors.newSingleThreadScheduledExecutor(
            createThreadFactory(pattern = "NodeManager(${node.name()})", daemon = false)
        )
        heartbeat = NodeHeartbeat()
        rescheduleNextHeartbeat(HEARTBEAT_DELAY_MS)
    }

    /**
     * Reschedule the heartbeat runnable.
     *
     * @param initialDelayMs        The initial delay to use.
     */
    fun rescheduleNextHeartbeat(initialDelayMs: Long) {
        if (heartbeatFuture != null) heartbeatFuture!!.cancel(false)
        
        heartbeatFuture = executor.scheduleAtFixedRate(
            heartbeat,
            initialDelayMs,
            HEARTBEAT_DELAY_MS,
            TimeUnit.MILLISECONDS,
        )
    }

    /**
     * The heartbeat runnable.
     */
    internal inner class NodeHeartbeat : Runnable {
        override fun run() {
            rescheduleNextHeartbeat(HEARTBEAT_DELAY_MS)
            try {
                val agentStatus = try {
                    client.status()
                } catch (exception: ConnectException) {
                    log.error("{}: failed to get agent status: ConnectException {}", node.name(), exception.message)
                    return
                } catch (exception: Exception) {
                    log.error("{}: failed to get agent status", node.name(), exception)
                    // TODO: eventually think about putting tasks into a bad state as a result of
                    // agents going down?
                    return
                }
                if (log.isTraceEnabled) log.trace("{}: got heartbeat status {}", node.name(), agentStatus)
                
                handleMissingWorkers(agentStatus)
                handlePresentWorkers(agentStatus)
            } catch (exception: Throwable) {
                log.error("{}: Unhandled exception in NodeHeartbeatRunnable", node.name(), exception)
            }
        }

        /**
         * Identify workers which we think should be running but do not appear in the agent's response.
         * We need to send startWorker requests for those
         */
        private fun handleMissingWorkers(agentStatus: AgentStatusResponse) {
            for ((workerId, worker) in workers) {
                if (!agentStatus.workers.containsKey(workerId)) {
                    if (worker.shouldRun) worker.tryCreate()
                }
            }
        }

        private fun handlePresentWorkers(agentStatus: AgentStatusResponse) {
            for ((workerId, state) in agentStatus.workers) {
                val worker = workers[workerId]
                if (worker == null) {
                    // Identify tasks which are running, but which we don't know about.
                    // Add these to the NodeManager as tasks that should not be running.
                    log.warn("{}: scheduling unknown worker with ID {} for stopping.", node.name(), workerId)
                    workers[workerId] = ManagedWorker(
                        workerId = workerId,
                        taskId = state.taskId(),
                        spec = state.spec(),
                        shouldRun = false,
                        state = state,
                    )
                } else {
                    // Handle workers which need to be stopped.
                    if (state is WorkerStarting || state is WorkerRunning) {
                        if (!worker.shouldRun) worker.tryStop()
                    }
                    // Notify the TaskManager if the worker state has changed.
                    if (worker.state == state) log.debug("{}: worker state is still {}", node.name(), worker.state)
                    else {
                        log.info("{}: worker state changed from {} to {}", node.name(), worker.state, state)
                        if (state is WorkerDone || state is WorkerStopping) worker.shouldRun = false
                        worker.state = state
                        taskManager.updateWorkerState(node.name(), worker.workerId, state)
                    }
                }
            }
        }
    }

    /**
     * Create a new worker.
     *
     * @param workerId The new worker id.
     * @param taskId The new task id.
     * @param spec The task specification to use with the new worker.
     */
    fun createWorker(workerId: Long, taskId: String, spec: TaskSpec) =
        executor.submit(CreateWorker(workerId, taskId, spec))

    /**
     * Starts a worker.
     */
    internal inner class CreateWorker(
        private val workerId: Long,
        private val taskId: String,
        private val spec: TaskSpec,
    ) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            var worker = workers[workerId]
            if (worker != null) {
                log.error(
                    "{}: there is already a worker {} with ID {}.",
                    node.name(), worker, workerId
                )
            }
            worker = ManagedWorker(workerId, taskId, spec, true, WorkerReceiving(taskId, spec))
            log.info("{}: scheduling worker {} to start.", node.name(), worker)
            workers[workerId] = worker
            rescheduleNextHeartbeat(0)
        }
    }

    /**
     * Stop a worker.
     *
     * @param workerId The id of the worker to stop.
     */
    fun stopWorker(workerId: Long) = executor.submit(StopWorker(workerId))

    /**
     * Stops a worker.
     */
    internal inner class StopWorker(private val workerId: Long) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            val worker = workers[workerId]
            if (worker == null) {
                log.error("{}: unable to locate worker to stop with ID {}.", node.name(), workerId)
            }
            if (!worker!!.shouldRun) {
                log.error("{}: Worker {} is already scheduled to stop.", node.name(), worker)
            }
            log.info("{}: scheduling worker {} to stop.", node.name(), worker)
            worker.shouldRun = false
            rescheduleNextHeartbeat(0)
        }
    }

    /**
     * Destroy a worker.
     *
     * @param workerId The id of the worker to destroy.
     */
    fun destroyWorker(workerId: Long) = executor.submit(DestroyWorker(workerId))

    /**
     * Destroys a worker.
     */
    internal inner class DestroyWorker(private val workerId: Long) : Callable<Unit> {
        @Throws(Exception::class)
        override fun call() {
            val worker = workers.remove(workerId)
            if (worker == null) log.error("{}: unable to locate worker to destroy with ID {}.", node.name(), workerId)
            rescheduleNextHeartbeat(0)
        }
    }

    fun beginShutdown(stopNode: Boolean) {
        executor.shutdownNow()
        if (stopNode) {
            try {
                client.invokeShutdown()
            } catch (e: Exception) {
                log.error("{}: Failed to send shutdown request", node.name(), e)
            }
        }
    }

    @Throws(InterruptedException::class)
    fun waitForShutdown() = executor.awaitTermination(1, TimeUnit.DAYS)

    internal inner class ManagedWorker(
        val workerId: Long,
        private val taskId: String,
        private val spec: TaskSpec,
        var shouldRun: Boolean,
        var state: WorkerState,
    ) {
        fun tryCreate() {
            try {
                client.createWorker(CreateWorkerRequest(workerId, taskId, spec))
            } catch (e: Throwable) {
                log.error("{}: error creating worker {}.", node.name(), this, e)
            }
        }

        fun tryStop() {
            try {
                client.stopWorker(StopWorkerRequest(workerId))
            } catch (e: Throwable) {
                log.error("{}: error stopping worker {}.", node.name(), this, e)
            }
        }

        override fun toString(): String = "${taskId}_$workerId"
    }

    companion object {

        private val log = LoggerFactory.getLogger(NodeManager::class.java)

        /**
         * The normal amount of seconds between heartbeats sent to the agent.
         */
        private const val HEARTBEAT_DELAY_MS = 1000L
    }
}
