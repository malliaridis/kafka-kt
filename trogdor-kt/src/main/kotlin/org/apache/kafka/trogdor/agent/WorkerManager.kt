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

package org.apache.kafka.trogdor.agent

import java.util.TreeMap
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.stackTrace
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.rest.RequestConflictException
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerRunning
import org.apache.kafka.trogdor.rest.WorkerStarting
import org.apache.kafka.trogdor.rest.WorkerState
import org.apache.kafka.trogdor.rest.WorkerStopping
import org.apache.kafka.trogdor.task.AgentWorkerStatusTracker
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker
import org.slf4j.LoggerFactory
import kotlin.math.max

/**
 * @property platform The platform to use.
 * @property scheduler The scheduler to use.
 */
class WorkerManager internal constructor(
    private val platform: Platform,
    private val scheduler: Scheduler,
) {

    /**
     * The name of this node.
     */
    private val nodeName: String = platform.curNode().name()

    /**
     * The clock to use.
     */
    private val time: Time = scheduler.time()

    /**
     * A map of task IDs to Work objects.
     */
    private val workers: MutableMap<Long, Worker>

    /**
     * An ExecutorService used to schedule events in the future.
     */
    private val stateChangeExecutor: ScheduledExecutorService

    /**
     * An ExecutorService used to clean up TaskWorkers.
     */
    private val workerCleanupExecutor: ExecutorService

    /**
     * An ExecutorService to help with shutting down.
     */
    private val shutdownExecutor: ScheduledExecutorService

    /**
     * The shutdown manager.
     */
    private val shutdownManager = ShutdownManager()

    init {
        workers = HashMap()
        stateChangeExecutor = Executors.newSingleThreadScheduledExecutor(
            createThreadFactory("WorkerManagerStateThread", false)
        )
        workerCleanupExecutor = Executors.newCachedThreadPool(
            createThreadFactory("WorkerCleanupThread%d", false)
        )
        shutdownExecutor = Executors.newScheduledThreadPool(
            0,
            createThreadFactory("WorkerManagerShutdownThread%d", false)
        )
    }

    @Throws(Throwable::class)
    fun createWorker(workerId: Long, taskId: String, spec: TaskSpec): KafkaFuture<String> {
        try {
            shutdownManager.takeReference().use {
                val worker = stateChangeExecutor.submit(
                    CreateWorker(
                        workerId = workerId,
                        taskId = taskId,
                        spec = spec,
                        now = time.milliseconds(),
                    )
                ).get()!!
                if (worker.doneFuture != null) {
                    log.info(
                        "{}: Ignoring request to create worker {}, because there is already " +
                                "a worker with that id.", nodeName, workerId
                    )
                    return worker.doneFuture!!
                }
                worker.doneFuture = KafkaFutureImpl()
                if (worker.spec.endMs() <= time.milliseconds()) {
                    log.info("{}: Will not run worker {} as it has expired.", nodeName, worker)
                    stateChangeExecutor.submit(
                        HandleWorkerHalting(
                            worker = worker,
                            failure = "worker expired",
                            startupHalt = true,
                        )
                    )
                    return worker.doneFuture!!
                }
                val haltFuture = KafkaFutureImpl<String>()
                haltFuture.thenApply { errorString: String? ->
                    val error = errorString ?: ""
                    if (error.isEmpty()) log.info("{}: Worker {} is halting.", nodeName, worker)
                    else log.info(
                        "{}: Worker {} is halting with error {}",
                        nodeName, worker, error
                    )

                    stateChangeExecutor.submit(
                        HandleWorkerHalting(worker, error, false)
                    )
                    null
                }
                try {
                    worker.taskWorker.start(
                        platform = platform,
                        status = worker.status,
                        haltFuture = haltFuture,
                    )
                } catch (exception: Exception) {
                    log.info("{}: Worker {} start() exception", nodeName, worker, exception)
                    stateChangeExecutor.submit(
                        HandleWorkerHalting(
                            worker = worker,
                            failure = "worker.start() exception: ${stackTrace(exception)}",
                            startupHalt = true,
                        )
                    )
                }
                stateChangeExecutor.submit(FinishCreatingWorker(worker))
                return worker.doneFuture!!
            }
        } catch (exception: ExecutionException) {
            if (exception.cause is RequestConflictException) log.info(
                "{}: request conflict while creating worker {} for task {} with spec {}.",
                nodeName, workerId, taskId, spec
            )
            else log.info(
                "{}: Error creating worker {} for task {} with spec {}",
                nodeName, workerId, taskId, spec, exception
            )
            throw exception.cause!!
        }
    }

    /**
     * Handles a request to create a new worker. Processed by the state change thread.
     */
    internal inner class CreateWorker(
        private val workerId: Long,
        private val taskId: String,
        private val spec: TaskSpec,
        private val now: Long,
    ) : Callable<Worker> {

        @Throws(Exception::class)
        override fun call(): Worker {
            try {
                var worker = workers[workerId]
                if (worker != null) {
                    if (worker.taskId != taskId) throw RequestConflictException(
                        "There is already a worker ID $workerId with a different task ID."
                    )
                    else return if (worker.spec != spec) throw RequestConflictException(
                        "There is already a worker ID $workerId with a different task spec."
                    )
                    else worker
                }
                worker = Worker(workerId, taskId, spec, now)
                workers[workerId] = worker
                log.info("{}: Created worker {} with spec {}", nodeName, worker, spec)
                return worker
            } catch (e: Exception) {
                log.info(
                    "{}: unable to create worker {} for task {}, with spec {}",
                    nodeName, workerId, taskId, spec, e
                )
                throw e
            }
        }
    }

    /**
     * Finish creating a Worker. Processed by the state change thread.
     */
    internal inner class FinishCreatingWorker(private val worker: Worker) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            when (worker.state) {
                State.CANCELLING -> {
                    log.info(
                        "{}: Worker {} was cancelled while it was starting up. Transitioning to STOPPING.",
                        nodeName, worker
                    )
                    worker.transitionToStopping()
                }

                State.STARTING -> {
                    log.info(
                        "{}: Worker {} is now RUNNING. Scheduled to stop in {} ms.",
                        nodeName, worker, worker.spec.durationMs()
                    )
                    worker.transitionToRunning()
                }

                else -> Unit
            }
        }
    }

    /**
     * Handles a worker halting. Processed by the state change thread.
     */
    internal inner class HandleWorkerHalting(
        private val worker: Worker,
        private val failure: String,
        private val startupHalt: Boolean,
    ) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            if (worker.error.isEmpty()) worker.error = failure
            val verb = if (worker.error.isEmpty()) "halting" else "halting with error [${worker.error}]"
            
            when (worker.state) {
                State.STARTING -> if (startupHalt) {
                    log.info("{}: Worker {} {} during startup. Transitioning to DONE.", nodeName, worker, verb)
                    worker.transitionToDone()
                } else {
                    log.info("{}: Worker {} {} during startup. Transitioning to CANCELLING.", nodeName, worker, verb)
                    worker.state = State.CANCELLING
                }

                State.CANCELLING -> log.info("{}: Cancelling worker {} {}. ", nodeName, worker, verb)
                State.RUNNING -> {
                    log.info("{}: Running worker {} {}. Transitioning to STOPPING.", nodeName, worker, verb)
                    worker.transitionToStopping()
                }

                State.STOPPING -> log.info("{}: Stopping worker {} {}.", nodeName, worker, verb)
                State.DONE -> log.info("{}: Can't halt worker {} because it is already DONE.", nodeName, worker)
            }
        }
    }

    /**
     * Transitions a worker to WorkerDone. Processed by the state change thread.
     */
    internal inner class CompleteWorker(
        private val worker: Worker,
        private val failure: String,
    ) : Callable<Unit> {
        
        @Throws(Exception::class)
        override fun call() {
            if (worker.error.isEmpty() && failure.isNotEmpty()) worker.error = failure
            worker.transitionToDone()
            if (worker.mustDestroy) {
                log.info("{}: destroying worker {} with error {}", nodeName, worker, worker.error)
                workers.remove(worker.workerId)
            } else log.info("{}: completed worker {} with error {}", nodeName, worker, worker.error)
        }
    }

    @Throws(Throwable::class)
    fun stopWorker(workerId: Long, mustDestroy: Boolean) {
        try {
            shutdownManager.takeReference().use {
                stateChangeExecutor.submit(StopWorker(workerId, mustDestroy)).get()
            }
        } catch (e: ExecutionException) {
            throw (e.cause)!!
        }
    }

    /**
     * Stops a worker. Processed by the state change thread.
     */
    internal inner class StopWorker(
        private val workerId: Long,
        private val mustDestroy: Boolean,
    ) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            val worker = workers[workerId]
            if (worker == null) {
                log.info(
                    "{}: Can't stop worker {} because there is no worker with that ID.",
                    nodeName, workerId
                )
                return
            }
            if (mustDestroy) worker.mustDestroy = true

            when (worker.state) {
                State.STARTING -> {
                    log.info("{}: Cancelling worker {} during its startup process.", nodeName, worker)
                    worker.state = State.CANCELLING
                }

                State.CANCELLING ->
                    log.info("{}: Can't stop worker {}, because it is already being cancelled.", nodeName, worker)

                State.RUNNING -> {
                    log.info("{}: Stopping running worker {}.", nodeName, worker)
                    worker.transitionToStopping()
                }

                State.STOPPING ->
                    log.info("{}: Can't stop worker {}, because it is already stopping.", nodeName, worker)

                State.DONE -> if (worker.mustDestroy) {
                    log.info("{}: destroying worker {} with error {}", nodeName, worker, worker.error)
                    workers.remove(worker.workerId)
                } else log.debug("{}: Can't stop worker {}, because it is already done.", nodeName, worker)
            }
        }
    }

    /**
     * Cleans up the resources associated with a worker. Processed by the worker
     * cleanup thread pool.
     */
    internal inner class HaltWorker(private val worker: Worker) : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            var failure = ""
            try {
                worker.taskWorker.stop(platform)
            } catch (exception: Exception) {
                log.error("{}: worker.stop() exception", nodeName, exception)
                failure = exception.message!!
            }
            stateChangeExecutor.submit(CompleteWorker(worker, failure))
        }
    }

    @Throws(Exception::class)
    fun workerStates(): TreeMap<Long, WorkerState> {
        shutdownManager.takeReference().use {
            return stateChangeExecutor.submit(GetWorkerStates()).get()
        }
    }

    internal inner class GetWorkerStates : Callable<TreeMap<Long, WorkerState>> {
        @Throws(Exception::class)
        override fun call(): TreeMap<Long, WorkerState> {
            val workerMap = TreeMap<Long, WorkerState>()
            for (worker in workers.values) {
                workerMap[worker.workerId] = worker.state()
            }
            return workerMap
        }
    }

    @Throws(Exception::class)
    fun beginShutdown() {
        if (shutdownManager.shutdown())
            shutdownExecutor.submit(Shutdown())
    }

    @Throws(Exception::class)
    fun waitForShutdown() {
        while (!shutdownExecutor.isShutdown) {
            shutdownExecutor.awaitTermination(1, TimeUnit.DAYS)
        }
    }

    internal inner class Shutdown : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            log.info("{}: Shutting down WorkerManager.", nodeName)
            try {
                stateChangeExecutor.submit(DestroyAllWorkers()).get()
                log.info("{}: Waiting for shutdownManager quiescence...", nodeName)
                shutdownManager.waitForQuiescence()
                workerCleanupExecutor.shutdownNow()
                stateChangeExecutor.shutdownNow()
                log.info("{}: Waiting for workerCleanupExecutor to terminate...", nodeName)
                workerCleanupExecutor.awaitTermination(1, TimeUnit.DAYS)
                log.info("{}: Waiting for stateChangeExecutor to terminate...", nodeName)
                stateChangeExecutor.awaitTermination(1, TimeUnit.DAYS)
                log.info("{}: Shutting down shutdownExecutor.", nodeName)
                shutdownExecutor.shutdown()
            } catch (e: Exception) {
                log.info("{}: Caught exception while shutting down WorkerManager", nodeName, e)
                throw e
            }
        }
    }

    /**
     * Begins the process of destroying all workers. Processed by the state change thread.
     */
    internal inner class DestroyAllWorkers : Callable<Unit> {

        @Throws(Exception::class)
        override fun call() {
            log.info("{}: Destroying all workers.", nodeName)

            // StopWorker may remove elements from the set of worker IDs. That might generate
            // a ConcurrentModificationException if we were iterating over the worker ID
            // set directly. Therefore, we make a copy of the worker IDs here and iterate
            // over that instead.
            //
            // Note that there is no possible way that more worker IDs can be added while this
            // callable is running, because the state change executor is single-threaded.
            val workerIds = ArrayList(workers.keys)
            for (workerId in workerIds) {
                try {
                    StopWorker(workerId, true).call()
                } catch (e: Exception) {
                    log.error("Failed to stop worker {}", workerId, e)
                }
            }
        }
    }

    /**
     * The shutdown manager handles shutting down gracefully.
     *
     * We can shut down gracefully only when all the references handed out by the ShutdownManager has been closed,
     * and the shutdown bit has been set. RPC operations hold a reference for the duration of their execution,
     * and so do Workers which have not been shut down. This prevents us from shutting down in the middle of an RPC,
     * or with workers which are still running.
     */
    internal class ShutdownManager {

        private var shutdown = false

        private var refCount: Long = 0

        internal inner class Reference : AutoCloseable {

            var closed = AtomicBoolean(false)

            override fun close() {
                if (closed.compareAndSet(false, true)) {
                    synchronized(this@ShutdownManager) {
                        refCount--
                        if (shutdown && (refCount == 0L))
                            (this@ShutdownManager as Object).notifyAll()
                    }
                }
            }
        }

        @Synchronized
        fun takeReference(): Reference {
            if (shutdown) throw KafkaException("WorkerManager is shut down.")
            refCount++
            return Reference()
        }

        @Synchronized
        fun shutdown(): Boolean {
            if (shutdown) return false
            shutdown = true

            if (refCount == 0L) (this as Object).notifyAll()
            return true
        }

        @Synchronized
        @Throws(InterruptedException::class)
        fun waitForQuiescence() {
            while (!shutdown || refCount > 0) (this as Object).wait()
        }
    }

    internal enum class State {
        STARTING,
        CANCELLING,
        RUNNING,
        STOPPING,
        DONE
    }

    /**
     * A worker which is being tracked.
     *
     * @property workerId The worker ID.
     * @property taskId The task ID.
     * @property spec The task specification.
     * @property startedMs The time when this task was started.
     */
    internal inner class Worker(
        val workerId: Long,
        val taskId: String,
        val spec: TaskSpec,
        private val startedMs: Long,
    ) {

        /**
         * The work which this worker is performing.
         */
        val taskWorker: TaskWorker = spec.newTaskWorker(taskId)

        /**
         * The worker status.
         */
        val status = AgentWorkerStatusTracker()

        /**
         * The work state.
         */
        var state = State.STARTING

        /**
         * The time when this task was completed, or -1 if it has not been.
         */
        private var doneMs: Long = -1

        /**
         * The worker error.
         */
        var error: String = ""

        /**
         * If there is a task timeout scheduled, this is a future which can be used to cancel it.
         */
        private var timeoutFuture: Future<Unit>? = null

        /**
         * A future which is completed when the task transitions to DONE state.
         */
        var doneFuture: KafkaFutureImpl<String>? = null

        /**
         * A shutdown manager reference which will keep the WorkerManager alive for as long as this worker is alive.
         */
        private var reference: ShutdownManager.Reference? = shutdownManager.takeReference()

        /**
         * Whether we should destroy the records of this worker once it stops.
         */
        var mustDestroy = false

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("workerId"),
        )
        fun workerId(): Long = workerId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("taskId"),
        )
        fun taskId(): String = taskId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("spec"),
        )
        fun spec(): TaskSpec = spec

        fun state(): WorkerState = when (state) {
            State.STARTING -> WorkerStarting(taskId, spec)
            State.RUNNING -> WorkerRunning(taskId, spec, startedMs, status.get())
            State.CANCELLING, State.STOPPING -> WorkerStopping(taskId, spec, startedMs, status.get())
            State.DONE -> WorkerDone(taskId, spec, startedMs, doneMs, status.get(), error)
        }

        fun transitionToRunning() {
            state = State.RUNNING
            timeoutFuture = scheduler.schedule(
                executor = stateChangeExecutor,
                callable = StopWorker(workerId, false),
                delayMs = (spec.endMs() - time.milliseconds()).coerceAtLeast(0),
            )
        }

        fun transitionToStopping(): Future<Unit> {
            state = State.STOPPING
            if (timeoutFuture != null) {
                timeoutFuture!!.cancel(false)
                timeoutFuture = null
            }
            return workerCleanupExecutor.submit(HaltWorker(this))
        }

        fun transitionToDone() {
            state = State.DONE
            doneMs = time.milliseconds()
            reference?.let {
                it.close()
                reference = null
            }
            doneFuture!!.complete(error)
        }

        override fun toString(): String = String.format("%s_%d", taskId, workerId)
    }

    companion object {
        private val log = LoggerFactory.getLogger(WorkerManager::class.java)
    }
}
