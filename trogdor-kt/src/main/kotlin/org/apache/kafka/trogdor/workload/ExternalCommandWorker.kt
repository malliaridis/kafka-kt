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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

/**
 * ExternalCommandWorker starts an external process to run a Trogdor command.
 *
 * The worker communicates with the external process over the standard input and output streams.
 *
 * When the process is first launched, ExternalCommandWorker will send a message on standard input describing
 * the task ID and the workload. This message will not contain line breaks. It will have this JSON format:
 *
 * ```json
 * {"id":<task ID string>, "workload":<configured workload JSON object>}
 * ```
 *
 * ExternalCommandWorker will log anything that the process writes to stderr, but will take no other action with it.
 *
 * If the process sends a single-line JSON object to stdout, ExternalCommandWorker will parse it.
 * The JSON object can contain the following fields:
 *
 * - status: If the object contains this field, the status will be set to the given value.
 * - error: If the object contains this field, the error will be set to the given value.
 * Once an error occurs, we will try to terminate the process.
 * - log: If the object contains this field, a log message will be issued with this text.
 *
 * Note that standard output is buffered by default. The subprocess may wish to flush it after writing its status JSON.
 * This will ensure that the status is seen in a timely fashion.
 *
 * If the process sends a non-JSON line to stdout, the worker will log it.
 *
 * If the process exits, ExternalCommandWorker will finish. If the process exits unsuccessfully, this is considered
 * an error. If the worker needs to stop the process, it will start by sending a `SIGTERM`. If this does not have
 * the required effect, it will send a SIGKILL, once the shutdown grace period has elapsed.
 *
 * @property id The task ID.
 * @property spec The command specification.
 */
class ExternalCommandWorker(
    private val id: String,
    private val spec: ExternalCommandSpec,
) : TaskWorker {

    /**
     * True only if the worker is running.
     */
    private val running = AtomicBoolean(false)

    internal enum class TerminatorAction {
        DESTROY,
        DESTROY_FORCIBLY,
        CLOSE
    }

    /**
     * A queue used to communicate with the signal sender thread.
     */
    private val terminatorActionQueue = LinkedBlockingQueue<TerminatorAction>()

    /**
     * The queue of objects to write to the process stdin.
     */
    private val stdinQueue = LinkedBlockingQueue<Optional<JsonNode>>()

    /**
     * Tracks the worker status.
     */
    private var status: WorkerStatusTracker? = null

    /**
     * A future which should be completed when this worker is done.
     */
    private var doneFuture: KafkaFutureImpl<String>? = null

    /**
     * The executor service for this worker.
     */
    private var executor: ExecutorService? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) { "ConsumeBenchWorker is already running." }
        log.info("{}: Activating ExternalCommandWorker with {}", id, spec)
        this.status = status
        this.doneFuture = haltFuture
        executor = Executors.newCachedThreadPool(
            createThreadFactory("ExternalCommandWorkerThread%d", false)
        )
        val executor = executor
        val process = try {
            startProcess()
        } catch (throwable: Throwable) {
            log.error("{}: Unable to start process", id, throwable)
            executor!!.shutdown()
            haltFuture.complete("Unable to start process: " + throwable.message)
            return
        }
        val stdoutFuture = executor!!.submit(StdoutMonitor(process))
        val stderrFuture = executor.submit(StderrMonitor(process))
        executor.submit(StdinWriter(process))
        val terminatorFuture = executor.submit(Terminator(process))
        executor.submit(ExitMonitor(process, stdoutFuture, stderrFuture, terminatorFuture))
        val startMessage = ObjectNode(JsonNodeFactory.instance)
        startMessage.set<JsonNode>("id", TextNode(id))
        startMessage.set<JsonNode>("workload", spec.workload())
        stdinQueue.add(Optional.of(startMessage))
    }

    @Throws(Exception::class)
    private fun startProcess(): Process {
        if (spec.command().isEmpty()) throw RuntimeException("No command specified")

        val bld = ProcessBuilder(spec.command())
        return bld.start()
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform) {
        check(running.compareAndSet(true, false)) { "ExternalCommandWorker is not running." }
        log.info("{}: Deactivating ExternalCommandWorker.", id)
        terminatorActionQueue.add(TerminatorAction.DESTROY)
        val shutdownGracePeriodMs = spec.shutdownGracePeriodMs() ?: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS
        if (!executor!!.awaitTermination(shutdownGracePeriodMs.toLong(), TimeUnit.MILLISECONDS)) {
            terminatorActionQueue.add(TerminatorAction.DESTROY_FORCIBLY)
            executor!!.awaitTermination(1, TimeUnit.DAYS)
        }
        status = null
        doneFuture = null
        executor = null
    }

    internal inner class StdoutMonitor(private val process: Process) : Runnable {

        override fun run() {
            log.trace("{}: starting stdout monitor.", id)
            try {
                BufferedReader(InputStreamReader(process.inputStream, StandardCharsets.UTF_8)).use { br ->
                    var line: String?
                    while (true) {
                        try {
                            line = br.readLine()
                            if (line == null) throw IOException("EOF")
                        } catch (e: IOException) {
                            log.info("{}: can't read any more from stdout: {}", id, e.message)
                            return
                        }
                        log.trace("{}: read line from stdin: {}", id, line)
                        val resp =
                            readObject(line)
                        if (resp.has("status")) {
                            log.info("{}: New status: {}", id, resp["status"].toString())
                            status!!.update(resp["status"])
                        }
                        if (resp.has("log")) {
                            log.info("{}: (stdout): {}", id, resp["log"].asText())
                        }
                        if (resp.has("error")) {
                            val error = resp["error"].asText()
                            log.error("{}: error: {}", id, error)
                            doneFuture!!.complete(error)
                        }
                    }
                }
            } catch (exception: Throwable) {
                log.info("{}: error reading from stdout.", id, exception)
            }
        }
    }

    internal inner class StderrMonitor(private val process: Process) : Runnable {

        override fun run() {
            log.trace("{}: starting stderr monitor.", id)
            try {
                BufferedReader(InputStreamReader(process.errorStream, StandardCharsets.UTF_8)).use { br ->
                    var line: String?
                    while (true) {
                        try {
                            line = br.readLine()
                            if (line == null) throw IOException("EOF")
                        } catch (exception: IOException) {
                            log.info("{}: can't read any more from stderr: {}", id, exception.message)
                            return
                        }
                        log.error("{}: (stderr):{}", id, line)
                    }
                }
            } catch (exception: Throwable) {
                log.info("{}: error reading from stderr.", id, exception)
            }
        }
    }

    internal inner class StdinWriter(private val process: Process) : Runnable {

        override fun run() {
            val stdinWriter = OutputStreamWriter(process.outputStream, StandardCharsets.UTF_8)
            try {
                while (true) {
                    log.info("{}: stdin writer ready.", id)
                    val node = stdinQueue.take()
                    if (!node.isPresent) {
                        log.trace("{}: StdinWriter terminating.", id)
                        return
                    }
                    val inputString = toJsonString(node.get())
                    log.info("{}: writing to stdin: {}", id, inputString)
                    stdinWriter.write(inputString + "\n")
                    stdinWriter.flush()
                }
            } catch (exception: IOException) {
                log.info("{}: can't write any more to stdin: {}", id, exception.message)
            } catch (exception: Throwable) {
                log.info("{}: error writing to stdin.", id, exception)
            } finally {
                try {
                    stdinWriter.close()
                } catch (exception: IOException) {
                    log.debug("{}: error closing stdinWriter: {}", id, exception.message)
                }
            }
        }
    }

    internal inner class ExitMonitor(
        private val process: Process,
        private val stdoutFuture: Future<*>,
        private val stderrFuture: Future<*>,
        private val terminatorFuture: Future<*>,
    ) : Runnable {

        override fun run() {
            try {
                val exitStatus = process.waitFor()
                log.info("{}: process exited with return code {}", id, exitStatus)
                // Wait for the stdout and stderr monitors to exit. It's particularly important
                // to wait for the stdout monitor to exit since there may be an error or status
                // there that we haven't seen yet.
                stdoutFuture.get()
                stderrFuture.get()
                // Try to complete doneFuture with an error status based on the exit code. Note
                // that if doneFuture was already completed previously, this will have no effect.
                if (exitStatus == 0) doneFuture!!.complete("")
                else doneFuture!!.complete("exited with return code $exitStatus")

                // Tell the StdinWriter thread to exit.
                stdinQueue.add(Optional.empty())
                // Tell the shutdown manager thread to exit.
                terminatorActionQueue.add(TerminatorAction.CLOSE)
                terminatorFuture.get()
                executor!!.shutdown()
            } catch (e: Throwable) {
                log.error("{}: ExitMonitor error", id, e)
                doneFuture!!.complete("ExitMonitor error: " + e.message)
            }
        }
    }

    /**
     * The thread which manages terminating the child process.
     */
    internal inner class Terminator(private val process: Process) : Runnable {

        override fun run() {
            try {
                while (true) {
                    when (terminatorActionQueue.take()) {
                        TerminatorAction.DESTROY -> {
                            log.info("{}: destroying process", id)
                            process.inputStream.close()
                            process.errorStream.close()
                            process.destroy()
                        }

                        TerminatorAction.DESTROY_FORCIBLY -> {
                            log.info("{}: forcibly destroying process", id)
                            process.inputStream.close()
                            process.errorStream.close()
                            process.destroyForcibly()
                        }

                        TerminatorAction.CLOSE -> {
                            log.trace("{}: closing Terminator thread.", id)
                            return
                        }
                        null -> {}
                    }
                }
            } catch (exception: Throwable) {
                log.error("{}: Terminator error", id, exception)
                doneFuture!!.complete("Terminator error: " + exception.message)
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(ExternalCommandWorker::class.java)

        private const val DEFAULT_SHUTDOWN_GRACE_PERIOD_MS = 5000

        private fun readObject(line: String): JsonNode {
            val resp = try {
                JsonUtil.JSON_SERDE.readTree(line)
            } catch (e: IOException) {
                return NullNode.instance
            }
            return resp
        }
    }
}
