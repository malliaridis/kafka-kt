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

import com.fasterxml.jackson.databind.node.TextNode
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.PrintStream
import java.nio.file.Paths
import java.util.TreeMap
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.MockScheduler
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.Utils.delete
import org.apache.kafka.test.TestUtils.tempDirectory
import org.apache.kafka.trogdor.basic.BasicNode
import org.apache.kafka.trogdor.basic.BasicPlatform
import org.apache.kafka.trogdor.basic.BasicPlatform.ShellCommandRunner
import org.apache.kafka.trogdor.basic.BasicTopology
import org.apache.kafka.trogdor.common.ExpectedTasks
import org.apache.kafka.trogdor.common.ExpectedTasks.ExpectedTaskBuilder
import org.apache.kafka.trogdor.common.JsonUtil.toPrettyJsonString
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec
import org.apache.kafka.trogdor.fault.Kibosh
import org.apache.kafka.trogdor.fault.Kibosh.KiboshControlFile
import org.apache.kafka.trogdor.fault.Kibosh.KiboshFilesUnreadableFaultSpec
import org.apache.kafka.trogdor.rest.CreateWorkerRequest
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.apache.kafka.trogdor.rest.RequestConflictException
import org.apache.kafka.trogdor.rest.StopWorkerRequest
import org.apache.kafka.trogdor.rest.TaskDone
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerRunning
import org.apache.kafka.trogdor.task.NoOpTaskSpec
import org.apache.kafka.trogdor.task.SampleTaskSpec
import org.apache.kafka.trogdor.task.TaskSpec
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class AgentTest {

    private fun createAgent(scheduler: Scheduler): Agent {
        val restServer = JsonRestServer(0)
        val resource = AgentRestResource()
        restServer.start(resource)
        return Agent(
            platform = createBasicPlatform(scheduler),
            scheduler = scheduler,
            restServer = restServer,
            resource = resource
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAgentStartShutdown() {
        val agent = createAgent(Scheduler.SYSTEM)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentProgrammaticShutdown() {
        val agent = createAgent(Scheduler.SYSTEM)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        client.invokeShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentGetStatus() {
        val agent = createAgent(Scheduler.SYSTEM)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        val status = client.status()
        assertEquals(agent.status(), status)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testCreateExpiredWorkerIsNotScheduled() {
        val initialTimeMs: Long = 100
        val tickMs: Long = 15
        val toSleep = booleanArrayOf(true)
        val time = object : MockTime(tickMs, initialTimeMs, 0) {
            
            /**
             * Modify sleep() to call super.sleep() every second call
             * in order to avoid the endless loop in the tick() calls to the MockScheduler listener
             */
            override fun sleep(ms: Long) {
                toSleep[0] = !toSleep[0]
                if (toSleep[0]) super.sleep(ms)
            }
        }
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        val status = client.status()
        assertEquals(emptyMap(), status.workers)
        ExpectedTasks().waitFor(client)
        val fooSpec = NoOpTaskSpec(10, 10)
        client.createWorker(CreateWorkerRequest(0, "foo", fooSpec))
        val actualStartTimeMs = initialTimeMs + tickMs
        val doneMs = actualStartTimeMs + 2 * tickMs
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = actualStartTimeMs,
                    doneMs = doneMs,
                    status = null,
                    error = "worker expired",
                )
            ).taskState(
                TaskDone(
                    spec = fooSpec,
                    startedMs = actualStartTimeMs,
                    doneMs = doneMs,
                    error = "worker expired",
                    cancelled = false,
                    status = null
                )
            ).build()
        ).waitFor(client)
    }

    @Test
    @Throws(Exception::class)
    fun testAgentGetUptime() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 111, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        val uptime = client.uptime()
        assertEquals(agent.uptime(), uptime)
        time.setCurrentTimeMs(150)
        assertNotEquals(agent.uptime(), uptime)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentCreateWorkers() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        val status = client.status()
        assertEquals(emptyMap(), status.workers)
        ExpectedTasks().waitFor(client)
        val fooSpec = NoOpTaskSpec(1000, 600000)
        client.createWorker(CreateWorkerRequest(0, "foo", fooSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        val barSpec = NoOpTaskSpec(startMs = 2000, durationMs = 900000)
        client.createWorker(CreateWorkerRequest(workerId = 1, taskId = "bar", spec = barSpec))
        client.createWorker(CreateWorkerRequest(workerId = 1, taskId = "bar", spec = barSpec))
        assertFailsWith<RequestConflictException>(
            message = "Recreating a request with a different taskId is not allowed",
        ) { client.createWorker(CreateWorkerRequest(1, "foo", barSpec)) }
        assertFailsWith<RequestConflictException>(
            message = "Recreating a request with a different spec is not allowed",
        ) { client.createWorker(CreateWorkerRequest(1, "bar", fooSpec)) }
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    status = TextNode("active")
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerRunning(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        
        val bazSpec = NoOpTaskSpec(1, 450000)
        client.createWorker(CreateWorkerRequest(2, "baz", bazSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerRunning(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("baz").workerState(
                WorkerRunning(
                    taskId = "baz",
                    spec = bazSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentFinishesTasks() {
        val startTimeMs: Long = 2000
        val time = MockTime(autoTickMs = 0, currentTimeMs = startTimeMs, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        ExpectedTasks().waitFor(client)
        val fooSpec = NoOpTaskSpec(startMs = startTimeMs, durationMs = 2)
        client.createWorker(CreateWorkerRequest(workerId = 0, taskId = "foo", spec = fooSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = startTimeMs,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        time.sleep(1)
        val barSpecWorkerId: Long = 1
        val barSpecStartTimeMs = startTimeMs + 1
        val barSpec = NoOpTaskSpec(startTimeMs, 900000)
        client.createWorker(CreateWorkerRequest(barSpecWorkerId, "bar", barSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = startTimeMs,
                    status = TextNode("active"),
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerRunning(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = barSpecStartTimeMs,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        time.sleep(1)

        // foo task expired
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = startTimeMs,
                    doneMs = startTimeMs + 2,
                    status = TextNode("done"),
                    error = "",
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerRunning(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = barSpecStartTimeMs,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        time.sleep(5)
        client.stopWorker(StopWorkerRequest(barSpecWorkerId))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = startTimeMs,
                    doneMs = startTimeMs + 2,
                    status = TextNode("done"),
                    error = "",
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerDone(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = barSpecStartTimeMs,
                    doneMs = startTimeMs + 7,
                    status = TextNode("done"),
                    error = "",
                )
            ).build()
        ).waitFor(client)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testWorkerCompletions() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        ExpectedTasks().waitFor(client)
        val fooSpec = SampleTaskSpec(
            startMs = 0,
            durationMs = 900000,
            nodeToExitMs = mapOf("node01" to 1L),
            error = "",
        )
        client.createWorker(CreateWorkerRequest(workerId = 0, taskId = "foo", spec = fooSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    status = TextNode("active")
                )
            ).build()
        ).waitFor(client)
        val barSpec = SampleTaskSpec(
            startMs = 0,
            durationMs = 900000,
            nodeToExitMs = mapOf("node01" to 2L),
            error = "baz",
        )
        client.createWorker(CreateWorkerRequest(workerId = 1, taskId = "bar", spec = barSpec))
        time.sleep(1)
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    doneMs = 1,
                    status = TextNode("halted"),
                    error = "",
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerRunning(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        time.sleep(1)
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    doneMs = 1,
                    status = TextNode("halted"),
                    error = "",
                )
            ).build()
        ).addTask(
            ExpectedTaskBuilder("bar").workerState(
                WorkerDone(
                    taskId = "bar",
                    spec = barSpec,
                    startedMs = 0,
                    doneMs = 2,
                    status = TextNode("halted"),
                    error = "baz",
                )
            ).build()
        ).waitFor(client)
    }

    private class MockKibosh : AutoCloseable {

        val tempDir = tempDirectory()

        val controlFile = Paths.get(tempDir.toPath().toString(), Kibosh.KIBOSH_CONTROL)

        init {
            KiboshControlFile.EMPTY.write(controlFile)
        }

        @Throws(IOException::class)
        fun read(): KiboshControlFile {
            return KiboshControlFile.read(controlFile)
        }

        @Throws(Exception::class)
        override fun close() {
            delete(tempDir)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testKiboshFaults() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        ExpectedTasks().waitFor(client)
        MockKibosh().use { mockKibosh ->
            assertEquals(KiboshControlFile.EMPTY, mockKibosh.read())
            val fooSpec = FilesUnreadableFaultSpec(
                startMs = 0,
                durationMs = 900000,
                nodeNames = setOf("myAgent"),
                mountPath = mockKibosh.tempDir.path,
                prefix = "/foo",
                errorCode = 123
            )
            client.createWorker(CreateWorkerRequest(0, "foo", fooSpec))
            ExpectedTasks().addTask(
                ExpectedTaskBuilder("foo").workerState(
                    WorkerRunning(
                        taskId = "foo",
                        spec = fooSpec,
                        startedMs = 0,
                        status = TextNode("Added fault foo"),
                    )
                ).build()
            ).waitFor(client)

            assertEquals(
                expected = KiboshControlFile(
                    listOf(KiboshFilesUnreadableFaultSpec("/foo", 123))
                ),
                actual = mockKibosh.read(),
            )

            val barSpec = FilesUnreadableFaultSpec(
                startMs = 0,
                durationMs = 900000,
                nodeNames = setOf("myAgent"),
                mountPath = mockKibosh.tempDir.path,
                prefix = "/bar",
                errorCode = 456,
            )
            client.createWorker(CreateWorkerRequest(1, "bar", barSpec))
            ExpectedTasks().addTask(
                ExpectedTaskBuilder("foo").workerState(
                    WorkerRunning(
                        taskId = "foo",
                        spec = fooSpec,
                        startedMs = 0,
                        status = TextNode("Added fault foo"),
                    )
                ).build()
            ).addTask(
                ExpectedTaskBuilder("bar").workerState(
                    WorkerRunning(
                        taskId = "bar",
                        spec = barSpec,
                        startedMs = 0,
                        status = TextNode("Added fault bar"),
                    )
                ).build()
            ).waitFor(client)

            assertEquals(
                expected = KiboshControlFile(
                    listOf(
                        KiboshFilesUnreadableFaultSpec("/foo", 123),
                        KiboshFilesUnreadableFaultSpec("/bar", 456),
                    )
                ),
                actual = mockKibosh.read(),
            )

            time.sleep(1)
            client.stopWorker(StopWorkerRequest(0))
            ExpectedTasks().addTask(
                ExpectedTaskBuilder("foo").workerState(
                    WorkerDone(
                        taskId = "foo",
                        spec = fooSpec,
                        startedMs = 0,
                        doneMs = 1,
                        status = TextNode("Removed fault foo"),
                        error = "",
                    )
                ).build()
            ).addTask(
                ExpectedTaskBuilder("bar").workerState(
                    WorkerRunning(
                        taskId = "bar",
                        spec = barSpec,
                        startedMs = 0,
                        status = TextNode("Added fault bar"),
                    )
                ).build()
            ).waitFor(client)

            assertEquals(
                expected = KiboshControlFile(listOf(KiboshFilesUnreadableFaultSpec("/bar", 456))),
                actual = mockKibosh.read(),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDestroyWorkers() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        val agent = createAgent(scheduler)
        val client = AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
        ExpectedTasks().waitFor(client)
        val fooSpec = NoOpTaskSpec(startMs = 0, durationMs = 5)
        client.createWorker(CreateWorkerRequest(workerId = 0, taskId = "foo", spec = fooSpec))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec,
                    startedMs = 0,
                    status = TextNode("active"),
                )
            ).build()
        ).waitFor(client)
        time.sleep(1)
        client.destroyWorker(DestroyWorkerRequest(0))
        client.destroyWorker(DestroyWorkerRequest(0))
        client.destroyWorker(DestroyWorkerRequest(1))
        ExpectedTasks().waitFor(client)
        time.sleep(1)
        val fooSpec2 = NoOpTaskSpec(2, 1)
        client.createWorker(CreateWorkerRequest(1, "foo", fooSpec2))
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerRunning(
                    taskId = "foo",
                    spec = fooSpec2,
                    startedMs = 2,
                    status = TextNode("active")
                )
            ).build()
        ).waitFor(client)
        time.sleep(2)
        ExpectedTasks().addTask(
            ExpectedTaskBuilder("foo").workerState(
                WorkerDone(
                    taskId = "foo",
                    spec = fooSpec2,
                    startedMs = 2,
                    doneMs = 4,
                    status = TextNode("done"),
                    error = ""
                )
            ).build()
        ).waitFor(client)
        time.sleep(1)
        client.destroyWorker(DestroyWorkerRequest(1))
        ExpectedTasks().waitFor(client)
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentExecWithTimeout() {
        val agent = createAgent(Scheduler.SYSTEM)
        val spec = NoOpTaskSpec(0, 1)
        val rebasedSpec = agent.rebaseTaskSpecTime(spec)
        testExec(
            agent = agent,
            expected = String.format("Waiting for completion of task:%s%n", toPrettyJsonString(rebasedSpec)) +
                    String.format("Task failed with status null and error worker expired%n"),
            expectedReturn = false,
            spec = rebasedSpec,
        )
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    @Test
    @Throws(Exception::class)
    fun testAgentExecWithNormalExit() {
        val agent = createAgent(Scheduler.SYSTEM)
        val spec = SampleTaskSpec(
            startMs = 0,
            durationMs = 120000,
            nodeToExitMs = mapOf("node01" to 1L),
            error = "",
        )
        val rebasedSpec = agent.rebaseTaskSpecTime(spec)
        testExec(
            agent = agent,
            expected = String.format("Waiting for completion of task:%s%n", toPrettyJsonString(rebasedSpec)) +
                    String.format("Task succeeded with status \"halted\"%n"),
            expectedReturn = true,
            spec = rebasedSpec,
        )
        agent.beginShutdown()
        agent.waitForShutdown()
    }

    companion object {

        private fun createBasicPlatform(scheduler: Scheduler): BasicPlatform {
            val nodes = TreeMap<String, Node>()
            val config = mutableMapOf<String, String>()
            config[Platform.Config.TROGDOR_AGENT_PORT] = Agent.DEFAULT_PORT.toString()
            nodes["node01"] = BasicNode(
                name = "node01",
                hostname = "localhost",
                config = config,
                tags = emptySet(),
            )
            val topology = BasicTopology(nodes)
            return BasicPlatform(
                curNodeName = "node01",
                topology = topology,
                scheduler = scheduler,
                commandRunner = ShellCommandRunner(),
            )
        }

        @Throws(Exception::class)
        fun testExec(agent: Agent, expected: String?, expectedReturn: Boolean, spec: TaskSpec?) {
            val b = ByteArrayOutputStream()
            val p = PrintStream(b, true, Charsets.UTF_8.toString())
            val actualReturn = agent.exec(spec!!, p)
            assertEquals(expected, b.toString())
            assertEquals(expectedReturn, actualReturn)
        }
    }
}
