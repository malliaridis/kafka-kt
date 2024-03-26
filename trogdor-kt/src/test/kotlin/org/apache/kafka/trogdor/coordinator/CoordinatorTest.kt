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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import java.util.concurrent.TimeUnit
import javax.ws.rs.NotFoundException
import org.apache.kafka.common.utils.MockScheduler
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.test.TestUtils
import org.apache.kafka.trogdor.common.CapturingCommandRunner
import org.apache.kafka.trogdor.common.ExpectedTasks
import org.apache.kafka.trogdor.common.ExpectedTasks.ExpectedTaskBuilder
import org.apache.kafka.trogdor.common.MiniTrogdorCluster
import org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse
import org.apache.kafka.trogdor.rest.CreateTaskRequest
import org.apache.kafka.trogdor.rest.DestroyTaskRequest
import org.apache.kafka.trogdor.rest.RequestConflictException
import org.apache.kafka.trogdor.rest.StopTaskRequest
import org.apache.kafka.trogdor.rest.TaskDone
import org.apache.kafka.trogdor.rest.TaskPending
import org.apache.kafka.trogdor.rest.TaskRequest
import org.apache.kafka.trogdor.rest.TaskRunning
import org.apache.kafka.trogdor.rest.TaskStateType
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.UptimeResponse
import org.apache.kafka.trogdor.rest.WorkerDone
import org.apache.kafka.trogdor.rest.WorkerRunning
import org.apache.kafka.trogdor.task.NoOpTaskSpec
import org.apache.kafka.trogdor.task.SampleTaskSpec
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@Tag("integration")
@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class CoordinatorTest {

    @Test
    @Throws(Exception::class)
    fun testCoordinatorStatus() {
        MiniTrogdorCluster.Builder().addCoordinator("node01")
            .build()
            .use { cluster ->
                val status: CoordinatorStatusResponse = cluster.coordinatorClient().status()
                assertEquals(cluster.coordinator!!.status(), status)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testCoordinatorUptime() {
        val time = MockTime(0, 200, 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val uptime: UptimeResponse = cluster.coordinatorClient().uptime()
                assertEquals(cluster.coordinator!!.uptime(), uptime)
                time.setCurrentTimeMs(250)
                assertNotEquals(cluster.coordinator.uptime(), uptime)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTask() {
        val time = MockTime(0, 0, 0)
        val scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                ExpectedTasks().waitFor(cluster.coordinatorClient())
                val fooSpec: NoOpTaskSpec = NoOpTaskSpec(1, 2)
                cluster.coordinatorClient().createTask(
                    CreateTaskRequest("foo", fooSpec)
                )
                ExpectedTasks().addTask(ExpectedTaskBuilder("foo").taskState(TaskPending(fooSpec)).build())
                    .waitFor(cluster.coordinatorClient())

                // Re-creating a task with the same arguments is not an error.
                cluster.coordinatorClient().createTask(
                    CreateTaskRequest("foo", fooSpec)
                )

                // Re-creating a task with different arguments gives a RequestConflictException.
                val barSpec = NoOpTaskSpec(1000, 2000)
                assertFailsWith<RequestConflictException>(
                    message = "Recreating task with different task spec is not allowed",
                ) {
                    cluster.coordinatorClient().createTask(CreateTaskRequest("foo", barSpec))
                }
                time.sleep(2)
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskRunning(
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).workerState(
                        WorkerRunning(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        ),
                    ).build()
                ).waitFor(cluster.coordinatorClient())
                    .waitFor(cluster.agentClient("node02"))
                time.sleep(3)
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskDone(
                            spec = fooSpec,
                            startedMs = 2,
                            doneMs = 5,
                            error = "",
                            cancelled = false,
                            status = TextNode("done"),
                        )
                    ).build()
                ).waitFor(cluster.coordinatorClient())
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskDistribution() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val agentClient1 = cluster.agentClient("node01")
                val agentClient2 = cluster.agentClient("node02")
                ExpectedTasks()
                    .waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)

                val fooSpec = NoOpTaskSpec(startMs = 5, durationMs = 7)
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskPending(fooSpec))
                        .build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
                time.sleep(11)
                val status1 = ObjectNode(JsonNodeFactory.instance)
                status1.set<JsonNode>("node01", TextNode("active"))
                status1.set<JsonNode>("node02", TextNode("active"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(spec = fooSpec, startedMs = 11, status = status1))
                        .workerState(
                            WorkerRunning(
                                taskId = "foo",
                                spec = fooSpec,
                                startedMs = 11,
                                status = TextNode("active"),
                            )
                        ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)

                time.sleep(7)
                val status2 = ObjectNode(JsonNodeFactory.instance)
                status2.set<JsonNode>("node01", TextNode("done"))
                status2.set<JsonNode>("node02", TextNode("done"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskDone(
                            spec = fooSpec,
                            startedMs = 11,
                            doneMs = 18,
                            error = "",
                            cancelled = false,
                            status = status2,
                        )
                    ).workerState(
                        WorkerDone(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 11,
                            doneMs = 18,
                            status = TextNode("done"),
                            error = "",
                        )
                    ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskCancellation() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val agentClient1 = cluster.agentClient("node01")
                val agentClient2 = cluster.agentClient("node02")
                ExpectedTasks().waitFor(coordinatorClient).waitFor(agentClient1).waitFor(agentClient2)
                val fooSpec = NoOpTaskSpec(startMs = 5, durationMs = 7)
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))

                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(TaskPending(fooSpec)).build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)

                time.sleep(11)
                val status1 = ObjectNode(JsonNodeFactory.instance)
                status1.set<JsonNode>("node01", TextNode("active"))
                status1.set<JsonNode>("node02", TextNode("active"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(fooSpec, 11, status1))
                        .workerState(
                            WorkerRunning(
                                taskId = "foo",
                                spec = fooSpec,
                                startedMs = 11,
                                status = TextNode("active"),
                            )
                        ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
                val status2 = ObjectNode(JsonNodeFactory.instance)
                status2.set<JsonNode>("node01", TextNode("done"))
                status2.set<JsonNode>("node02", TextNode("done"))
                time.sleep(7)
                coordinatorClient.stopTask(StopTaskRequest("foo"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskDone(
                            spec = fooSpec,
                            startedMs = 11,
                            doneMs = 18,
                            error = "",
                            cancelled = true,
                            status = status2,
                        )
                    ).workerState(
                        WorkerDone(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 11,
                            doneMs = 18,
                            status = TextNode("done"),
                            error = "",
                        )
                    ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)

                coordinatorClient.destroyTask(DestroyTaskRequest("foo"))
                ExpectedTasks().waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskDestruction() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val agentClient1 = cluster.agentClient("node01")
                val agentClient2 = cluster.agentClient("node02")
                ExpectedTasks().waitFor(coordinatorClient).waitFor(agentClient1).waitFor(agentClient2)
                val fooSpec = NoOpTaskSpec(2, 12)
                coordinatorClient.destroyTask(DestroyTaskRequest("foo"))
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                val barSpec = NoOpTaskSpec(20, 20)
                coordinatorClient.createTask(CreateTaskRequest("bar", barSpec))
                coordinatorClient.destroyTask(DestroyTaskRequest("bar"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskPending(fooSpec))
                        .build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
                time.sleep(10)
                val status1 = ObjectNode(JsonNodeFactory.instance)
                status1.set<JsonNode>("node01", TextNode("active"))
                status1.set<JsonNode>("node02", TextNode("active"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(fooSpec, 10, status1))
                        .build()
                ).waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
                coordinatorClient.destroyTask(DestroyTaskRequest("foo"))
                ExpectedTasks().waitFor(coordinatorClient)
                    .waitFor(agentClient1)
                    .waitFor(agentClient2)
            }
    }

    class ExpectedLines {

        var expectedLines = mutableListOf<String>()

        fun addLine(line: String): ExpectedLines {
            expectedLines.add(line)
            return this
        }

        @Throws(InterruptedException::class)
        fun waitFor(
            nodeName: String,
            runner: CapturingCommandRunner,
        ): ExpectedLines {
            TestUtils.waitForCondition(
                testCondition = { linesMatch(nodeName, runner.lines(nodeName)) },
                conditionDetails = "failed to find the expected lines $this",
            )
            return this
        }

        private fun linesMatch(nodeName: String, actualLines: List<String>): Boolean {
            var matchIdx = 0
            var i = 0
            while (true) {
                if (matchIdx == expectedLines.size) {
                    log.debug("Got expected lines for {}", nodeName)
                    return true
                }
                if (i == actualLines.size) {
                    log.info(
                        "Failed to find the expected lines for {}.  First " +
                                "missing line on index {}: {}",
                        nodeName, matchIdx, expectedLines[matchIdx]
                    )
                    return false
                }
                val actualLine = actualLines[i++]
                val expectedLine = expectedLines[matchIdx]
                if (expectedLine == actualLine) matchIdx++
                else {
                    log.trace("Expected:\n'{}', Got:\n'{}'", expectedLine, actualLine)
                    matchIdx = 0
                }
            }
        }

        override fun toString(): String = expectedLines.joinToString()
    }

    @Test
    @Throws(Exception::class)
    fun testNetworkPartitionFault() {
        val runner = CapturingCommandRunner()
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node01")
            .addAgent("node02")
            .addAgent("node03")
            .commandRunner(runner)
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val spec = NetworkPartitionFaultSpec(
                    startMs = 0,
                    durationMs = Long.MAX_VALUE,
                    partitions = listOf(
                        listOf("node01", "node02"),
                        listOf("node03"),
                    )
                )
                coordinatorClient.createTask(CreateTaskRequest("netpart", spec))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("netpart")
                        .taskSpec(spec)
                        .build()
                ).waitFor(coordinatorClient)
                checkLines("-A", runner)
            }
        checkLines("-D", runner)
    }

    @Throws(InterruptedException::class)
    private fun checkLines(prefix: String, runner: CapturingCommandRunner) {
        ExpectedLines()
            .addLine("sudo iptables $prefix INPUT -p tcp -s 127.0.0.1 -j DROP -m comment --comment node03")
            .waitFor("node01", runner)
        ExpectedLines()
            .addLine("sudo iptables $prefix INPUT -p tcp -s 127.0.0.1 -j DROP -m comment --comment node03")
            .waitFor("node02", runner)
        ExpectedLines()
            .addLine("sudo iptables $prefix INPUT -p tcp -s 127.0.0.1 -j DROP -m comment --comment node01")
            .addLine("sudo iptables $prefix INPUT -p tcp -s 127.0.0.1 -j DROP -m comment --comment node02")
            .waitFor("node03", runner)
    }

    @Test
    @Throws(Exception::class)
    fun testTasksRequestMatches() {
        val req1 = TasksRequest(
            taskIds = null,
            firstStartMs = 0,
            lastStartMs = 0,
            firstEndMs = 0,
            lastEndMs = 0,
            state = null
        )
        assertTrue(req1.matches(taskId = "foo1", startMs = -1, endMs = -1, state = TaskStateType.PENDING))
        assertTrue(req1.matches(taskId = "bar1", startMs = 100, endMs = 200, state = TaskStateType.DONE))
        assertTrue(req1.matches(taskId = "baz1", startMs = 100, endMs = -1, state = TaskStateType.RUNNING))

        val req2 = TasksRequest(
            taskIds = null,
            firstStartMs = 100,
            lastStartMs = 0,
            firstEndMs = 0,
            lastEndMs = 0,
            state = null
        )
        assertFalse(req2.matches(taskId = "foo1", startMs = -1, endMs = -1, state = TaskStateType.PENDING))
        assertTrue(req2.matches(taskId = "bar1", startMs = 100, endMs = 200, state = TaskStateType.DONE))
        assertFalse(req2.matches(taskId = "bar1", startMs = 99, endMs = 200, state = TaskStateType.DONE))
        assertFalse(req2.matches(taskId = "baz1", startMs = 99, endMs = -1, state = TaskStateType.RUNNING))

        val req3 = TasksRequest(
            taskIds = null,
            firstStartMs = 200,
            lastStartMs = 900,
            firstEndMs = 200,
            lastEndMs = 900,
            state = null
        )
        assertFalse(req3.matches(taskId = "foo1", startMs = -1, endMs = -1, state = TaskStateType.PENDING))
        assertFalse(req3.matches(taskId = "bar1", startMs = 100, endMs = 200, state = TaskStateType.DONE))
        assertFalse(req3.matches(taskId = "bar1", startMs = 200, endMs = 1000, state = TaskStateType.DONE))
        assertTrue(req3.matches(taskId = "bar1", startMs = 200, endMs = 700, state = TaskStateType.DONE))
        assertFalse(req3.matches(taskId = "baz1", startMs = 101, endMs = -1, state = TaskStateType.RUNNING))

        val taskIds = listOf("foo1", "bar1", "baz1")
        val req4 = TasksRequest(
            taskIds = taskIds,
            firstStartMs = 1000,
            lastStartMs = -1,
            firstEndMs = -1,
            lastEndMs = -1,
            state = null,
        )
        assertFalse(req4.matches(taskId = "foo1", startMs = -1, endMs = -1, state = TaskStateType.PENDING))
        assertTrue(req4.matches(taskId = "foo1", startMs = 1000, endMs = -1, state = TaskStateType.RUNNING))
        assertFalse(req4.matches(taskId = "foo1", startMs = 900, endMs = -1, state = TaskStateType.RUNNING))
        assertFalse(req4.matches(taskId = "baz2", startMs = 2000, endMs = -1, state = TaskStateType.RUNNING))
        assertFalse(req4.matches(taskId = "baz2", startMs = -1, endMs = -1, state = TaskStateType.PENDING))

        val req5 = TasksRequest(
            taskIds = null,
            firstStartMs = 0,
            lastStartMs = 0,
            firstEndMs = 0,
            lastEndMs = 0,
            state = TaskStateType.RUNNING,
        )
        assertTrue(req5.matches(taskId = "foo1", startMs = -1, endMs = -1, state = TaskStateType.RUNNING))
        assertFalse(req5.matches(taskId = "bar1", startMs = -1, endMs = -1, state = TaskStateType.DONE))
        assertFalse(req5.matches(taskId = "baz1", startMs = -1, endMs = -1, state = TaskStateType.STOPPING))
        assertFalse(req5.matches(taskId = "baz1", startMs = -1, endMs = -1, state = TaskStateType.PENDING))
    }

    @Test
    @Throws(Exception::class)
    fun testTasksRequest() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                ExpectedTasks().waitFor(coordinatorClient)
                val fooSpec = NoOpTaskSpec(startMs = 1, durationMs = 10)
                val barSpec = NoOpTaskSpec(startMs = 3, durationMs = 1)
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                coordinatorClient.createTask(CreateTaskRequest("bar", barSpec))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskPending(fooSpec))
                        .build()
                ).addTask(
                    ExpectedTaskBuilder("bar")
                        .taskState(TaskPending(barSpec))
                        .build()
                ).waitFor(coordinatorClient)
                assertEquals(
                    expected = 0,
                    actual = coordinatorClient.tasks(
                        TasksRequest(
                            taskIds = null,
                            firstStartMs = 10,
                            lastStartMs = 0,
                            firstEndMs = 10,
                            lastEndMs = 0,
                            state = null,
                        )
                    ).tasks().size,
                )
                val resp1 = coordinatorClient.tasks(
                    TasksRequest(
                        taskIds = mutableListOf("foo", "baz"),
                        firstStartMs = 0,
                        lastStartMs = 0,
                        firstEndMs = 0,
                        lastEndMs = 0,
                        state = null,
                    )
                )
                assertTrue(resp1.tasks().containsKey("foo"))
                assertFalse(resp1.tasks().containsKey("bar"))
                assertEquals(1, resp1.tasks().size)
                time.sleep(2)

                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskRunning(
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).workerState(
                        WorkerRunning(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).build()
                ).addTask(
                    ExpectedTaskBuilder("bar")
                        .taskState(TaskPending(barSpec))
                        .build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))

                val resp2 = coordinatorClient.tasks(
                    TasksRequest(
                        taskIds = null,
                        firstStartMs = 1,
                        lastStartMs = 0,
                        firstEndMs = 0,
                        lastEndMs = 0,
                        state = null,
                    )
                )
                assertTrue(resp2.tasks().containsKey("foo"))
                assertFalse(resp2.tasks().containsKey("bar"))
                assertEquals(1, resp2.tasks().size)
                assertEquals(
                    expected = 0,
                    actual = coordinatorClient.tasks(
                        TasksRequest(
                            taskIds = null,
                            firstStartMs = 3,
                            lastStartMs = 0,
                            firstEndMs = 0,
                            lastEndMs = 0,
                            state = null,
                        )
                    ).tasks().size,
                )
            }
    }

    /**
     * If an agent fails in the middle of a task and comes back up when the task is considered expired,
     * we want the task to be marked as DONE and not re-sent should a second failure happen.
     */
    @Test
    @Throws(Exception::class)
    fun testAgentFailureAndTaskExpiry() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler: Scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val fooSpec = NoOpTaskSpec(startMs = 1, durationMs = 500)
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                val expectedState = ExpectedTaskBuilder("foo")
                    .taskState(TaskPending(fooSpec))
                    .build()
                    .taskState()
                val resp = coordinatorClient.task(TaskRequest("foo"))
                assertEquals(expectedState, resp)
                time.sleep(2)
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskRunning(
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).workerState(
                        WorkerRunning(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))
                cluster.restartAgent("node02")
                time.sleep(550)
                // coordinator heartbeat sees that the agent is back up, re-schedules the task but the agent expires it
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskDone(
                            spec = fooSpec,
                            startedMs = 2,
                            doneMs = 552,
                            error = "worker expired",
                            cancelled = false,
                            status = null,
                        )
                    ).workerState(
                        WorkerDone(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 552,
                            doneMs = 552,
                            status = null,
                            error = "worker expired",
                        )
                    ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))
                cluster.restartAgent("node02")
                // coordinator heartbeat sees that the agent is back up but does not re-schedule the task as it is DONE
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(
                            TaskDone(
                                spec = fooSpec,
                                startedMs = 2,
                                doneMs = 552,
                                error = "worker expired",
                                cancelled = false,
                                status = null,
                            )
                        ).build()  // no worker states
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskRequestWithOldStartMsGetsUpdated() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val fooSpec = NoOpTaskSpec(startMs = 1, durationMs = 500)
                time.sleep(552)

                val coordinatorClient = cluster.coordinatorClient()
                val updatedSpec = NoOpTaskSpec(startMs = 552, durationMs = 500)
                coordinatorClient.createTask(CreateTaskRequest("fooSpec", fooSpec))
                val expectedState = ExpectedTaskBuilder("fooSpec")
                    .taskState(
                        TaskRunning(
                            spec = updatedSpec,
                            startedMs = 552,
                            status = TextNode("receiving"),
                        )
                    ).build()
                    .taskState()
                val resp = coordinatorClient.task(TaskRequest("fooSpec"))
                assertEquals(expectedState, resp)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskRequestWithFutureStartMsDoesNotGetRun() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val fooSpec = NoOpTaskSpec(1000, 500)
                time.sleep(999)
                val coordinatorClient = cluster.coordinatorClient()
                coordinatorClient.createTask(CreateTaskRequest("fooSpec", fooSpec))
                val expectedState = ExpectedTaskBuilder("fooSpec")
                    .taskState(TaskPending(fooSpec))
                    .build()
                    .taskState()
                val resp = coordinatorClient.task(TaskRequest("fooSpec"))
                assertEquals(expectedState, resp)
            }
    }

    @Test
    @Throws(Exception::class)
    fun testTaskRequest() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                val fooSpec = NoOpTaskSpec(startMs = 1, durationMs = 10)
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                val expectedState = ExpectedTaskBuilder("foo")
                    .taskState(TaskPending(fooSpec))
                    .build()
                    .taskState()
                val resp = coordinatorClient.task(TaskRequest("foo"))
                assertEquals(expectedState, resp)
                time.sleep(2)
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo").taskState(
                        TaskRunning(
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).workerState(
                        WorkerRunning(
                            taskId = "foo",
                            spec = fooSpec,
                            startedMs = 2,
                            status = TextNode("active"),
                        )
                    ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))
                assertFailsWith<NotFoundException> {
                    coordinatorClient.task(TaskRequest("non-existent-foo"))
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun testWorkersExitingAtDifferentTimes() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val scheduler = MockScheduler(time)
        MiniTrogdorCluster.Builder()
            .addCoordinator("node01")
            .addAgent("node02")
            .addAgent("node03")
            .scheduler(scheduler)
            .build()
            .use { cluster ->
                val coordinatorClient = cluster.coordinatorClient()
                ExpectedTasks().waitFor(coordinatorClient)
                val nodeToExitMs = mapOf(
                    "node02" to 10L,
                    "node03" to 20L,
                )
                val fooSpec = SampleTaskSpec(startMs = 2, durationMs = 100, nodeToExitMs = nodeToExitMs, error = "")
                coordinatorClient.createTask(CreateTaskRequest("foo", fooSpec))
                ExpectedTasks().addTask(ExpectedTaskBuilder("foo")
                    .taskState(TaskPending(fooSpec)).build())
                    .waitFor(coordinatorClient)
                time.sleep(2)
                val status1 = ObjectNode(JsonNodeFactory.instance)
                status1.set<JsonNode>("node02", TextNode("active"))
                status1.set<JsonNode>("node03", TextNode("active"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(fooSpec, 2, status1))
                        .workerState(
                            WorkerRunning(
                                taskId = "foo",
                                spec = fooSpec,
                                startedMs = 2,
                                status = TextNode("active"),
                            )
                        ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node02"))
                    .waitFor(cluster.agentClient("node03"))
                time.sleep(10)
                val status2 = ObjectNode(JsonNodeFactory.instance)
                status2.set<JsonNode>("node02", TextNode("halted"))
                status2.set<JsonNode>("node03", TextNode("active"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(fooSpec, 2, status2))
                        .workerState(
                            WorkerRunning(
                                taskId = "foo",
                                spec = fooSpec,
                                startedMs = 2,
                                status = TextNode("active"),
                            )
                        ).build()
                ).waitFor(coordinatorClient)
                    .waitFor(cluster.agentClient("node03"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(TaskRunning(fooSpec, 2, status2))
                        .workerState(
                            WorkerDone(
                                taskId = "foo",
                                spec = fooSpec,
                                startedMs = 2,
                                doneMs = 12,
                                status = TextNode("halted"),
                                error = "",
                            )
                        ).build()
                ).waitFor(cluster.agentClient("node02"))
                time.sleep(10)
                val status3 = ObjectNode(JsonNodeFactory.instance)
                status3.set<JsonNode>("node02", TextNode("halted"))
                status3.set<JsonNode>("node03", TextNode("halted"))
                ExpectedTasks().addTask(
                    ExpectedTaskBuilder("foo")
                        .taskState(
                            TaskDone(
                                spec = fooSpec,
                                startedMs = 2,
                                doneMs = 22,
                                error = "",
                                cancelled = false,
                                status = status3,
                            )
                        ).build()
                ).waitFor(coordinatorClient)
            }
    }

    companion object {
        private val log = LoggerFactory.getLogger(CoordinatorTest::class.java)
    }
}
