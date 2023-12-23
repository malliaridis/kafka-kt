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

package org.apache.kafka.trogdor.common

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.TreeMap
import org.apache.kafka.test.TestUtils
import org.apache.kafka.trogdor.agent.AgentClient
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.coordinator.CoordinatorClient
import org.apache.kafka.trogdor.rest.TaskState
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.WorkerState
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.LoggerFactory

class ExpectedTasks {

    private val expected = TreeMap<String, ExpectedTask>()

    fun addTask(task: ExpectedTask): ExpectedTasks {
        expected[task.id] = task
        return this
    }

    @Throws(InterruptedException::class)
    fun waitFor(client: CoordinatorClient): ExpectedTasks {
        TestUtils.waitForCondition(
            testCondition = {
                val tasks = try {
                    client.tasks(
                        TasksRequest(
                            taskIds = null,
                            firstStartMs = 0,
                            lastStartMs = 0,
                            firstEndMs = 0,
                            lastEndMs = 0,
                            state = null,
                        )
                    )
                } catch (e: Exception) {
                    log.info("Unable to get coordinator tasks", e)
                    throw RuntimeException(e)
                }
                val errors = StringBuilder()
                for ((id, task) in expected) {
                    val differences = task.compare(tasks.tasks()[id])
                    if (differences != null) errors.append(differences)
                }
                val errorString = errors.toString()
                if (errorString.isNotEmpty()) {
                    log.info("EXPECTED TASKS: {}", toJsonString(expected))
                    log.info("ACTUAL TASKS  : {}", toJsonString(tasks.tasks()))
                    log.info(errorString)
                    return@waitForCondition false
                }
                true
            },
            conditionDetails = "Timed out waiting for expected tasks " + toJsonString(expected),
        )
        return this
    }

    @Throws(InterruptedException::class)
    fun waitFor(client: AgentClient): ExpectedTasks {
        TestUtils.waitForCondition(
            testCondition = {
                val status = try {
                    client.status()
                } catch (exception: Exception) {
                    log.info("Unable to get agent status", exception)
                    throw RuntimeException(exception)
                }
                val errors = StringBuilder()
                val taskIdToWorkerState = mutableMapOf<String, WorkerState>()
                for (state in status.workers.values) {
                    taskIdToWorkerState[state.taskId()!!] = state
                }
                for ((id, worker) in expected)  {
                val differences = worker.compare(taskIdToWorkerState[id])
                if (differences != null) errors.append(differences)
            }
                val errorString = errors.toString()
                if (errorString.isNotEmpty()) {
                    log.info("EXPECTED WORKERS: {}", toJsonString(expected))
                    log.info("ACTUAL WORKERS  : {}", toJsonString(status.workers))
                    log.info(errorString)
                    return@waitForCondition false
                }
                true
            },
            conditionDetails = "Timed out waiting for expected workers " + toJsonString(expected),
        )
        return this
    }

    class ExpectedTaskBuilder(private val id: String) {

        private var taskSpec: TaskSpec? = null

        private var taskState: TaskState? = null

        private var workerState: WorkerState? = null

        fun taskSpec(taskSpec: TaskSpec?): ExpectedTaskBuilder {
            this.taskSpec = taskSpec
            return this
        }

        fun taskState(taskState: TaskState?): ExpectedTaskBuilder {
            this.taskState = taskState
            return this
        }

        fun workerState(workerState: WorkerState?): ExpectedTaskBuilder {
            this.workerState = workerState
            return this
        }

        fun build(): ExpectedTask = ExpectedTask(id, taskSpec, taskState, workerState)
    }

    class ExpectedTask @JsonCreator constructor(
        @param:JsonProperty("id") val id: String,
        @param:JsonProperty("taskSpec") private val taskSpec: TaskSpec?,
        @param:JsonProperty("taskState") private val taskState: TaskState?,
        @param:JsonProperty("workerState") private val workerState: WorkerState?,
    ) {
        fun compare(actual: TaskState?): String? {
            if (actual == null) return "Did not find task $id\n"

            if (taskSpec != null && actual.spec() != taskSpec)
                return "Invalid spec for task $id: expected $taskSpec, got ${actual.spec()}"

            return if (taskState != null && actual != taskState)
                "Invalid state for task $id: expected $taskState, got $actual"
            else null
        }

        fun compare(actual: WorkerState?): String? {
            return if (workerState != null && workerState != actual) {
                if (actual == null) "Did not find worker $id\n"
                else "Invalid state for task $id: expected $workerState, got $actual"
            } else null
        }

        @JsonProperty
        fun id(): String = id

        @JsonProperty
        fun taskSpec(): TaskSpec? = taskSpec

        @JsonProperty
        fun taskState(): TaskState? = taskState

        @JsonProperty
        fun workerState(): WorkerState? = workerState
    }

    companion object {
        private val log = LoggerFactory.getLogger(ExpectedTasks::class.java)
    }
}
