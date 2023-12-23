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

import com.fasterxml.jackson.core.type.TypeReference
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.TreeMap
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException
import javax.ws.rs.NotFoundException
import javax.ws.rs.core.UriBuilder
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParser
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.trogdor.common.JsonUtil.objectFromCommandLineArgument
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.common.JsonUtil.toPrettyJsonString
import org.apache.kafka.trogdor.common.StringFormatter.dateString
import org.apache.kafka.trogdor.common.StringFormatter.durationString
import org.apache.kafka.trogdor.common.StringFormatter.prettyPrintGrid
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse
import org.apache.kafka.trogdor.rest.CreateTaskRequest
import org.apache.kafka.trogdor.rest.DestroyTaskRequest
import org.apache.kafka.trogdor.rest.Empty
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.apache.kafka.trogdor.rest.RequestConflictException
import org.apache.kafka.trogdor.rest.StopTaskRequest
import org.apache.kafka.trogdor.rest.TaskDone
import org.apache.kafka.trogdor.rest.TaskPending
import org.apache.kafka.trogdor.rest.TaskRequest
import org.apache.kafka.trogdor.rest.TaskRunning
import org.apache.kafka.trogdor.rest.TaskState
import org.apache.kafka.trogdor.rest.TaskStateType
import org.apache.kafka.trogdor.rest.TaskStopping
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.TasksResponse
import org.apache.kafka.trogdor.rest.UptimeResponse
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * A client for the Trogdor coordinator.
 *
 * @property maxTries The maximum number of tries to make.
 * @property target The URL target.
 */
class CoordinatorClient private constructor(
    private val log: Logger,
    val maxTries: Int,
    private val target: String,
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("maxTries"),
    )
    fun maxTries(): Int = maxTries

    private fun url(suffix: String): String = "http://$target$suffix"

    @Throws(Exception::class)
    fun status(): CoordinatorStatusResponse {
        val response = JsonRestServer.httpRequest(
            url = url("/coordinator/status"),
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<CoordinatorStatusResponse>() {},
            maxTries = maxTries,
        )
        return response.body()!!
    }

    @Throws(Exception::class)
    fun uptime(): UptimeResponse {
        val response = JsonRestServer.httpRequest(
            url = url("/coordinator/uptime"),
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<UptimeResponse>() {},
            maxTries = maxTries,
        )
        return response.body()!!
    }

    @Throws(Exception::class)
    fun createTask(request: CreateTaskRequest) {
        val response = JsonRestServer.httpRequest(
            logger = log,
            url = url("/coordinator/task/create"),
            method = "POST",
            requestBodyData = request,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries,
        )
        response.body()
    }

    @Throws(Exception::class)
    fun stopTask(request: StopTaskRequest) {
        val response = JsonRestServer.httpRequest(
            logger = log,
            url = url("/coordinator/task/stop"),
            method = "PUT",
            requestBodyData = request,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries,
        )
        response.body()
    }

    @Throws(Exception::class)
    fun destroyTask(request: DestroyTaskRequest) {
        val uriBuilder = UriBuilder.fromPath(url("/coordinator/tasks"))
        uriBuilder.queryParam("taskId", request.id())
        val response = JsonRestServer.httpRequest(
            logger = log,
            url = uriBuilder.build().toString(),
            method = "DELETE",
            requestBodyData = null,
            responseFormat = object : TypeReference<Empty?>() {},
            maxTries = maxTries,
        )
        response.body()
    }

    @Throws(Exception::class)
    fun tasks(request: TasksRequest): TasksResponse {
        val uriBuilder = UriBuilder.fromPath(url("/coordinator/tasks"))
        uriBuilder.queryParam("taskId", *request.taskIds().toTypedArray())
        uriBuilder.queryParam("firstStartMs", request.firstStartMs())
        uriBuilder.queryParam("lastStartMs", request.lastStartMs())
        uriBuilder.queryParam("firstEndMs", request.firstEndMs())
        uriBuilder.queryParam("lastEndMs", request.lastEndMs())

        request.state()?.let { state -> uriBuilder.queryParam("state", state.toString()) }

        val resp = JsonRestServer.httpRequest(
            logger = log,
            url = uriBuilder.build().toString(),
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<TasksResponse?>() {},
            maxTries = maxTries,
        )
        return resp.body()!!
    }

    @Throws(Exception::class)
    fun task(request: TaskRequest): TaskState {
        val uri = UriBuilder.fromPath(url("/coordinator/tasks/{taskId}"))
            .build(request.taskId())
            .toString()

        val response = JsonRestServer.httpRequest(
            logger = log,
            url = uri,
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<TaskState>() {},
            maxTries = maxTries,
        )
        return response.body()!!
    }

    @Throws(Exception::class)
    fun shutdown() {
        val response = JsonRestServer.httpRequest(
            logger = log,
            url = url("/coordinator/shutdown"),
            method = "PUT",
            requestBodyData = null,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries
        )
        response.body()!!
    }

    class Builder {

        private var log = LoggerFactory.getLogger(CoordinatorClient::class.java)

        private var maxTries = 1

        private var target: String? = null

        fun log(log: Logger): Builder {
            this.log = log
            return this
        }

        fun maxTries(maxTries: Int): Builder {
            this.maxTries = maxTries
            return this
        }

        fun target(target: String?): Builder {
            this.target = target
            return this
        }

        fun target(host: String?, port: Int): Builder {
            target = "$host:$port"
            return this
        }

        fun build(): CoordinatorClient {
            if (target == null) throw RuntimeException("You must specify a target.")
            return CoordinatorClient(log, maxTries, target!!)
        }
    }

    companion object {

        private fun addTargetArgument(parser: ArgumentParser) = parser.addArgument("--target", "-t")
            .action(Arguments.store())
            .required(true)
            .type(String::class.java)
            .dest("target")
            .metavar("TARGET")
            .help("A colon-separated host and port pair.  For example, example.com:8889")

        private fun addJsonArgument(parser: ArgumentParser) = parser.addArgument("--json")
            .action(Arguments.storeTrue())
            .dest("json")
            .metavar("JSON")
            .help("Show the full response as JSON.")

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val rootParser = ArgumentParsers
                .newArgumentParser("trogdor-coordinator-client")
                .description("The Trogdor coordinator client.")
            val subParsers = rootParser.addSubparsers().dest("command")
            val uptimeParser = subParsers.addParser("uptime")
                .help("Get the coordinator uptime.")
            addTargetArgument(uptimeParser)
            addJsonArgument(uptimeParser)
            val statusParser = subParsers.addParser("status")
                .help("Get the coordinator status.")
            addTargetArgument(statusParser)
            addJsonArgument(statusParser)
            val showTaskParser = subParsers.addParser("showTask")
                .help("Show a coordinator task.")
            addTargetArgument(showTaskParser)
            addJsonArgument(showTaskParser)
            showTaskParser.addArgument("--id", "-i")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskId")
                .metavar("TASK_ID")
                .help("The task ID to show.")
            showTaskParser.addArgument("--verbose", "-v")
                .action(Arguments.storeTrue())
                .dest("verbose")
                .metavar("VERBOSE")
                .help("Print out everything.")
            showTaskParser.addArgument("--show-status", "-S")
                .action(Arguments.storeTrue())
                .dest("showStatus")
                .metavar("SHOW_STATUS")
                .help("Show the task status.")
            val showTasksParser = subParsers.addParser("showTasks")
                .help(
                    "Show many coordinator tasks.  By default, all tasks are shown, but " +
                            "command-line options can be specified as filters."
                )
            addTargetArgument(showTasksParser)
            addJsonArgument(showTasksParser)
            val idGroup = showTasksParser.addMutuallyExclusiveGroup()
            idGroup.addArgument("--id", "-i")
                .action(Arguments.append())
                .type(String::class.java)
                .dest("taskIds")
                .metavar("TASK_IDS")
                .help("Show only this task ID.  This option may be specified multiple times.")
            idGroup.addArgument("--id-pattern")
                .action(Arguments.store())
                .type(String::class.java)
                .dest("taskIdPattern")
                .metavar("TASK_ID_PATTERN")
                .help("Only display tasks which match the given ID pattern.")
            showTasksParser.addArgument("--state", "-s")
                .type(TaskStateType::class.java)
                .dest("taskStateType")
                .metavar("TASK_STATE_TYPE")
                .help("Show only tasks in this state.")
            val createTaskParser = subParsers.addParser("createTask")
                .help("Create a new task.")
            addTargetArgument(createTaskParser)
            createTaskParser.addArgument("--id", "-i")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskId")
                .metavar("TASK_ID")
                .help("The task ID to create.")
            createTaskParser.addArgument("--spec", "-s")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskSpec")
                .metavar("TASK_SPEC")
                .help("The task spec to create, or a path to a file containing the task spec.")
            val stopTaskParser = subParsers.addParser("stopTask")
                .help("Stop a task.")
            addTargetArgument(stopTaskParser)
            stopTaskParser.addArgument("--id", "-i")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskId")
                .metavar("TASK_ID")
                .help("The task ID to create.")
            val destroyTaskParser = subParsers.addParser("destroyTask")
                .help("Destroy a task.")
            addTargetArgument(destroyTaskParser)
            destroyTaskParser.addArgument("--id", "-i")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskId")
                .metavar("TASK_ID")
                .help("The task ID to destroy.")
            val shutdownParser = subParsers.addParser("shutdown")
                .help("Shut down the coordinator.")
            addTargetArgument(shutdownParser)
            val res = rootParser.parseArgsOrFail(args)
            val target = res.getString("target")
            val client = Builder().maxTries(3).target(target).build()
            val localOffset = OffsetDateTime.now().offset
            when (res.getString("command")) {
                "uptime" -> {
                    val uptime = client.uptime()
                    if (res.getBoolean("json")) {
                        println(toJsonString(uptime))
                    } else {
                        System.out.printf("Coordinator is running at %s.%n", target)
                        System.out.printf(
                            "\tStart time: %s%n",
                            dateString(uptime.serverStartMs(), localOffset)
                        )
                        System.out.printf(
                            "\tCurrent server time: %s%n",
                            dateString(uptime.nowMs(), localOffset)
                        )
                        System.out.printf(
                            "\tUptime: %s%n",
                            durationString(uptime.nowMs() - uptime.serverStartMs())
                        )
                    }
                }

                "status" -> {
                    val response = client.status()
                    if (res.getBoolean("json")) println(toJsonString(response))
                    else {
                        System.out.printf("Coordinator is running at %s.%n", target)
                        System.out.printf("\tStart time: %s%n", dateString(response.serverStartMs(), localOffset))
                    }
                }

                "showTask" -> {
                    val taskId = res.getString("taskId")
                    val req = TaskRequest(taskId)
                    lateinit var taskState: TaskState
                    try {
                        taskState = client.task(req)
                    } catch (exception: NotFoundException) {
                        System.out.printf("Task %s was not found.%n", taskId)
                        exit(1)
                    }
                    if (res.getBoolean("json")) {
                        println(toJsonString(taskState))
                    } else {
                        System.out.printf(
                            "Task %s of type %s is %s. %s%n", taskId,
                            taskState.spec().javaClass.getCanonicalName(),
                            taskState.stateType(), prettyPrintTaskInfo(taskState, localOffset)
                        )
                        if (taskState is TaskDone) {
                            val taskDone = taskState
                            if (taskDone.error().isNotEmpty()) System.out.printf("Error: %s%n", taskDone.error())
                        }
                        if (res.getBoolean("verbose"))
                            System.out.printf("Spec: %s%n%n", toPrettyJsonString(taskState.spec()))

                        if (res.getBoolean("verbose") || res.getBoolean("showStatus"))
                            System.out.printf("Status: %s%n%n", toPrettyJsonString(taskState.status()))
                    }
                }

                "showTasks" -> {
                    val taskStateType: TaskStateType? = res.get<TaskStateType>("taskStateType")
                    val taskIds = mutableListOf<String>()
                    var taskIdPattern: Pattern? = null

                    val taskIdList = res.getList<String>("taskIds")
                    val pattern = res.getString("taskIdPattern")
                    if (taskIdList != null) {
                        for (taskId in taskIdList)
                            taskIds.add(taskId as String)
                    } else if (pattern != null) try {
                        taskIdPattern = Pattern.compile(pattern)
                    } catch (exception: PatternSyntaxException) {
                        println("Invalid task ID regular expression $pattern")
                        exception.printStackTrace()
                        exit(1)
                    }

                    val req = TasksRequest(
                        taskIds = taskIds,
                        firstStartMs = 0,
                        lastStartMs = 0,
                        firstEndMs = 0,
                        lastEndMs = 0,
                        state = taskStateType,
                    )
                    var response = client.tasks(req)
                    if (taskIdPattern != null) {
                        val filteredTasks = TreeMap<String, TaskState>()
                        for ((key, value) in response.tasks()) {
                            if (taskIdPattern.matcher(key).matches())
                                filteredTasks[key] = value
                        }
                        response = TasksResponse(filteredTasks)
                    }
                    if (res.getBoolean("json")) println(toJsonString(response))
                    else println(prettyPrintTasksResponse(response, localOffset))

                    if (response.tasks().isEmpty()) exit(1)
                }

                "createTask" -> {
                    val taskId = res.getString("taskId")
                    val taskSpec =
                        objectFromCommandLineArgument(res.getString("taskSpec"), TaskSpec::class.java)
                    val req = CreateTaskRequest(taskId, taskSpec)
                    try {
                        client.createTask(req)
                        System.out.printf("Sent CreateTaskRequest for task %s.%n", req.id)
                    } catch (rce: RequestConflictException) {
                        System.out.printf(
                            "CreateTaskRequest for task ${req.id} got a 409 status code - a task with the same ID " +
                                    "but a different specification already exists.%nException: ${rce.message}%n",
                        )
                        exit(1)
                    }
                }

                "stopTask" -> {
                    val taskId = res.getString("taskId")
                    val req = StopTaskRequest(taskId)
                    client.stopTask(req)
                    System.out.printf("Sent StopTaskRequest for task %s.%n", taskId)
                }

                "destroyTask" -> {
                    val taskId = res.getString("taskId")
                    val req = DestroyTaskRequest(taskId)
                    client.destroyTask(req)
                    System.out.printf("Sent DestroyTaskRequest for task %s.%n", taskId)
                }

                "shutdown" -> {
                    client.shutdown()
                    println("Sent ShutdownRequest.")
                }

                else -> {
                    println("You must choose an action. Type --help for help.")
                    exit(1)
                }
            }
        }

        fun prettyPrintTasksResponse(response: TasksResponse, zoneOffset: ZoneOffset?): String {
            if (response.tasks().isEmpty()) {
                return "No matching tasks found."
            }
            val lines = mutableListOf<List<String>>()
            val header = listOf("ID", "TYPE", "STATE", "INFO")
            lines.add(header)
            for ((taskId, taskState) in response.tasks()) {
                val cols = listOf(
                    taskId,
                    taskState.spec().javaClass.getCanonicalName(),
                    taskState.stateType().toString(),
                    prettyPrintTaskInfo(taskState, zoneOffset),
                )
                lines.add(cols)
            }
            return prettyPrintGrid(lines)
        }

        fun prettyPrintTaskInfo(taskState: TaskState, zoneOffset: ZoneOffset?): String = when (taskState) {
            is TaskPending -> "Will start at ${dateString(taskState.spec().startMs(), zoneOffset)}"
            is TaskRunning -> {
                "Started ${dateString(taskState.startedMs(), zoneOffset)}; will stop after " +
                        durationString(taskState.spec().durationMs())
            }

            is TaskStopping -> "Started ${dateString(taskState.startedMs(), zoneOffset)}"

            is TaskDone -> {
                val status = if (taskState.error().isEmpty()) {
                    if (taskState.cancelled()) "CANCELLED"
                    else "FINISHED"
                } else "FAILED"

                "$status at ${dateString(taskState.doneMs(), zoneOffset)} after " +
                        durationString(taskState.doneMs() - taskState.startedMs())
            }

            else -> throw RuntimeException("Unknown task state type ${taskState!!.stateType()}")
        }
    }
}
