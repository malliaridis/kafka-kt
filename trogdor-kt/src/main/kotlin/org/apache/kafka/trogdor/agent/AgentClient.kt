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

import com.fasterxml.jackson.core.type.TypeReference
import java.time.OffsetDateTime
import javax.ws.rs.core.UriBuilder
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParser
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.trogdor.common.JsonUtil.objectFromCommandLineArgument
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.common.StringFormatter.dateString
import org.apache.kafka.trogdor.common.StringFormatter.durationString
import org.apache.kafka.trogdor.common.StringFormatter.prettyPrintGrid
import org.apache.kafka.trogdor.rest.AgentStatusResponse
import org.apache.kafka.trogdor.rest.CreateWorkerRequest
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest
import org.apache.kafka.trogdor.rest.Empty
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.apache.kafka.trogdor.rest.StopWorkerRequest
import org.apache.kafka.trogdor.rest.UptimeResponse
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * A client for the Trogdor agent.
 *
 * @property maxTries The maximum number of tries to make.
 * @property target The URL target.
 */
class AgentClient private constructor(
    private val log: Logger,
    val maxTries: Int,
    val target: String,
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("target"),
    )
    fun target(): String = target

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("maxTries"),
    )
    fun maxTries(): Int = maxTries

    private fun url(suffix: String): String = "http://$target$suffix"

    @Throws(Exception::class)
    fun status(): AgentStatusResponse {
        val response = JsonRestServer.httpRequest(
            url = url("/agent/status"),
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<AgentStatusResponse>() {},
            maxTries = maxTries,
        )
        return response.body()!!
    }

    @Throws(Exception::class)
    fun uptime(): UptimeResponse {
        val resp = JsonRestServer.httpRequest(
            url = url("/agent/uptime"),
            method = "GET",
            requestBodyData = null,
            responseFormat = object : TypeReference<UptimeResponse?>() {},
            maxTries = maxTries,
        )
        return resp.body()!!
    }

    @Throws(Exception::class)
    fun createWorker(request: CreateWorkerRequest) {
        val reponse =
            JsonRestServer.httpRequest(
                url = url("/agent/worker/create"),
                method = "POST",
                requestBodyData = request,
                responseFormat = object : TypeReference<Empty>() {},
                maxTries = maxTries,
            )
        reponse.body()
    }

    @Throws(Exception::class)
    fun stopWorker(request: StopWorkerRequest) {
        val resp = JsonRestServer.httpRequest(
            url = url("/agent/worker/stop"),
            method = "PUT",
            requestBodyData = request,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries,
        )
        resp.body()
    }

    @Throws(Exception::class)
    fun destroyWorker(request: DestroyWorkerRequest) {
        val uriBuilder = UriBuilder.fromPath(url("/agent/worker"))
        uriBuilder.queryParam("workerId", request.workerId())
        val response = JsonRestServer.httpRequest(
            url = uriBuilder.build().toString(),
            method = "DELETE",
            requestBodyData = null,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries,
        )
        response.body()
    }

    @Throws(Exception::class)
    fun invokeShutdown() {
        val response =JsonRestServer.httpRequest(
            url = url("/agent/shutdown"),
            method = "PUT",
            requestBodyData = null,
            responseFormat = object : TypeReference<Empty>() {},
            maxTries = maxTries,
        )
        response.body()
    }

    class Builder {

        private var log = LoggerFactory.getLogger(AgentClient::class.java)

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
            target = String.format("%s:%d", host, port)
            return this
        }

        fun build(): AgentClient {
            if (target == null) throw RuntimeException("You must specify a target.")
            return AgentClient(log, maxTries, target!!)
        }
    }

    companion object {

        private fun addTargetArgument(parser: ArgumentParser) {
            parser.addArgument("--target", "-t")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("target")
                .metavar("TARGET")
                .help("A colon-separated host and port pair.  For example, example.com:8888")
        }

        private fun addJsonArgument(parser: ArgumentParser) {
            parser.addArgument("--json")
                .action(Arguments.storeTrue())
                .dest("json")
                .metavar("JSON")
                .help("Show the full response as JSON.")
        }

        private fun addWorkerIdArgument(parser: ArgumentParser, help: String) {
            parser.addArgument("--workerId")
                .action(Arguments.storeTrue())
                .type(Long::class.java)
                .dest("workerId")
                .metavar("WORKER_ID")
                .help(help)
        }

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val rootParser = ArgumentParsers
                .newArgumentParser("trogdor-agent-client")
                .defaultHelp(true)
                .description("The Trogdor agent client.")
            val subParsers = rootParser.addSubparsers().dest("command")
            val uptimeParser = subParsers.addParser("uptime")
                .help("Get the agent uptime.")
            addTargetArgument(uptimeParser)
            addJsonArgument(uptimeParser)
            val statusParser = subParsers.addParser("status")
                .help("Get the agent status.")
            addTargetArgument(statusParser)
            addJsonArgument(statusParser)
            val createWorkerParser = subParsers.addParser("createWorker")
                .help("Create a new worker.")
            addTargetArgument(createWorkerParser)
            addWorkerIdArgument(createWorkerParser, "The worker ID to create.")
            createWorkerParser.addArgument("--taskId")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskId")
                .metavar("TASK_ID")
                .help("The task ID to create.")
            createWorkerParser.addArgument("--spec", "-s")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("taskSpec")
                .metavar("TASK_SPEC")
                .help("The task spec to create, or a path to a file containing the task spec.")
            val stopWorkerParser = subParsers.addParser("stopWorker")
                .help("Stop a worker.")
            addTargetArgument(stopWorkerParser)
            addWorkerIdArgument(stopWorkerParser, "The worker ID to stop.")
            val destroyWorkerParser = subParsers.addParser("destroyWorker")
                .help("Destroy a worker.")
            addTargetArgument(destroyWorkerParser)
            addWorkerIdArgument(destroyWorkerParser, "The worker ID to destroy.")
            val shutdownParser = subParsers.addParser("shutdown")
                .help("Shut down the agent.")
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
                        System.out.printf("Agent is running at %s.%n", target)
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
                    val status = client.status()
                    if (res.getBoolean("json")) println(toJsonString(status))
                    else {
                        System.out.printf("Agent is running at %s.%n", target)
                        System.out.printf(
                            "\tStart time: %s%n",
                            dateString(status.serverStartMs, localOffset)
                        )
                        val lines = mutableListOf<List<String>>()
                        val header = listOf("WORKER_ID", "TASK_ID", "STATE", "TASK_TYPE")
                        lines.add(header)
                        for ((key, value) in status.workers) {
                            val cols: MutableList<String> = ArrayList()
                            cols.add(key.toString())
                            cols.add(value.taskId())
                            cols.add(value.javaClass.getSimpleName())
                            cols.add(value.spec().javaClass.getCanonicalName())
                            lines.add(cols)
                        }
                        print(prettyPrintGrid(lines))
                    }
                }

                "createWorker" -> {
                    val workerId = res.getLong("workerId")
                    val taskId = res.getString("taskId")
                    val taskSpec = objectFromCommandLineArgument(
                        res.getString("taskSpec"),
                        TaskSpec::class.java
                    )
                    val req = CreateWorkerRequest(workerId, taskId, taskSpec)
                    client.createWorker(req)
                    System.out.printf("Sent CreateWorkerRequest for worker %d%n.", req.workerId())
                }

                "stopWorker" -> {
                    val workerId = res.getLong("workerId")
                    client.stopWorker(StopWorkerRequest(workerId))
                    System.out.printf("Sent StopWorkerRequest for worker %d%n.", workerId)
                }

                "destroyWorker" -> {
                    val workerId = res.getLong("workerId")
                    client.destroyWorker(DestroyWorkerRequest(workerId))
                    System.out.printf("Sent DestroyWorkerRequest for worker %d%n.", workerId)
                }

                "shutdown" -> {
                    client.invokeShutdown()
                    println("Sent ShutdownRequest.")
                }

                else -> {
                    println("You must choose an action. Type --help for help.")
                    exit(1)
                }
            }
        }
    }
}
