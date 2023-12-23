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

import java.util.concurrent.ThreadLocalRandom
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.utils.Exit.addShutdownHook
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.Time
import org.apache.kafka.trogdor.common.Node.Util.getTrogdorCoordinatorPort
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.Platform.Config.parse
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse
import org.apache.kafka.trogdor.rest.CreateTaskRequest
import org.apache.kafka.trogdor.rest.DestroyTaskRequest
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.apache.kafka.trogdor.rest.StopTaskRequest
import org.apache.kafka.trogdor.rest.TaskRequest
import org.apache.kafka.trogdor.rest.TaskState
import org.apache.kafka.trogdor.rest.TasksRequest
import org.apache.kafka.trogdor.rest.TasksResponse
import org.apache.kafka.trogdor.rest.UptimeResponse
import org.slf4j.LoggerFactory

/**
 * The Trogdor coordinator.
 *
 * The coordinator manages the agent processes in the cluster.
 *
 * @constructor Create a new Coordinator.
 * @param platform The platform object to use.
 * @param scheduler The scheduler to use for this Coordinator.
 * @property restServer The REST server to use.
 * @param resource The AgentRestResource to use.
 */
class Coordinator(
    platform: Platform,
    scheduler: Scheduler,
    private val restServer: JsonRestServer,
    resource: CoordinatorRestResource,
    firstWorkerId: Long,
) {

    private val time: Time = scheduler.time()

    /**
     * The start time of the Coordinator in milliseconds.
     */
    private val startTimeMs: Long = time.milliseconds()

    /**
     * The task manager.
     */
    private val taskManager: TaskManager = TaskManager(platform, scheduler, firstWorkerId)

    init {
        resource.setCoordinator(this)
    }

    fun port(): Int = restServer.port()

    @Throws(Exception::class)
    fun status(): CoordinatorStatusResponse = CoordinatorStatusResponse(startTimeMs)

    fun uptime(): UptimeResponse = UptimeResponse(startTimeMs, time.milliseconds())

    @Throws(Throwable::class)
    fun createTask(request: CreateTaskRequest) =
        taskManager.createTask(request.id, request.spec)

    @Throws(Throwable::class)
    fun stopTask(request: StopTaskRequest) = taskManager.stopTask(request.id())

    @Throws(Throwable::class)
    fun destroyTask(request: DestroyTaskRequest) = taskManager.destroyTask(request.id())

    @Throws(Exception::class)
    fun tasks(request: TasksRequest): TasksResponse = taskManager.tasks(request)

    @Throws(Exception::class)
    fun task(request: TaskRequest): TaskState? = taskManager.task(request)

    @Throws(Exception::class)
    fun beginShutdown(stopAgents: Boolean) {
        restServer.beginShutdown()
        taskManager.beginShutdown(stopAgents)
    }

    @Throws(Exception::class)
    fun waitForShutdown() {
        restServer.waitForShutdown()
        taskManager.waitForShutdown()
    }

    companion object {

        private val log = LoggerFactory.getLogger(Coordinator::class.java)

        const val DEFAULT_PORT = 8889

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val parser = ArgumentParsers
                .newArgumentParser("trogdor-coordinator")
                .defaultHelp(true)
                .description("The Trogdor fault injection coordinator")
            parser.addArgument("--coordinator.config", "-c")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("config")
                .metavar("CONFIG")
                .help("The configuration file to use.")
            parser.addArgument("--node-name", "-n")
                .action(Arguments.store())
                .required(true)
                .type(String::class.java)
                .dest("node_name")
                .metavar("NODE_NAME")
                .help("The name of this node.")
            var res: Namespace? = null
            try {
                res = parser.parseArgs(args)
            } catch (e: ArgumentParserException) {
                if (args.isEmpty()) {
                    parser.printHelp()
                    exit(0)
                } else {
                    parser.handleError(e)
                    exit(1)
                }
            }
            val configPath = res!!.getString("config")
            val nodeName = res.getString("node_name")
            val platform = parse(nodeName, configPath)
            val restServer = JsonRestServer(
                getTrogdorCoordinatorPort(platform.curNode())
            )
            val resource = CoordinatorRestResource()
            log.info("Starting coordinator process.")
            val coordinator = Coordinator(
                platform, Scheduler.SYSTEM,
                restServer, resource, ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2)
            )
            restServer.start(resource)
            addShutdownHook("coordinator-shutdown-hook") {
                log.warn("Running coordinator shutdown hook.")
                try {
                    coordinator.beginShutdown(false)
                    coordinator.waitForShutdown()
                } catch (e: Exception) {
                    log.error("Got exception while running coordinator shutdown hook.", e)
                }
            }
            coordinator.waitForShutdown()
        }
    }
}
