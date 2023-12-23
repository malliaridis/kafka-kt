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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.PrintStream
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.utils.Exit.addShutdownHook
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.rest.AgentStatusResponse
import org.apache.kafka.trogdor.rest.CreateWorkerRequest
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.apache.kafka.trogdor.rest.StopWorkerRequest
import org.apache.kafka.trogdor.rest.UptimeResponse
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.slf4j.LoggerFactory
import kotlin.math.max

/**
 * The Trogdor agent.
 *
 * The agent process runs tasks.
 *
 * @constructor Create a new Agent.
 * @property platform The platform object to use for this agent.
 * @param scheduler The scheduler to use for this Agent.
 * @property restServer The REST server to use.
 * @param resource The AgentRestResource to use.
 */
class Agent(
    private val platform: Platform,
    scheduler: Scheduler,
    private val restServer: JsonRestServer,
    resource: AgentRestResource,
) {

    private val time: Time = scheduler.time()

    /**
     * The time at which this server was started.
     */
    private val serverStartMs: Long = time.milliseconds()

    /**
     * The WorkerManager.
     */
    private val workerManager: WorkerManager = WorkerManager(platform, scheduler)

    init {
        resource.setAgent(this)
    }

    fun port(): Int = restServer.port()

    @Throws(Exception::class)
    fun beginShutdown() {
        restServer.beginShutdown()
        workerManager.beginShutdown()
    }

    @Throws(Exception::class)
    fun waitForShutdown() {
        restServer.waitForShutdown()
        workerManager.waitForShutdown()
    }

    @Throws(Exception::class)
    fun status(): AgentStatusResponse = AgentStatusResponse(serverStartMs, workerManager.workerStates())

    fun uptime(): UptimeResponse = UptimeResponse(serverStartMs, time.milliseconds())

    @Throws(Throwable::class)
    fun createWorker(request: CreateWorkerRequest) {
        workerManager.createWorker(request.workerId(), request.taskId(), request.spec())
    }

    @Throws(Throwable::class)
    fun stopWorker(request: StopWorkerRequest) {
        workerManager.stopWorker(request.workerId(), false)
    }

    @Throws(Throwable::class)
    fun destroyWorker(request: DestroyWorkerRequest) {
        workerManager.stopWorker(request.workerId(), true)
    }

    /**
     * Rebase the task spec time so that it is not earlier than the current time. This is only needed for tasks
     * passed in with --exec. Normally, the controller rebases the task spec time.
     */
    @Throws(Exception::class)
    fun rebaseTaskSpecTime(spec: TaskSpec): TaskSpec {
        val node: ObjectNode = JsonUtil.JSON_SERDE.valueToTree(spec)
        node.set<JsonNode>("startMs", LongNode(max(time.milliseconds(), spec.startMs())))
        return JsonUtil.JSON_SERDE.treeToValue(node, TaskSpec::class.java)
    }

    /**
     * Start a task on the agent, and block until it completes.
     *
     * @param spec The task specification.
     * @param out The output stream to print to.
     * @return `true` if the task run successfully; `false` otherwise.
     */
    @Throws(Exception::class)
    fun exec(spec: TaskSpec, out: PrintStream): Boolean {
        val controller: TaskController
        try {
            controller = spec.newController(EXEC_TASK_ID)
        } catch (exception: Exception) {
            out.println("Unable to create the task controller.")
            exception.printStackTrace(out)
            return false
        }
        val nodes: Set<String> = controller.targetNodes(platform.topology())
        if (!nodes.contains(platform.curNode().name())) {
            out.println(
                "This task is not configured to run on this node. It runs on node(s): " +
                        nodes.joinToString() + ", whereas this node is " +
                        platform.curNode().name()
            )
            return false
        }

        val future = try {
            workerManager.createWorker(EXEC_WORKER_ID, EXEC_TASK_ID, spec)
        } catch (exception: Throwable) {
            out.println("createWorker failed")
            exception.printStackTrace(out)
            return false
        }
        out.println("Waiting for completion of task:" + JsonUtil.toPrettyJsonString(spec))
        val error = future.get()
        return if (error.isEmpty()) {
            val status = JsonUtil.toPrettyJsonString(workerManager.workerStates()[EXEC_WORKER_ID]!!.status())
            out.println("Task succeeded with status $status")
            true
        } else {
            val status = JsonUtil.toPrettyJsonString(workerManager.workerStates()[EXEC_WORKER_ID]!!.status())
            out.println("Task failed with status $status and error $error")
            false
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(Agent::class.java)

        /**
         * The default Agent port.
         */
        val DEFAULT_PORT = 8888

        /**
         * The workerId to use in exec mode.
         */
        private val EXEC_WORKER_ID: Long = 1

        /**
         * The taskId to use in exec mode.
         */
        private val EXEC_TASK_ID = "task0"

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val parser = ArgumentParsers
                .newArgumentParser("trogdor-agent")
                .defaultHelp(true)
                .description("The Trogdor fault injection agent")
            parser.addArgument("--agent.config", "-c")
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
            parser.addArgument("--exec", "-e")
                .action(Arguments.store())
                .type(String::class.java)
                .dest("task_spec")
                .metavar("TASK_SPEC")
                .help(
                    "Execute a single task spec and then exit. The argument is the task spec " +
                            "to load when starting up, or a path to it."
                )
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
            val taskSpec = res.getString("task_spec")
            val platform: Platform = Platform.Config.parse(nodeName, configPath)
            val restServer = JsonRestServer(Node.Util.getTrogdorAgentPort(platform.curNode()))
            val resource = AgentRestResource()
            log.info("Starting agent process.")
            val agent = Agent(platform, Scheduler.SYSTEM, restServer, resource)
            restServer.start(resource)
            addShutdownHook("agent-shutdown-hook") {
                log.warn("Running agent shutdown hook.")
                try {
                    agent.beginShutdown()
                    agent.waitForShutdown()
                } catch (exception: Exception) {
                    log.error("Got exception while running agent shutdown hook.", exception)
                }
            }
            if (taskSpec != null) {
                lateinit var spec: TaskSpec
                try {
                    spec = JsonUtil.objectFromCommandLineArgument(taskSpec, TaskSpec::class.java)
                } catch (exception: Exception) {
                    println("Unable to parse the supplied task spec.")
                    exception.printStackTrace()
                    exit(1)
                }
                val effectiveSpec: TaskSpec = agent.rebaseTaskSpecTime(spec)
                exit(if (agent.exec(effectiveSpec, System.out)) 0 else 1)
            }
            agent.waitForShutdown()
        }
    }
}
