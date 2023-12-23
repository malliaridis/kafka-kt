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

import java.util.TreeMap
import java.util.TreeSet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.trogdor.agent.Agent
import org.apache.kafka.trogdor.agent.AgentClient
import org.apache.kafka.trogdor.agent.AgentRestResource
import org.apache.kafka.trogdor.basic.BasicNode
import org.apache.kafka.trogdor.basic.BasicPlatform
import org.apache.kafka.trogdor.basic.BasicPlatform.CommandRunner
import org.apache.kafka.trogdor.basic.BasicPlatform.ShellCommandRunner
import org.apache.kafka.trogdor.basic.BasicTopology
import org.apache.kafka.trogdor.coordinator.Coordinator
import org.apache.kafka.trogdor.coordinator.CoordinatorClient
import org.apache.kafka.trogdor.coordinator.CoordinatorRestResource
import org.apache.kafka.trogdor.rest.JsonRestServer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.concurrent.Volatile

/**
 * MiniTrogdorCluster sets up a local cluster of Trogdor Agents and Coordinators.
 */
class MiniTrogdorCluster private constructor(
    private val scheduler: Scheduler,
    val agents: TreeMap<String, Agent>,
    private val nodesByAgent: TreeMap<String, Builder.NodeData>,
    val coordinator: Coordinator?,
) : AutoCloseable {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("agents"),
    )
    fun agents(): TreeMap<String, Agent> = agents

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("coordinator"),
    )
    fun coordinator(): Coordinator? = coordinator

    fun coordinatorClient(): CoordinatorClient {
        if (coordinator == null) throw RuntimeException("No coordinator configured.")
        return CoordinatorClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = coordinator.port())
            .build()
    }

    /**
     * Mimic a restart of a Trogdor agent, essentially cleaning out all of its active workers
     */
    fun restartAgent(nodeName: String) {
        if (!agents.containsKey(nodeName)) throw RuntimeException("There is no agent on node $nodeName")

        val node = nodesByAgent[nodeName]
        agents[nodeName] = Agent(
            platform = (node!!.platform)!!,
            scheduler = scheduler,
            restServer = node.agentRestServer!!,
            resource = node.agentRestResource!!,
        )
    }

    fun agentClient(nodeName: String): AgentClient {
        val agent = agents[nodeName] ?: throw RuntimeException("No agent configured on node $nodeName")
        return AgentClient.Builder()
            .maxTries(10)
            .target(host = "localhost", port = agent.port())
            .build()
    }

    @Throws(Exception::class)
    override fun close() {
        log.info("Closing MiniTrogdorCluster.")
        coordinator?.beginShutdown(false)

        for (agent in agents.values) agent.beginShutdown()
        for (agent in agents.values) agent.waitForShutdown()

        coordinator?.waitForShutdown()
    }

    /**
     * The MiniTrogdorCluster#Builder is used to set up a new MiniTrogdorCluster.
     */
    class Builder {

        private val agentNames: TreeSet<String> = TreeSet()

        private var coordinatorName: String? = null

        private var scheduler: Scheduler = Scheduler.SYSTEM

        private var commandRunner: CommandRunner = ShellCommandRunner()

        /**
         * Set the timekeeper used by this MiniTrogdorCluster.
         */
        fun scheduler(scheduler: Scheduler): Builder {
            this.scheduler = scheduler
            return this
        }

        fun commandRunner(commandRunner: CommandRunner): Builder {
            this.commandRunner = commandRunner
            return this
        }

        /**
         * Add a new trogdor coordinator node to the cluster.
         */
        fun addCoordinator(nodeName: String?): Builder {
            if (coordinatorName != null) throw RuntimeException("At most one coordinator is allowed.")
            coordinatorName = nodeName
            return this
        }

        /**
         * Add a new trogdor agent node to the cluster.
         */
        fun addAgent(nodeName: String): Builder {
            if (agentNames.contains(nodeName)) throw RuntimeException("There is already an agent on node $nodeName")
            agentNames.add(nodeName)
            return this
        }

        private fun getOrCreate(nodeName: String, nodes: TreeMap<String, NodeData>): NodeData {
            var data: NodeData? = nodes[nodeName]
            if (data != null) return data
            data = NodeData()
            data.hostname = "127.0.0.1"
            nodes[nodeName] = data
            return data
        }

        /**
         * Create the MiniTrogdorCluster.
         */
        @Throws(Exception::class)
        fun build(): MiniTrogdorCluster {
            log.info(
                "Creating MiniTrogdorCluster with agents: {} and coordinator: {}",
                agentNames.joinToString(), coordinatorName
            )
            val nodes = TreeMap<String, NodeData>()
            for (agentName in agentNames) {
                val node: NodeData = getOrCreate(agentName, nodes)
                node.agentRestResource = AgentRestResource()
                node.agentRestServer = JsonRestServer(0)
                node.agentRestServer!!.start(node.agentRestResource)
                node.agentPort = node.agentRestServer!!.port()
            }
            coordinatorName?.let { coordinatorName ->
                val node: NodeData = getOrCreate(coordinatorName, nodes)
                node.coordinatorRestResource = CoordinatorRestResource()
                node.coordinatorRestServer = JsonRestServer(0)
                node.coordinatorRestServer!!.start(node.coordinatorRestResource)
                node.coordinatorPort = node.coordinatorRestServer!!.port()
            }
            for ((nodeName, node) in nodes) {
                val config = mutableMapOf<String, String>()
                if (node.agentPort != 0)
                    config[Platform.Config.TROGDOR_AGENT_PORT] = node.agentPort.toString()
                if (node.coordinatorPort != 0)
                    config[Platform.Config.TROGDOR_COORDINATOR_PORT] = node.coordinatorPort.toString()

                node.node = BasicNode(
                    name = nodeName,
                    hostname = node.hostname!!,
                    config = config,
                    tags = emptySet(),
                )
            }
            val topologyNodes = TreeMap<String, Node>()
            for ((nodeName, nodeData) in nodes) {
                topologyNodes[nodeName] = nodeData.node!!
            }
            val topology = BasicTopology(topologyNodes)
            val executor = Executors.newScheduledThreadPool(
                1,
                createThreadFactory("MiniTrogdorClusterStartupThread%d", false)
            )
            val failure: AtomicReference<Exception?> = AtomicReference(null)
            for ((nodeName, node) in nodes.entries) {
                executor.submit {
                    try {
                        node.platform = BasicPlatform(nodeName, topology, scheduler, commandRunner)
                        if (node.agentRestResource != null) {
                            node.agent = Agent(
                                platform = node.platform!!,
                                scheduler = scheduler,
                                restServer = node.agentRestServer!!,
                                resource = node.agentRestResource!!,
                            )
                        }
                        if (node.coordinatorRestResource != null) {
                            node.coordinator = Coordinator(
                                platform = node.platform!!,
                                scheduler = scheduler,
                                restServer = (node.coordinatorRestServer)!!,
                                resource = node.coordinatorRestResource!!,
                                firstWorkerId = 0,
                            )
                        }
                    } catch (exception: Exception) {
                        log.error("Unable to initialize {}", nodeName, exception)
                        failure.compareAndSet(null, exception)
                    }
                }
            }
            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.DAYS)
            val failureException = failure.get()
            if (failureException != null) throw failureException

            val agents = TreeMap<String, Agent>()
            var coordinator: Coordinator? = null
            for ((nodeName, node) in nodes.entries) {
                if (node.agent != null)
                    agents[nodeName] = node.agent!!
                if (node.coordinator != null)
                    coordinator = node.coordinator
            }
            return MiniTrogdorCluster(
                scheduler = scheduler,
                agents = agents,
                nodesByAgent = nodes,
                coordinator = coordinator,
            )
        }

        class NodeData {

            var hostname: String? = null

            @Volatile
            var agentRestResource: AgentRestResource? = null

            @Volatile
            var agentRestServer: JsonRestServer? = null

            var agentPort: Int = 0

            @Volatile
            var coordinatorRestServer: JsonRestServer? = null

            var coordinatorPort: Int = 0

            @Volatile
            var coordinatorRestResource: CoordinatorRestResource? = null

            @Volatile
            var platform: Platform? = null

            @Volatile
            var agent: Agent? = null

            @Volatile
            var coordinator: Coordinator? = null

            @Volatile
            var node: BasicNode? = null
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(MiniTrogdorCluster::class.java)
    }
}
