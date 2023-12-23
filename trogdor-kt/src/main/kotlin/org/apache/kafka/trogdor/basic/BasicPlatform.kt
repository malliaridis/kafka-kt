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

package org.apache.kafka.trogdor.basic

import com.fasterxml.jackson.databind.JsonNode
import java.io.IOException
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.Shell
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.Topology
import org.slf4j.LoggerFactory

/**
 * Defines a cluster topology
 */
class BasicPlatform : Platform {

    private val curNode: Node

    private val topology: BasicTopology

    private val scheduler: Scheduler

    private val commandRunner: CommandRunner

    constructor(
        curNodeName: String,
        topology: BasicTopology,
        scheduler: Scheduler,
        commandRunner: CommandRunner,
    ) {
        curNode = topology.node(curNodeName) ?: throw RuntimeException(
            "No node named $curNodeName found in the cluster!  Cluster nodes are: " +
                    topology.nodes().keys.joinToString(",")
        )

        this.topology = topology
        this.scheduler = scheduler
        this.commandRunner = commandRunner
    }

    constructor(
        curNodeName: String,
        scheduler: Scheduler,
        configRoot: JsonNode,
    ) {
        val nodes = configRoot.get("nodes") ?: throw RuntimeException(
            "Expected to find a 'nodes' field in the root JSON configuration object"
        )
        topology = BasicTopology(nodes)
        this.scheduler = scheduler
        curNode = topology.node(curNodeName) ?: throw RuntimeException(
            "No node named $curNodeName found in the cluster!  Cluster nodes are: " +
                    topology.nodes().keys.joinToString(",")
        )
        commandRunner = ShellCommandRunner()
    }

    override fun name(): String = "BasicPlatform"

    override fun curNode(): Node = curNode

    override fun topology(): Topology = topology

    override fun scheduler(): Scheduler = scheduler

    @Throws(IOException::class)
    override fun runCommand(command: Array<String>): String = commandRunner.run(curNode, command)

    fun interface CommandRunner {

        @Throws(IOException::class)
        fun run(curNode: Node, command: Array<String>): String
    }

    class ShellCommandRunner : CommandRunner {

        @Throws(IOException::class)
        override fun run(curNode: Node, command: Array<String>): String {
            try {
                val result = Shell.execCommand(*command)
                log.info("RUN: {}. RESULT: [{}]", command.joinToString(" "), result)
                return result
            } catch (exception: RuntimeException) {
                log.info("RUN: {}. ERROR: [{}]", command.joinToString(" "), exception.message)
                throw exception
            } catch (exception: IOException) {
                log.info("RUN: {}. ERROR: [{}]", command.joinToString(" "), exception.message)
                throw exception
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(BasicPlatform::class.java)
    }
}
