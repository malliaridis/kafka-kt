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

import com.fasterxml.jackson.databind.JsonNode
import java.io.File
import java.io.IOException
import org.apache.kafka.common.utils.Scheduler
import org.apache.kafka.common.utils.Utils.newParameterizedInstance

/**
 * Defines a cluster topology
 */
interface Platform {

    /**
     * Get name for this platform.
     */
    fun name(): String

    /**
     * Get the current node.
     */
    fun curNode(): Node

    /**
     * Get the cluster topology.
     */
    fun topology(): Topology

    /**
     * Get the scheduler to use.
     */
    fun scheduler(): Scheduler

    /**
     * Run a command on this local node.
     *
     * Throws an exception if the command could not be run, or if the command returned a non-zero error status.
     *
     * @param command The command
     * @return The command output.
     */
    @Throws(IOException::class)
    fun runCommand(command: Array<String>): String

    object Config {

        val TROGDOR_AGENT_PORT = "trogdor.agent.port"

        val TROGDOR_COORDINATOR_PORT = "trogdor.coordinator.port"

        val TROGDOR_COORDINATOR_HEARTBEAT_MS = "trogdor.coordinator.heartbeat.ms"

        val TROGDOR_COORDINATOR_HEARTBEAT_MS_DEFAULT = 60000

        @Throws(Exception::class)
        fun parse(curNodeName: String?, path: String?): Platform {
            val root: JsonNode = JsonUtil.JSON_SERDE.readTree(File(path))
            val platformNode = root.get("platform")
                ?: throw RuntimeException("Expected to find a 'platform' field in the root JSON configuration object")
            val platformName = platformNode.textValue()

            return newParameterizedInstance(
                platformName,
                String::class.java, (curNodeName)!!,
                Scheduler::class.java, Scheduler.SYSTEM,
                JsonNode::class.java, root,
            )
        }
    }
}
