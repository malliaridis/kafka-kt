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

import org.apache.kafka.trogdor.agent.Agent
import org.apache.kafka.trogdor.coordinator.Coordinator

/**
 * Defines a node in a cluster topology
 */
interface Node {

    /**
     * Get name for this node.
     */
    fun name(): String

    /**
     * Get hostname for this node.
     */
    fun hostname(): String

    /**
     * Get the configuration value associated with the key, or `null` if there is none.
     */
    fun getConfig(key: String): String?

    /**
     * Get the tags for this node.
     */
    fun tags(): Set<String>

    object Util {
        fun getIntConfig(node: Node, key: String, defaultVal: Int): Int {
            val value = node.getConfig(key)
            return value?.toInt() ?: defaultVal
        }

        fun getTrogdorAgentPort(node: Node): Int {
            return getIntConfig(node, Platform.Config.TROGDOR_AGENT_PORT, Agent.DEFAULT_PORT)
        }

        fun getTrogdorCoordinatorPort(node: Node): Int {
            return getIntConfig(node, Platform.Config.TROGDOR_COORDINATOR_PORT, Coordinator.DEFAULT_PORT)
        }
    }
}
