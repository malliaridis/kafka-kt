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
import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.agent.Agent
import org.apache.kafka.trogdor.basic.BasicNode
import org.apache.kafka.trogdor.basic.BasicTopology
import org.apache.kafka.trogdor.common.Topology.Util.agentNodeNames
import org.apache.kafka.trogdor.coordinator.Coordinator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class TopologyTest {
    
    @Test
    fun testAgentNodeNames() {
        val nodes = TreeMap<String, Node>()
        val numNodes = 5
        for (i in 0..<numNodes) {
            val conf = mutableMapOf<String, String>()
            if (i == 0) conf[Platform.Config.TROGDOR_COORDINATOR_PORT] = Coordinator.DEFAULT_PORT.toString()
            else conf[Platform.Config.TROGDOR_AGENT_PORT] = Agent.DEFAULT_PORT.toString()
            
            val node = BasicNode(
                name = String.format("node%02d", i),
                hostname = String.format("node%d.example.com", i),
                config = conf,
                tags = HashSet()
            )
            nodes[node.name()] = node
        }
        val topology: Topology = BasicTopology(nodes)
        val names = agentNodeNames(topology)
        assertEquals(4, names.size)
        for (i in 1..<numNodes - 1) {
            assertTrue(names.contains(String.format("node%02d", i)))
        }
    }
}
