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
import java.util.NavigableMap
import java.util.TreeMap
import org.apache.kafka.trogdor.common.Node
import org.apache.kafka.trogdor.common.Topology

class BasicTopology : Topology {

    private val nodes: NavigableMap<String, Node>

    constructor(nodes: NavigableMap<String, Node>) {
        this.nodes = nodes
    }

    constructor(configRoot: JsonNode) {
        if (!configRoot.isObject) throw RuntimeException("Expected the 'nodes' element to be a JSON object.")

        nodes = TreeMap()
        val iter = configRoot.fieldNames()
        while (iter.hasNext()) {
            val nodeName = iter.next()
            val nodeConfig = configRoot[nodeName]
            val node = BasicNode(nodeName, nodeConfig)
            nodes[nodeName] = node
        }
    }

    override fun node(id: String): Node? = nodes[id]

    override fun nodes(): NavigableMap<String, Node> = nodes
}
