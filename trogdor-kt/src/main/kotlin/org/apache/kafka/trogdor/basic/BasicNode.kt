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
import java.util.Objects
import org.apache.kafka.trogdor.common.Node


class BasicNode : Node {

    private val name: String

    private val hostname: String

    private val config: Map<String, String>

    private val tags: Set<String>

    constructor(
        name: String,
        hostname: String,
        config: Map<String, String>,
        tags: Set<String>,
    ) {
        this.name = name
        this.hostname = hostname
        this.config = config
        this.tags = tags
    }

    constructor(name: String, root: JsonNode) {
        this.name = name
        var hostname = "localhost"
        var tags = emptySet<String>()
        val config = mutableMapOf<String, String>()
        val iter = root.fields()
        while (iter.hasNext()) {
            val (key, node) = iter.next()
            when (key) {
                "hostname" -> hostname = node.asText()
                "tags" -> {
                    if (!node.isArray) throw RuntimeException("Expected the 'tags' field to be an array of strings.")
                    tags = mutableSetOf()
                    val tagIter = node.elements()
                    while (tagIter.hasNext()) {
                        val tag = tagIter.next()
                        tags.add(tag.asText())
                    }
                }
                else -> config[key] = node.asText()
            }
        }
        this.hostname = hostname
        this.tags = tags
        this.config = config
    }

    override fun name(): String = name

    override fun hostname(): String = hostname

    override fun getConfig(key: String): String? = config[key]

    override fun tags(): Set<String> = tags

    override fun hashCode(): Int = Objects.hash(name, hostname, config, tags)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as BasicNode
        return name == that.name && hostname == that.hostname && config == that.config && tags == that.tags
    }
}
