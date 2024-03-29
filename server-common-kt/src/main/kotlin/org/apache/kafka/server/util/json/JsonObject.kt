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

package org.apache.kafka.server.util.json

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.AbstractMap
import java.util.Spliterator
import java.util.Spliterators
import java.util.stream.StreamSupport

data class JsonObject internal constructor(override val node: ObjectNode) : JsonValue {

    @Deprecated("Use property instead", replaceWith = ReplaceWith("node"))
    override fun node(): JsonNode = node

    @Throws(JsonMappingException::class)
    fun apply(name: String): JsonValue {
        return get(name) ?: throw JsonMappingException(null, "No such field exists: `$name`")
    }

    operator fun get(name: String?): JsonValue? = node[name]?.let { JsonValue.apply(it) }

    operator fun iterator(): Iterator<Map.Entry<String, JsonValue?>> {
        return node.fields().asSequence()
            .map { (key, value) -> AbstractMap.SimpleEntry(key, JsonValue.apply(value)) }
            .iterator()
    }

    override fun toString(): String = node.toString()
}
