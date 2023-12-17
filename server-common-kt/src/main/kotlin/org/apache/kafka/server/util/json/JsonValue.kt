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
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * A simple wrapper over Jackson's JsonNode that enables type safe parsing via the `DecodeJson` type class.
 *
 * Typical usage would be something like:
 *
 * ```
 * val jsonNode: JsonNode = ???
 * val jsonObject = JsonValue(jsonNode).asJsonObject
 * val intValue = jsonObject("int_field").to[Int]
 * val optionLongValue = jsonObject("option_long_field").to[Option[Long]]
 * val mapStringIntField = jsonObject("map_string_int_field").to[Map[String, Int]]
 * val seqStringField = jsonObject("seq_string_field").to[Seq[String]
 * ```
 *
 * The `to` method throws an exception if the value cannot be converted to the requested type.
 */
interface JsonValue {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("node"),
    )
    fun node(): JsonNode

    val node: JsonNode

    @Throws(JsonMappingException::class)
    fun <T> to(decodeJson: DecodeJson<T>): T = decodeJson.decode(node)

    /**
     * If this is a JSON object, return an instance of JsonObject. Otherwise, throw a JsonMappingException.
     */
    @Throws(JsonMappingException::class)
    fun asJsonObject(): JsonObject? {
        return asJsonObjectOptional() ?: throw JsonMappingException(null, "Expected JSON object, received $node")
    }

    /**
     * If this is a JSON object, return a JsonObject wrapped by a `Some`. Otherwise, return None.
     */
    fun asJsonObjectOptional(): JsonObject? {
        return if (this is JsonObject) this
        else if (node is ObjectNode) JsonObject(node as ObjectNode)
        else null
    }

    /**
     * If this is a JSON array, return an instance of JsonArray. Otherwise, throw a JsonMappingException.
     */
    @Throws(JsonMappingException::class)
    fun asJsonArray(): JsonArray? {
        return asJsonArrayOptional() ?: throw JsonMappingException(null, "Expected JSON array, received $node")
    }

    /**
     * If this is a JSON array, return a JsonArray wrapped by a `Some`. Otherwise, return None.
     */
    fun asJsonArrayOptional(): JsonArray? {
        return if (this is JsonArray) this
        else if (node is ArrayNode) JsonArray(node as ArrayNode)
        else null
    }

    data class BasicJsonValue internal constructor(override var node: JsonNode) : JsonValue {

        @Deprecated("Use property instead", replaceWith = ReplaceWith("node"))
        override fun node(): JsonNode = node

        override fun toString(): String = node.toString()
    }

    companion object {

        fun apply(node: JsonNode): JsonValue = when (node) {
            is ObjectNode -> JsonObject(node)
            is ArrayNode -> JsonArray(node)
            else -> BasicJsonValue(node)
        }
    }
}
