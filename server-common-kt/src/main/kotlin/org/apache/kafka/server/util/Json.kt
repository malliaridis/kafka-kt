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

package org.apache.kafka.server.util

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.MissingNode
import java.io.IOException
import org.apache.kafka.server.util.json.JsonValue

/**
 * Provides methods for parsing JSON with Jackson and encoding to JSON with a simple and naive custom implementation.
 */
object Json {
    private val mapper = ObjectMapper()

    /**
     * Parse a JSON string into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
     */
    fun parseFull(input: String?): JsonValue? {
        return try {
            tryParseFull(input)
        } catch (e: JsonProcessingException) {
            null
        }
    }

    /**
     * Parse a JSON string into a generic type T, or throw JsonProcessingException in the case of
     * exception.
     */
    @Throws(JsonProcessingException::class)
    fun <T> parseStringAs(input: String?, clazz: Class<T>?): T {
        return mapper.readValue(input, clazz)
    }

    /**
     * Parse a JSON byte array into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
     */
    @Throws(IOException::class)
    fun parseBytes(input: ByteArray): JsonValue? {
        return try {
            JsonValue.apply(mapper.readTree(input))
        } catch (e: JsonProcessingException) {
            null
        }
    }

    @Throws(IOException::class)
    fun tryParseBytes(input: ByteArray): JsonValue {
        return JsonValue.apply(mapper.readTree(input))
    }

    /**
     * Parse a JSON byte array into a generic type T, or throws a JsonProcessingException in the case of exception.
     */
    @Throws(IOException::class)
    fun <T> parseBytesAs(input: ByteArray?, clazz: Class<T>?): T {
        return mapper.readValue(input, clazz)
    }

    /**
     * Parse a JSON string into a JsonValue if possible.
     * @param input a JSON string to parse
     * @return the actual json value.
     * @throws JsonProcessingException if failed to parse
     */
    @Throws(JsonProcessingException::class)
    fun tryParseFull(input: String?): JsonValue {
        return if (input.isNullOrEmpty()) {
            throw JsonParseException(MissingNode.getInstance().traverse(), "The input string shouldn't be empty")
        } else {
            JsonValue.apply(mapper.readTree(input))
        }
    }

    /**
     * Encode an object into a JSON string. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    @Throws(JsonProcessingException::class)
    fun encodeAsString(obj: Any?): String = mapper.writeValueAsString(obj)

    /**
     * Encode an object into a JSON value in bytes. This method accepts any type supported by Jackson's ObjectMapper in
     * the default configuration. That is, Java collections are supported, but Scala collections are not (to avoid
     * a jackson-scala dependency).
     */
    @Throws(JsonProcessingException::class)
    fun encodeAsBytes(obj: Any?): ByteArray = mapper.writeValueAsBytes(obj)
}
