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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import java.io.File

/**
 * Utilities for working with JSON.
 */
object JsonUtil {

    val JSON_SERDE: ObjectMapper = ObjectMapper()

    init {
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        JSON_SERDE.registerModule(Jdk8Module())
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    }

    fun toJsonString(element: Any?): String {
        return try {
            JSON_SERDE.writeValueAsString(element)
        } catch (exception: JsonProcessingException) {
            throw RuntimeException(exception)
        }
    }

    fun toPrettyJsonString(element: Any?): String {
        return try {
            JSON_SERDE.writerWithDefaultPrettyPrinter().writeValueAsString(element)
        } catch (exception: JsonProcessingException) {
            throw RuntimeException(exception)
        }
    }

    /**
     * Determine if a string is a JSON object literal. Object literals must begin with an open brace.
     *
     * @param input The input string.
     * @return `true` if the string is a JSON literal.
     */
    fun openBraceComesFirst(input: String): Boolean {
        for (element in input) {
            if (!Character.isWhitespace(element))
                return element == '{'
        }
        return false
    }

    /**
     * Read a JSON object from a command-line argument. This can take the form of a path to a file containing
     * the JSON object, or simply the raw JSON object itself. We will assume that if the string is a valid JSON object,
     * the latter is true. If you want to specify a file name containing an open brace, you can force it to be
     * interpreted as a file name be prefixing a ./ or full path.
     *
     * @param argument The command-line argument.
     * @param clazz The class of the object to be read.
     * @param T The object type.
     * @return The object which we read.
     */
    @Throws(Exception::class)
    fun <T> objectFromCommandLineArgument(argument: String, clazz: Class<T>): T {
        return if (openBraceComesFirst(argument)) JSON_SERDE.readValue(argument, clazz)
        else JSON_SERDE.readValue(File(argument), clazz)
    }
}
