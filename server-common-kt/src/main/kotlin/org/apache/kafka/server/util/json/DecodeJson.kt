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

fun interface DecodeJson<T> {

    /**
     * Decode the JSON node provided into an instance of `T`.
     *
     * @throws JsonMappingException if `node` cannot be decoded into `T`.
     */
    @Throws(JsonMappingException::class)
    fun decode(node: JsonNode): T

    class DecodeBoolean : DecodeJson<Boolean> {

        @Throws(JsonMappingException::class)
        override fun decode(node: JsonNode): Boolean {
            if (node.isBoolean) return node.booleanValue()
            throw throwJsonMappingException(Boolean::class.java.name, node)
        }
    }

    class DecodeDouble : DecodeJson<Double> {
        @Throws(JsonMappingException::class)
        override fun decode(node: JsonNode): Double {
            if (node.isDouble || node.isLong || node.isInt) return node.doubleValue()
            throw throwJsonMappingException(Double::class.java.name, node)
        }
    }

    class DecodeInteger : DecodeJson<Int> {
        @Throws(JsonMappingException::class)
        override fun decode(node: JsonNode): Int {
            if (node.isInt) return node.intValue()
            throw throwJsonMappingException(Int::class.java.name, node)
        }
    }

    class DecodeLong : DecodeJson<Long> {
        @Throws(JsonMappingException::class)
        override fun decode(node: JsonNode): Long {
            if (node.isLong || node.isInt) return node.longValue()
            throw throwJsonMappingException(Long::class.java.name, node)
        }
    }

    class DecodeString : DecodeJson<String> {
        @Throws(JsonMappingException::class)
        override fun decode(node: JsonNode): String {
            if (node.isTextual) return node.textValue()
            throw throwJsonMappingException(String::class.java.name, node)
        }
    }

    companion object {

        fun throwJsonMappingException(expectedType: String, node: JsonNode): JsonMappingException {
            return JsonMappingException(null, "Expected `$expectedType` value, received $node")
        }

        fun <E> decodeOptional(decodeJson: DecodeJson<E>): DecodeJson<E?> {
            return DecodeJson { node ->
                if (node.isNull) return@DecodeJson null
                decodeJson.decode(node)
            }
        }

        fun <E> decodeList(decodeJson: DecodeJson<E>): DecodeJson<List<E>> {
            return DecodeJson { node ->
                if (node.isArray) {
                    val result: MutableList<E> = ArrayList()
                    val elements = node.elements()
                    while (elements.hasNext()) {
                        result.add(decodeJson.decode(elements.next()))
                    }
                    return@DecodeJson result
                }
                throw throwJsonMappingException("JSON array", node)
            }
        }

        fun <V> decodeMap(decodeJson: DecodeJson<V>): DecodeJson<Map<String, V>> {
            return DecodeJson { node ->
                if (node.isObject) {
                    val result = mutableMapOf<String, V>()
                    val elements = node.fields()
                    while (elements.hasNext()) {
                        val (key, value) = elements.next()
                        result[key] = decodeJson.decode(value)
                    }
                    return@DecodeJson result
                }
                throw throwJsonMappingException("JSON object", node)
            }
        }
    }
}
