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

package org.apache.kafka.server.utils

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.AbstractMap
import java.util.Arrays
import java.util.Optional
import java.util.function.Function
import java.util.stream.Collectors
import org.apache.kafka.server.util.Json.parseFull
import org.apache.kafka.server.util.json.DecodeJson
import org.apache.kafka.server.util.json.DecodeJson.DecodeBoolean
import org.apache.kafka.server.util.json.DecodeJson.DecodeDouble
import org.apache.kafka.server.util.json.DecodeJson.DecodeInteger
import org.apache.kafka.server.util.json.DecodeJson.DecodeLong
import org.apache.kafka.server.util.json.DecodeJson.DecodeString
import org.apache.kafka.server.util.json.JsonObject
import org.apache.kafka.server.util.json.JsonValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

class JsonTest {

    private fun parse(s: String): JsonValue {
        return parseFull(s) ?: throw RuntimeException("Failed to parse json: $s")
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testAsJsonObject() {
        val parsed = parse(JSON).asJsonObject()
        val obj = parsed!!.apply("object")
        assertEquals(obj, obj.asJsonObject()!!)
        assertFailsWith<JsonMappingException> { parsed.apply("array").asJsonObject() }
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testAsJsonObjectOption() {
        val parsed = parse(JSON).asJsonObject()
        assertNotNull(parsed!!.apply("object").asJsonObjectOptional())
        assertEquals(null, parsed.apply("array").asJsonObjectOptional())
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testAsJsonArray() {
        val parsed = parse(JSON).asJsonObject()
        val array = parsed!!.apply("array")
        assertEquals(array, array.asJsonArray()!!)
        assertFailsWith<JsonMappingException> { parsed.apply("object").asJsonArray() }
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testAsJsonArrayOption() {
        val parsed = parse(JSON).asJsonObject()
        assertNotNull(parsed!!.apply("array").asJsonArrayOptional())
        assertEquals(null, parsed.apply("object").asJsonArrayOptional())
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testJsonObjectGet() {
        val parsed = parse(JSON).asJsonObject()
        assertEquals(parse("{\"a\":true,\"b\":false}"), parsed!!["object"])
        assertEquals(null, parsed["aaaaa"])
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testJsonObjectApply() {
        val parsed = parse(JSON).asJsonObject()
        assertEquals(parse("{\"a\":true,\"b\":false}"), parsed!!.apply("object"))
        assertFailsWith<JsonMappingException> { parsed.apply("aaaaaaaa") }
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testJsonObjectIterator() {
        val results = mutableListOf<Map.Entry<String, JsonValue?>>()
        parse(JSON).asJsonObject()!!
            .apply("object")
            .asJsonObject()!!
            .iterator()
            .forEachRemaining { entry -> results.add(entry) }
        val entryA = AbstractMap.SimpleEntry("a", parse("true"))
        val entryB = AbstractMap.SimpleEntry("b", parse("false"))
        val expectedResult = listOf(entryA, entryB)
        assertContentEquals(expectedResult, results)
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testJsonArrayIterator() {
        val results = mutableListOf<JsonValue>()
        parse(JSON).asJsonObject()!!
            .apply("array")
            .asJsonArray()!!
            .iterator()
            .forEachRemaining { entry -> results.add(entry) }
        val expected = listOf("4.0", "11.1", "44.5").map { parse(it) }
        
        assertEquals(expected, results)
    }

    @Test
    fun testJsonValueEquals() {
        assertEquals(parse(JSON), parse(JSON))
        assertEquals(parse("{\"blue\": true, \"red\": false}"), parse("{\"red\": false, \"blue\": true}"))
        assertNotEquals(parse("{\"blue\": true, \"red\": true}"), parse("{\"red\": false, \"blue\": true}"))
        assertEquals(parse("[1, 2, 3]"), parse("[1, 2, 3]"))
        assertNotEquals(parse("[1, 2, 3]"), parse("[2, 1, 3]"))
        assertEquals(parse("1344"), parse("1344"))
        assertNotEquals(parse("1344"), parse("144"))
    }

    @Test
    @Throws(JsonProcessingException::class)
    fun testJsonValueHashCode() {
        assertEquals(ObjectMapper().readTree(JSON).hashCode(), parse(JSON).hashCode())
    }

    @Test
    fun testJsonValueToString() {
        val js = "{\"boolean\":false,\"int\":1234,\"array\":[4.0,11.1,44.5],\"object\":{\"a\":true,\"b\":false}}"
        assertEquals(js, parse(js).toString())
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeBoolean() {
        val decodeJson = DecodeBoolean()
        assertTo(
            expected = false,
            decodeJson = decodeJson,
            jsonValue = { jsonObject -> jsonObject["boolean"]!! },
        )
        assertToFails(decodeJson) { jsonObject -> jsonObject["int"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeString() {
        val decodeJson = DecodeString()
        assertTo<String>("string", decodeJson) { jsonObject -> jsonObject["string"]!! }
        assertTo<String>("123", decodeJson) { jsonObject -> jsonObject["number_as_string"]!! }
        assertToFails(decodeJson) { jsonObject -> jsonObject["int"]!! }
        assertToFails(decodeJson) { jsonObject -> jsonObject["array"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeInt() {
        val decodeJson = DecodeInteger()
        assertTo(1234, decodeJson) { jsonObject -> jsonObject["int"]!! }
        assertToFails(decodeJson) { jsonObject -> jsonObject["long"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeLong() {
        val decodeJson = DecodeLong()
        assertTo(3000000000L, decodeJson) { jsonObject -> jsonObject["long"]!! }
        assertTo(1234L, decodeJson) { jsonObject -> jsonObject["int"]!! }
        assertToFails(decodeJson) { jsonObject -> jsonObject["string"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeDouble() {
        val decodeJson = DecodeDouble()
        assertTo(16.244355, decodeJson) { jsonObject -> jsonObject["double"]!! }
        assertTo(1234.0, decodeJson) { jsonObject -> jsonObject["int"]!! }
        assertTo(3000000000.0, decodeJson) { jsonObject -> jsonObject["long"]!! }
        assertToFails(decodeJson) { jsonObject -> jsonObject["string"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeSeq() {
        (DecodeJson.decodeList(DecodeDouble())).also {
            assertTo(listOf(4.0, 11.1, 44.5), it) { jsonObject -> jsonObject["array"]!! }
            assertToFails(it) { jsonObject -> jsonObject["string"]!! }
            assertToFails(it) { jsonObject -> jsonObject["object"]!! }
        }
        assertToFails(DecodeJson.decodeList(DecodeString())) { jsonObject -> jsonObject["array"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeMap() {
        val decodeJson = DecodeJson.decodeMap(DecodeBoolean())
        val stringBooleanMap = HashMap<String, Boolean>()
        stringBooleanMap["a"] = true
        stringBooleanMap["b"] = false
        assertTo(stringBooleanMap, decodeJson) { jsonObject -> jsonObject["object"]!! }
        assertToFails(DecodeJson.decodeMap(DecodeInteger())) { jsonObject -> jsonObject["object"]!! }
        assertToFails(DecodeJson.decodeMap(DecodeString())) { jsonObject -> jsonObject["object"]!! }
        assertToFails(DecodeJson.decodeMap(DecodeDouble())) { jsonObject -> jsonObject["array"]!! }
    }

    @Test
    @Throws(JsonMappingException::class)
    fun testDecodeOptional() {
        val decodeJson = DecodeJson.decodeOptional(DecodeInteger())
        assertTo(null, decodeJson) { jsonObject -> jsonObject["null"]!! }
        assertTo(1234, decodeJson) { jsonObject -> jsonObject["int"]!! }
        assertToFails(DecodeJson.decodeOptional(DecodeString())) { jsonObject -> jsonObject["int"]!! }
    }

    @Throws(JsonMappingException::class)
    private fun <T> assertTo(
        expected: T,
        decodeJson: DecodeJson<T>,
        jsonValue: (JsonObject) -> JsonValue,
    ) {
        val parsed = jsonValue(parse(JSON).asJsonObject()!!)
        assertEquals(expected, parsed.to(decodeJson))
    }

    @Throws(JsonMappingException::class)
    private fun <T> assertToFails(
        decoder: DecodeJson<T>,
        jsonValue: (JsonObject) -> JsonValue,
    ) {
        val obj = parse(JSON).asJsonObject()
        val parsed = jsonValue(obj!!)
        assertFailsWith<JsonMappingException> { parsed.to(decoder) }
    }

    companion object {
        private const val JSON = "{\n" +
                "  \"boolean\": false,\n" +
                "  \"int\": 1234,\n" +
                "  \"long\": 3000000000,\n" +
                "  \"double\": 16.244355,\n" +
                "  \"string\": \"string\",\n" +
                "  \"number_as_string\": \"123\",\n" +
                "  \"array\": [4.0, 11.1, 44.5],\n" +
                "  \"object\": {\n" +
                "    \"a\": true,\n" +
                "    \"b\": false\n" +
                "  },\n" +
                "  \"null\": null\n" +
                "}"
    }
}
