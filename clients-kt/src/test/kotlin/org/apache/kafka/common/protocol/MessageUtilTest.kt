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

package org.apache.kafka.common.protocol

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.TextNode
import java.io.IOException
import java.io.StringWriter
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import org.apache.kafka.common.protocol.MessageUtil.byteBufferToArray
import org.apache.kafka.common.protocol.MessageUtil.compareRawTaggedFields
import org.apache.kafka.common.protocol.MessageUtil.deepToString
import org.apache.kafka.common.protocol.MessageUtil.duplicate
import org.apache.kafka.common.protocol.MessageUtil.jsonNodeToBinary
import org.apache.kafka.common.protocol.types.RawTaggedField
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Timeout(120)
class MessageUtilTest {
    
    @Test
    fun testDeepToString() {
        assertEquals("[1, 2, 3]", deepToString(mutableListOf(1, 2, 3).iterator()))
        assertEquals("[foo]", deepToString(mutableListOf("foo").iterator()))
    }

    @Test
    fun testByteBufferToArray() {
        assertContentEquals(
            byteArrayOf(1, 2, 3),
            byteBufferToArray(ByteBuffer.wrap(byteArrayOf(1, 2, 3)))
        )
        assertContentEquals(
            byteArrayOf(),
            byteBufferToArray(ByteBuffer.wrap(byteArrayOf()))
        )
    }

    @Test
    fun testDuplicate() {
        // Kotlin Migration: duplicate function does not allow nullable parameters anymore
        // assertNull(duplicate(null))
        assertContentEquals(
            byteArrayOf(),
            duplicate(byteArrayOf())
        )
        assertContentEquals(
            byteArrayOf(1, 2, 3),
            duplicate(byteArrayOf(1, 2, 3))
        )
    }

    @Test
    fun testCompareRawTaggedFields() {
        assertTrue(compareRawTaggedFields(null, null))
        assertTrue(compareRawTaggedFields(null, emptyList<RawTaggedField>()))
        assertTrue(compareRawTaggedFields(emptyList<RawTaggedField>(), null))
        assertFalse(
            compareRawTaggedFields(
                first = emptyList<RawTaggedField>(),
                second = listOf(RawTaggedField(1, byteArrayOf(1))),
            )
        )
        assertFalse(compareRawTaggedFields(null, listOf(RawTaggedField(1, byteArrayOf(1)))))
        assertFalse(
            compareRawTaggedFields(
                first = listOf(RawTaggedField(1, byteArrayOf(1))),
                second = emptyList<RawTaggedField>()
            )
        )
        assertTrue(
            compareRawTaggedFields(
                first = listOf(
                    RawTaggedField(tag = 1, data = byteArrayOf(1)),
                    RawTaggedField(tag = 2, data = byteArrayOf()),
                ),
                second = listOf(
                    RawTaggedField(tag = 1, data = byteArrayOf(1)),
                    RawTaggedField(tag = 2, data = byteArrayOf()),
                ),
            )
        )
    }

    @Test
    fun testConstants() {
        assertEquals(MessageUtil.UNSIGNED_SHORT_MAX, 0xFFFF)
        assertEquals(MessageUtil.UNSIGNED_INT_MAX, 0xFFFFFFFFL)
    }

    @Test
    @Throws(IOException::class)
    fun testBinaryNode() {
        val expected = byteArrayOf(5, 2, 9, 4, 1, 8, 7, 0, 3, 6)
        val writer = StringWriter()
        val mapper = ObjectMapper()

        mapper.writeTree(mapper.createGenerator(writer), BinaryNode(expected))

        val textNode = mapper.readTree(writer.toString())

        assertTrue(textNode.isTextual, "Expected a JSON string but was: $textNode")
        val actual = jsonNodeToBinary(textNode, "Test base64 JSON string")
        assertContentEquals(expected, actual)
    }

    @Test
    fun testInvalidBinaryNode() {
        assertFailsWith<IllegalArgumentException> {
            jsonNodeToBinary(IntNode(42), "Test int to binary")
        }
        assertFailsWith<UncheckedIOException> {
            jsonNodeToBinary(TextNode("This is not base64!"), "Test non-base64 to binary")
        }
    }
}
