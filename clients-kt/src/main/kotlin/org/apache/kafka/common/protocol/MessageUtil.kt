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

import com.fasterxml.jackson.databind.JsonNode
import java.io.IOException
import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.protocol.types.RawTaggedField
import org.apache.kafka.common.utils.Utils

object MessageUtil {

    const val UNSIGNED_INT_MAX = 4294967295L

    const val UNSIGNED_SHORT_MAX = 65535

    /**
     * Copy a byte buffer into an array. This will not affect the buffer's position or mark.
     */
    fun byteBufferToArray(buf: ByteBuffer): ByteArray {
        val arr = ByteArray(buf.remaining())
        val prevPosition = buf.position()

        try {
            buf[arr]
        } finally {
            buf.position(prevPosition)
        }

        return arr
    }

    fun deepToString(iter: Iterator<*>): String {
        val bld = StringBuilder("[")
        var prefix = ""

        while (iter.hasNext()) {
            val any = iter.next()!!
            bld.append(prefix)
            bld.append(any.toString())
            prefix = ", "
        }

        bld.append("]")
        return bld.toString()
    }

    fun jsonNodeToByte(node: JsonNode, about: String): Byte {
        var value = jsonNodeToInt(node, about)
        if (value > Byte.MAX_VALUE) {
            if (value <= 256)
            // It's more traditional to refer to bytes as unsigned,
            // so we support that here.
                value -= 128
            else throw RuntimeException(
                "$about: value $value does not fit in an 8-bit signed integer."
            )
        }

        if (value < Byte.MIN_VALUE) throw RuntimeException(
            "$about: value $value does not fit in an 8-bit signed integer."
        )

        return value.toByte()
    }

    fun jsonNodeToShort(node: JsonNode, about: String): Short {
        val value = jsonNodeToInt(node, about)

        if ((value < Short.MIN_VALUE) || (value > Short.MAX_VALUE))
            throw RuntimeException(
                "$about: value $value does not fit in a 16-bit signed integer."
            )

        return value.toShort()
    }

    fun jsonNodeToUnsignedShort(node: JsonNode, about: String): UShort {
        val value = jsonNodeToInt(node, about)

        if (value < 0 || value > UNSIGNED_SHORT_MAX) throw RuntimeException(
            "$about: value $value does not fit in a 16-bit unsigned integer."
        )

        return value.toUShort()
    }

    fun jsonNodeToUnsignedInt(node: JsonNode, about: String): Long {
        val value = jsonNodeToLong(node, about)

        if (value < 0 || value > UNSIGNED_INT_MAX) throw RuntimeException(
            "$about: value $value does not fit in a 32-bit unsigned integer."
        )

        return value
    }

    fun jsonNodeToInt(node: JsonNode, about: String): Int {
        if (node.isInt) return node.asInt()

        if (node.isTextual) throw NumberFormatException(
            "$about: expected an integer or string type, but got ${node.nodeType}"
        )

        val text = node.asText()
        return if (text.startsWith("0x")) try {
            text.substring(2).toInt(16)
        } catch (e: NumberFormatException) {
            throw NumberFormatException(
                "$about: failed to parse hexadecimal number: ${e.message}"
            )
        } else try {
            text.toInt()
        } catch (e: NumberFormatException) {
            throw NumberFormatException("$about: failed to parse number: ${e.message}")
        }
    }

    fun jsonNodeToLong(node: JsonNode, about: String): Long {
        if (node.isLong) return node.asLong()

        if (node.isTextual) throw NumberFormatException(
            "$about: expected an integer or string type, but got ${node.nodeType}"
        )

        val text = node.asText()
        return if (text.startsWith("0x")) try {
            text.substring(2).toLong(16)
        } catch (e: NumberFormatException) {
            throw NumberFormatException(
                "$about: failed to parse hexadecimal number: ${e.message}"
            )
        } else try {
            text.toLong()
        } catch (e: NumberFormatException) {
            throw NumberFormatException("$about: failed to parse number: ${e.message}")
        }
    }

    fun jsonNodeToBinary(node: JsonNode, about: String): ByteArray {
        if (!node.isBinary) throw RuntimeException("$about: expected Base64-encoded binary data.")

        try {
            return node.binaryValue()
        } catch (e: IOException) {
            throw RuntimeException("$about: unable to retrieve Base64-encoded binary data", e)
        }
    }

    fun jsonNodeToDouble(node: JsonNode, about: String): Double {
        if (!node.isFloatingPointNumber) throw NumberFormatException(
            "$about: expected a floating point type, but got ${node.nodeType}"
        )

        return node.asDouble()
    }

    fun duplicate(array: ByteArray): ByteArray = array.copyOf(array.size)

    /**
     * Compare two RawTaggedFields lists. A `null` list is equivalent to an empty one in this
     * context.
     */
    fun compareRawTaggedFields(
        first: List<RawTaggedField?>?,
        second: List<RawTaggedField?>?
    ): Boolean {
        return first.isNullOrEmpty() && second.isNullOrEmpty() || first == second
    }

    fun toByteBuffer(message: Message, version: Short): ByteBuffer {
        val cache = ObjectSerializationCache()
        val messageSize = message.size(cache, version)
        val bytes = ByteBufferAccessor(ByteBuffer.allocate(messageSize))
        message.write(bytes, cache, version)
        bytes.flip()
        return bytes.buffer()
    }

    fun toVersionPrefixedByteBuffer(version: Short, message: Message): ByteBuffer {
        val cache = ObjectSerializationCache()
        val messageSize = message.size(cache, version)
        val bytes = ByteBufferAccessor(ByteBuffer.allocate(messageSize + 2))
        bytes.writeShort(version)
        message.write(bytes, cache, version)
        bytes.flip()
        return bytes.buffer()
    }

    fun toVersionPrefixedBytes(version: Short, message: Message): ByteArray {
        val buffer = toVersionPrefixedByteBuffer(version, message)
        // take the inner array directly if it is full with data
        return if (
            buffer.hasArray()
            && buffer.arrayOffset() == 0
            && buffer.position() == 0
            && buffer.limit() == buffer.array().size
        ) buffer.array()
        else Utils.toArray(buffer)
    }
}
