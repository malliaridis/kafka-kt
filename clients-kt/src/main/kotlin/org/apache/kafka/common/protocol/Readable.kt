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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.protocol.types.RawTaggedField
import org.apache.kafka.common.record.MemoryRecords

interface Readable {

    fun readByte(): Byte

    fun readShort(): Short

    fun readInt(): Int

    fun readLong(): Long

    fun readDouble(): Double

    fun readArray(length: Int): ByteArray

    fun readUnsignedVarint(): Int

    fun readByteBuffer(length: Int): ByteBuffer

    fun readVarint(): Int

    fun readVarlong(): Long

    fun remaining(): Int

    fun readString(length: Int): String {
        val arr = readArray(length)
        return String(arr, StandardCharsets.UTF_8)
    }

    fun readUnknownTaggedField(
        unknowns: MutableList<RawTaggedField>?,
        tag: Int,
        size: Int
    ): MutableList<RawTaggedField> {
        val unknownList = unknowns ?: mutableListOf()

        val data = readArray(size)
        unknownList.add(RawTaggedField(tag, data))

        return unknownList
    }

    fun readRecords(length: Int): MemoryRecords? {
        return if (length < 0) null // no records
        else {
            val recordsBuffer = readByteBuffer(length)
            MemoryRecords.readableRecords(recordsBuffer)
        }
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    fun readUuid(): Uuid = Uuid(readLong(), readLong())

    fun readUnsignedShort(): Int = java.lang.Short.toUnsignedInt(readShort())

    fun readUnsignedInt(): Long = Integer.toUnsignedLong(readInt())
}
