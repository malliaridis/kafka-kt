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
import org.apache.kafka.common.utils.ByteUtils

class ByteBufferAccessor(private val buf: ByteBuffer) : Readable, Writable {

    override fun readByte(): Byte = buf.get()

    override fun readShort(): Short = buf.getShort()

    override fun readInt(): Int = buf.getInt()

    override fun readLong(): Long = buf.getLong()

    override fun readDouble(): Double = ByteUtils.readDouble(buf)

    override fun readArray(length: Int): ByteArray {
        val remaining = buf.remaining()
        if (length > remaining) throw RuntimeException(
            "Error reading byte array of $length byte(s): only $remaining byte(s) available"
        )

        val arr = ByteArray(length)
        buf[arr]
        return arr
    }

    override fun readUnsignedVarint(): Int {
        return ByteUtils.readUnsignedVarint(buf)
    }

    override fun readByteBuffer(length: Int): ByteBuffer {
        val res = buf.slice()
        res.limit(length)
        buf.position(buf.position() + length)
        return res
    }

    override fun writeByte(value: Byte) {
        buf.put(value)
    }

    override fun writeShort(value: Short) {
        buf.putShort(value)
    }

    override fun writeInt(value: Int) {
        buf.putInt(value)
    }

    override fun writeLong(value: Long) {
        buf.putLong(value)
    }

    override fun writeDouble(value: Double) {
        ByteUtils.writeDouble(value, buf)
    }

    override fun writeByteArray(arr: ByteArray) {
        buf.put(arr)
    }

    override fun writeUnsignedVarint(i: Int) {
        ByteUtils.writeUnsignedVarint(i, buf)
    }

    override fun writeByteBuffer(buf: ByteBuffer) {
        this.buf.put(buf.duplicate())
    }

    override fun writeVarint(i: Int) {
        ByteUtils.writeVarint(i, buf)
    }

    override fun writeVarlong(i: Long) {
        ByteUtils.writeVarlong(i, buf)
    }

    override fun readVarint(): Int {
        return ByteUtils.readVarint(buf)
    }

    override fun readVarlong(): Long {
        return ByteUtils.readVarlong(buf)
    }

    override fun remaining(): Int {
        return buf.remaining()
    }

    fun flip() {
        buf.flip()
    }

    fun buffer(): ByteBuffer = buf
}
