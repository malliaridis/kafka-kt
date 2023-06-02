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

class ByteBufferAccessor(
    private val buf: ByteBuffer
) : Readable, Writable {

    override fun readByte(): Byte {
        return buf.get()
    }

    override fun readShort(): Short {
        return buf.getShort()
    }

    override fun readInt(): Int {
        return buf.getInt()
    }

    override fun readLong(): Long {
        return buf.getLong()
    }

    override fun readDouble(): Double {
        return ByteUtils.readDouble(buf)
    }

    override fun readArray(size: Int): ByteArray {
        val remaining = buf.remaining()
        if (size > remaining) {
            throw RuntimeException(
                "Error reading byte array of $size byte(s): only $remaining" +
                        " byte(s) available"
            )
        }
        val arr = ByteArray(size)
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

    override fun writeByte(`val`: Byte) {
        buf.put(`val`)
    }

    override fun writeShort(`val`: Short) {
        buf.putShort(`val`)
    }

    override fun writeInt(`val`: Int) {
        buf.putInt(`val`)
    }

    override fun writeLong(`val`: Long) {
        buf.putLong(`val`)
    }

    override fun writeDouble(`val`: Double) {
        ByteUtils.writeDouble(`val`, buf)
    }

    override fun writeByteArray(arr: ByteArray) {
        buf.put(arr)
    }

    override fun writeUnsignedVarint(i: Int) {
        ByteUtils.writeUnsignedVarint(i, buf)
    }

    override fun writeByteBuffer(src: ByteBuffer) {
        buf.put(src.duplicate())
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

    fun buffer(): ByteBuffer {
        return buf
    }
}
