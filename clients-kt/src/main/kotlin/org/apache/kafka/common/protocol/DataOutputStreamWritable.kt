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

import java.io.Closeable
import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarlong
import org.apache.kafka.common.utils.Utils

class DataOutputStreamWritable(
    private val out: DataOutputStream
) : Writable, Closeable {

    override fun writeByte(value: Byte) {
        try {
            out.writeByte(value.toInt())
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeShort(value: Short) {
        try {
            out.writeShort(value.toInt())
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeInt(value: Int) {
        try {
            out.writeInt(value)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeLong(value: Long) {
        try {
            out.writeLong(value)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeDouble(value: Double) {
        try {
            out.writeDouble(value)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeByteArray(arr: ByteArray) {
        try {
            out.write(arr)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeUnsignedVarint(i: Int) {
        try {
            writeUnsignedVarint(i, out)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeByteBuffer(buf: ByteBuffer) {
        try {
            if (buf.hasArray()) out.write(
                buf.array(),
                buf.arrayOffset() + buf.position(),
                buf.remaining(),
            )
            else {
                val bytes = Utils.toArray(buf)
                out.write(bytes)
            }
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeVarint(i: Int) {
        try {
            writeVarint(i, out)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun writeVarlong(i: Long) {
        try {
            writeVarlong(i, out)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    fun flush() {
        try {
            out.flush()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    override fun close() {
        try {
            out.close()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }
}
