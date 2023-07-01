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

package org.apache.kafka.common.utils

import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.nio.ByteBuffer
import java.util.zip.Checksum

/**
 * Utility methods for `Checksum` instances.
 *
 * Implementation note: we can add methods to our implementations of CRC32 and CRC32C, but we cannot
 * do the same for the Java implementations (we prefer the Java 9 implementation of CRC32C if
 * available). A utility class is the simplest way to add methods that are useful for all Checksum
 * implementations.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
object Checksums {

    private val BYTE_BUFFER_UPDATE: MethodHandle?

    init {
        var byteBufferUpdate: MethodHandle? = null

        if (Java.IS_JAVA9_COMPATIBLE) {
            try {
                byteBufferUpdate = MethodHandles.publicLookup().findVirtual(
                    Checksum::class.java,
                    "update",
                    MethodType.methodType(Void.TYPE, ByteBuffer::class.java)
                )
            } catch (t: Throwable) {
                handleUpdateThrowable(t)
            }
        }

        BYTE_BUFFER_UPDATE = byteBufferUpdate
    }

    /**
     * Uses [Checksum.update] on `buffer`'s content, without modifying its position and limit.<br></br>
     * This is semantically equivalent to [.update] with `offset = 0`.
     */
    fun update(checksum: Checksum, buffer: ByteBuffer, length: Int) {
        update(checksum, buffer, 0, length)
    }

    /**
     * Uses [Checksum.update] on `buffer`'s content, starting from the given `offset`
     * by the provided `length`, without modifying its position and limit.
     */
    fun update(checksum: Checksum, buffer: ByteBuffer, offset: Int, length: Int) {
        if (buffer.hasArray()) {
            checksum.update(
                buffer.array(),
                buffer.position() + buffer.arrayOffset() + offset,
                length
            )
        } else if (BYTE_BUFFER_UPDATE != null && buffer.isDirect) {
            val oldPosition = buffer.position()
            val oldLimit = buffer.limit()
            try {
                // save a slice to be used to save an allocation in the hot-path
                val start = oldPosition + offset
                buffer.limit(start + length)
                buffer.position(start)
                BYTE_BUFFER_UPDATE.invokeExact(checksum, buffer)
            } catch (t: Throwable) {
                handleUpdateThrowable(t)
            } finally {
                // reset buffer's offsets
                buffer.limit(oldLimit)
                buffer.position(oldPosition)
            }
        } else {
            // slow-path
            val start = buffer.position() + offset
            for (i in start until start + length) {
                checksum.update(buffer[i].toInt())
            }
        }
    }

    private fun handleUpdateThrowable(t: Throwable) {
        when (t) {
            is RuntimeException,
            is Error -> throw t
            else -> throw IllegalStateException(t)
        }
    }

    fun updateInt(checksum: Checksum, input: Int) {
        checksum.update((input shr 24).toByte().toInt())
        checksum.update((input shr 16).toByte().toInt())
        checksum.update((input shr 8).toByte().toInt())
        checksum.update(input.toByte().toInt() /* >> 0 */)
    }

    fun updateLong(checksum: Checksum, input: Long) {
        checksum.update((input shr 56).toByte().toInt())
        checksum.update((input shr 48).toByte().toInt())
        checksum.update((input shr 40).toByte().toInt())
        checksum.update((input shr 32).toByte().toInt())
        checksum.update((input shr 24).toByte().toInt())
        checksum.update((input shr 16).toByte().toInt())
        checksum.update((input shr 8).toByte().toInt())
        checksum.update(input.toByte().toInt() /* >> 0 */)
    }
}
