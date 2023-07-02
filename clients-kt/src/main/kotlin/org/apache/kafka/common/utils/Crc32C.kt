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
 * A class that can be used to compute the CRC32C (Castagnoli) of a ByteBuffer or array of bytes.
 *
 * We use java.util.zip.CRC32C (introduced in Java 9) if it is available and fallback to
 * PureJavaCrc32C, otherwise. java.util.zip.CRC32C is significantly faster on reasonably modern CPUs
 * as it uses the CRC32 instruction introduced in SSE4.2.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
object Crc32C {

    private val CHECKSUM_FACTORY: ChecksumFactory

    init {
        CHECKSUM_FACTORY =
            if (Java.IS_JAVA9_COMPATIBLE) Java9ChecksumFactory()
            else PureJavaChecksumFactory()
    }

    /**
     * Compute the CRC32C (Castagnoli) of the segment of the byte array given by the specified size
     * and offset.
     *
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin the checksum computation
     * @param size the number of bytes to checksum
     * @return The CRC32C
     */
    fun compute(bytes: ByteArray?, offset: Int, size: Int): Long {
        val crc = create()
        crc.update(bytes, offset, size)
        return crc.value
    }

    /**
     * Compute the CRC32C (Castagnoli) of a byte buffer from a given offset (relative to the
     * buffer's current position).
     *
     * @param buffer The buffer with the underlying data
     * @param offset The offset relative to the current position
     * @param size The number of bytes beginning from the offset to include
     * @return The CRC32C
     */
    fun compute(buffer: ByteBuffer?, offset: Int, size: Int): Long {
        val crc = create()
        Checksums.update(crc, buffer, offset, size)
        return crc.value
    }

    fun create(): Checksum {
        return CHECKSUM_FACTORY.create()
    }

    private interface ChecksumFactory {
        fun create(): Checksum
    }

    private class Java9ChecksumFactory : ChecksumFactory {

        override fun create(): Checksum {
            return try {
                CONSTRUCTOR.invoke() as Checksum
            } catch (throwable: Throwable) {
                // Should never happen
                throw RuntimeException(throwable)
            }
        }

        companion object {

            private val CONSTRUCTOR: MethodHandle

            init {
                try {
                    val cls = Class.forName("java.util.zip.CRC32C")
                    CONSTRUCTOR = MethodHandles.publicLookup()
                        .findConstructor(cls, MethodType.methodType(Void.TYPE))
                } catch (e: ReflectiveOperationException) {
                    // Should never happen
                    throw RuntimeException(e)
                }
            }
        }
    }

    private class PureJavaChecksumFactory : ChecksumFactory {
        override fun create(): Checksum = PureJavaCrc32C()
    }
}
