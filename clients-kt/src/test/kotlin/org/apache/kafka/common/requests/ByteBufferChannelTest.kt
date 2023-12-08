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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import org.apache.kafka.common.utils.Utils.utf8
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ByteBufferChannelTest {

    @Test
    fun testWriteBufferArrayWithNonZeroPosition() {
        val data = "hello".toByteArray()
        val buffer = ByteBuffer.allocate(32)
        buffer.position(10)
        buffer.put(data)
        val limit = buffer.position()
        buffer.position(10)
        buffer.limit(limit)
        val channel = ByteBufferChannel(buffer.remaining().toLong())
        val buffers = arrayOf(buffer)
        channel.write(buffers)
        channel.close()
        val channelBuffer = channel.buffer()
        assertEquals(data.size, channelBuffer.remaining())
        assertEquals("hello", utf8(channelBuffer))
    }

    @Test
    fun testWriteMultiplesByteBuffers() {
        val buffers = arrayOf(
            ByteBuffer.wrap("hello".toByteArray()),
            ByteBuffer.wrap("world".toByteArray()),
        )
        val size = buffers.sumOf { it.remaining() }
        var buf: ByteBuffer
        ByteBufferChannel(size.toLong()).use { channel ->
            channel.write(buffers, 1, 1)
            buf = channel.buffer()
        }
        assertEquals("world", utf8(buf))
        ByteBufferChannel(size.toLong()).use { channel ->
            channel.write(buffers, 0, 1)
            buf = channel.buffer()
        }
        assertEquals("hello", utf8(buf))
        ByteBufferChannel(size.toLong()).use { channel ->
            channel.write(buffers, 0, 2)
            buf = channel.buffer()
        }
        assertEquals("helloworld", utf8(buf))
    }

    @Test
    fun testInvalidArgumentsInWritsMultiplesByteBuffers() {
        ByteBufferChannel(10).use { channel ->
            assertFailsWith<IndexOutOfBoundsException> {
                channel.write(
                    srcs = emptyArray(),
                    offset = 1,
                    length = 1,
                )
            }
            assertFailsWith<IndexOutOfBoundsException> {
                channel.write(
                    srcs = emptyArray(),
                    offset = -1,
                    length = 1,
                )
            }
            assertFailsWith<IndexOutOfBoundsException> {
                channel.write(
                    srcs = emptyArray(),
                    offset = 0,
                    length = -1,
                )
            }
            assertFailsWith<IndexOutOfBoundsException> {
                channel.write(
                    srcs = emptyArray(),
                    offset = 0,
                    length = 1,
                )
            }
            assertEquals(
                expected = 0,
                actual = channel.write(
                    srcs = emptyArray(),
                    offset = 0,
                    length = 0,
                ),
            )
        }
    }
}
