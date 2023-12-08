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

package org.apache.kafka.common.record

import java.io.IOException
import java.nio.ByteBuffer
import java.util.LinkedList
import java.util.Queue
import org.apache.kafka.common.network.ByteBufferSend
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.ByteBufferChannel
import org.apache.kafka.test.TestUtils.randomBytes
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MultiRecordsSendTest {

    @Test
    @Throws(IOException::class)
    fun testSendsFreedAfterWriting() {
        val numChunks = 4
        val chunkSize = 32
        val totalSize = numChunks * chunkSize
        val sends: Queue<Send> = LinkedList()
        val chunks = arrayOfNulls<ByteBuffer>(numChunks)
        for (i in 0 until numChunks) {
            val buffer = ByteBuffer.wrap(randomBytes(chunkSize))
            chunks[i] = buffer
            sends.add(ByteBufferSend(buffer))
        }
        val send = MultiRecordsSend(sends)
        assertEquals(totalSize.toLong(), send.size())
        for (i in 0 until numChunks) {
            assertEquals(numChunks - i, send.numResidentSends())
            val out = NonOverflowingByteBufferChannel(chunkSize.toLong())
            send.writeTo(out)
            out.close()
            assertEquals(chunks[i], out.buffer())
        }
        assertEquals(0, send.numResidentSends())
        assertTrue(send.completed())
    }

    private class NonOverflowingByteBufferChannel(size: Long) : ByteBufferChannel(size) {
        override fun write(srcs: Array<ByteBuffer>): Long {
            // Instead of overflowing, this channel refuses additional writes once the buffer is full,
            // which allows us to test the MultiRecordsSend behavior on a per-send basis.
            return if (!buffer().hasRemaining()) 0 else super.write(srcs)
        }
    }
}
