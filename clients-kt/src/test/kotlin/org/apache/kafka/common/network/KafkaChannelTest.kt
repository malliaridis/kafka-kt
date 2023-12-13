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

package org.apache.kafka.common.network

import java.io.IOException
import java.nio.ByteBuffer
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.test.TestUtils.randomBytes
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KafkaChannelTest {

    @Test
    @Throws(IOException::class)
    fun testSending() {
        val authenticator = mock<Authenticator>()
        val transport = mock<TransportLayer>()
        val pool = mock<MemoryPool>()
        val metadataRegistry = mock<ChannelMetadataRegistry>()

        val channel = KafkaChannel(
            id = "0",
            transportLayer = transport,
            authenticatorCreator = { authenticator },
            maxReceiveSize = 1024,
            memoryPool = pool,
            metadataRegistry = metadataRegistry,
        )
        val send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(randomBytes(128)))
        val networkSend = NetworkSend("0", send)
        channel.setSend(networkSend)
        assertTrue(channel.hasSend())
        assertFailsWith<IllegalStateException> { channel.setSend(networkSend) }

        whenever(transport.write(any<Array<ByteBuffer>>())).thenReturn(4L)
        assertEquals(4L, channel.write())
        assertEquals(128, send.remaining)
        assertNull(channel.maybeCompleteSend())

        whenever(transport.write(any<Array<ByteBuffer>>())).thenReturn(64L)
        assertEquals(64, channel.write())
        assertEquals(64, send.remaining)
        assertNull(channel.maybeCompleteSend())

        whenever(transport.write(any<Array<ByteBuffer>>())).thenReturn(64L)
        assertEquals(64, channel.write())
        assertEquals(0, send.remaining)
        assertEquals(networkSend, channel.maybeCompleteSend())
    }

    @Test
    @Throws(IOException::class)
    fun testReceiving() {
        val authenticator = mock<Authenticator>()
        val transport = mock<TransportLayer>()
        val pool = mock<MemoryPool>()
        val metadataRegistry = mock<ChannelMetadataRegistry>()

        val sizeCaptor = argumentCaptor<Int>()
        whenever(pool.tryAllocate(sizeCaptor.capture())).thenAnswer { ByteBuffer.allocate(sizeCaptor.lastValue) }
        val channel = KafkaChannel(
            id = "0",
            transportLayer = transport,
            authenticatorCreator = { authenticator },
            maxReceiveSize = 1024,
            memoryPool = pool,
            metadataRegistry = metadataRegistry
        )

        val bufferCaptor = argumentCaptor<ByteBuffer>()
        whenever(transport.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.lastValue.putInt(128)
            4
        }.thenReturn(0)
        assertEquals(4, channel.read())
        assertEquals(4, channel.currentReceive()!!.bytesRead())
        assertNull(channel.maybeCompleteReceive())

        reset(transport)
        whenever(transport.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.lastValue.put(randomBytes(64))
            64
        }
        assertEquals(64, channel.read())
        assertEquals(68, channel.currentReceive()!!.bytesRead())
        assertNull(channel.maybeCompleteReceive())

        reset(transport)
        whenever(transport.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.lastValue.put(randomBytes(64))
            64
        }
        assertEquals(64, channel.read())
        assertEquals(132, channel.currentReceive()!!.bytesRead())
        assertNotNull(channel.maybeCompleteReceive())
        assertNull(channel.currentReceive())
    }
}
