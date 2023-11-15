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
import java.nio.channels.ScatteringByteChannel
import org.apache.kafka.test.TestUtils.randomBytes
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.mock
import org.mockito.Mockito.reset
import org.mockito.Mockito.`when`
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NetworkReceiveTest {
    
    @Test
    @Throws(IOException::class)
    fun testBytesRead() {
        val receive = NetworkReceive(source = "0", maxSize = 128)
        assertEquals(0, receive.bytesRead())
        val channel = mock<ScatteringByteChannel>()
        val bufferCaptor = ArgumentCaptor.forClass(ByteBuffer::class.java)
        `when`(channel.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.value.putInt(128)
            4
        }.thenReturn(0)
        assertEquals(4, receive.readFrom(channel))
        assertEquals(4, receive.bytesRead())
        assertFalse(receive.complete())

        reset(channel)
        `when`(channel.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.value.put(randomBytes(64))
            64
        }
        assertEquals(64, receive.readFrom(channel))
        assertEquals(68, receive.bytesRead())
        assertFalse(receive.complete())
        reset(channel)
        `when`(channel.read(bufferCaptor.capture())).thenAnswer {
            bufferCaptor.value.put(randomBytes(64))
            64
        }
        assertEquals(64, receive.readFrom(channel))
        assertEquals(132, receive.bytesRead())
        assertTrue(receive.complete())
    }
}
