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

import java.nio.ByteBuffer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ByteBufferInputStreamTest {

    @Test
    fun testReadUnsignedIntFromInputStream() {
        val buffer = ByteBuffer.allocate(8)
        buffer.put(10.toByte())
        buffer.put(20.toByte())
        buffer.put(30.toByte())
        buffer.flip() // prepare for reading

        val b = ByteArray(6)

        val inputStream = ByteBufferInputStream(buffer)
        assertEquals(3, inputStream.available())
        // read two bytes
        assertEquals(10, inputStream.read())
        assertEquals(20, inputStream.read())

        // try to read 3 bytes but only able to read one
        assertEquals(1, inputStream.read(b, 3, b.size - 3))
        // all bytes have been read, no more data to read, return -1
        assertEquals(-1, inputStream.read())

        // rewind input and prepare for read again
        buffer.rewind()

        // read 3 bytes
        assertEquals(3, inputStream.read(b, 0, b.size))
        // all bytes have been read, no more data to read, return -1
        assertEquals(-1, inputStream.read(b, 0, b.size))

        // read 0 bytes
        assertEquals(0, inputStream.read(b, 0, 0))
        assertEquals(-1, inputStream.read())
    }
}
