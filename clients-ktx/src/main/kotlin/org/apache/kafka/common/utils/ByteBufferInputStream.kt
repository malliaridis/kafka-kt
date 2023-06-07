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

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * A byte buffer backed input inputStream
 */
class ByteBufferInputStream(private val buffer: ByteBuffer) : InputStream() {

    override fun read(): Int {
        return if (!buffer.hasRemaining()) -1
        else buffer.get().toInt() and 0xFF
    }

    override fun read(bytes: ByteArray, off: Int, length: Int): Int {
        var len = length

        if (len == 0) return 0
        if (!buffer.hasRemaining()) return -1

        len = len.coerceAtMost(buffer.remaining())
        buffer[bytes, off, len]
        return len
    }
}
