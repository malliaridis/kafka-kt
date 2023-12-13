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

import java.nio.ByteBuffer
import org.apache.kafka.common.compress.KafkaLZ4BlockInputStream
import org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.ChunkedBytesStream
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class CompressionTypeTest {

    @Test
    fun testLZ4FramingMagicV0() {
        val buffer = ByteBuffer.allocate(256)
        val out = CompressionType.LZ4.wrapForOutput(
            bufferStream = ByteBufferOutputStream(buffer),
            messageVersion = RecordBatch.MAGIC_VALUE_V0,
        ) as KafkaLZ4BlockOutputStream
        assertTrue(out.useBrokenFlagDescriptorChecksum())
        buffer.rewind()

        val input = CompressionType.LZ4.wrapForInput(
            buffer = buffer,
            messageVersion = RecordBatch.MAGIC_VALUE_V0,
            decompressionBufferSupplier = BufferSupplier.NO_CACHING,
        ) as ChunkedBytesStream
        val stream = assertIs<KafkaLZ4BlockInputStream>(input.sourceStream())
        assertTrue(stream.ignoreFlagDescriptorChecksum())
    }

    @Test
    fun testLZ4FramingMagicV1() {
        val buffer = ByteBuffer.allocate(256)
        val out = CompressionType.LZ4.wrapForOutput(
            bufferStream = ByteBufferOutputStream(buffer),
            messageVersion = RecordBatch.MAGIC_VALUE_V1
        ) as KafkaLZ4BlockOutputStream
        assertFalse(out.useBrokenFlagDescriptorChecksum())
        buffer.rewind()

        val input = CompressionType.LZ4.wrapForInput(
            buffer = buffer,
            messageVersion = RecordBatch.MAGIC_VALUE_V1,
            decompressionBufferSupplier = BufferSupplier.create()
        ) as ChunkedBytesStream
        val stream = assertIs<KafkaLZ4BlockInputStream>(input.sourceStream())
        assertFalse(stream.ignoreFlagDescriptorChecksum())
    }
}
