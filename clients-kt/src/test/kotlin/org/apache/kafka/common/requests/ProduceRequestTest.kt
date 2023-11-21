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
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.RecordVersion
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.RequestTestUtils.hasIdempotentRecords
import org.apache.kafka.common.requests.RequestUtils.hasTransactionalRecords
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProduceRequestTest {

    private val simpleRecord = SimpleRecord(
        timestamp = System.currentTimeMillis(),
        key = "key".toByteArray(),
        value = "value".toByteArray(),
    )

    @Test
    fun shouldBeFlaggedAsTransactionalWhenTransactionalRecords() {
        val memoryRecords = MemoryRecords.withTransactionalRecords(
            initialOffset = 0,
            compressionType = CompressionType.NONE,
            producerId = 1L,
            producerEpoch = 1,
            baseSequence = 1,
            partitionLeaderEpoch = 1,
            records = arrayOf(simpleRecord),
        )
        val request = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("topic")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(1)
                                            .setRecords(memoryRecords)
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(-1)
                .setTimeoutMs(10)
        ).build()
        assertTrue(hasTransactionalRecords(request))
    }

    @Test
    fun shouldNotBeFlaggedAsTransactionalWhenNoRecords() {
        val request = createNonIdempotentNonTransactionalRecords()
        assertFalse(hasTransactionalRecords(request))
    }

    @Test
    fun shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent() {
        val request = createNonIdempotentNonTransactionalRecords()
        assertFalse(hasTransactionalRecords(request))
    }

    @Test
    fun shouldBeFlaggedAsIdempotentWhenIdempotentRecords() {
        val memoryRecords = MemoryRecords.withIdempotentRecords(
            initialOffset = 1,
            compressionType = CompressionType.NONE,
            producerId = 1L,
            producerEpoch = 1,
            baseSequence = 1,
            partitionLeaderEpoch = 1,
            records = arrayOf(simpleRecord),
        )
        val request = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("topic")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(1)
                                            .setRecords(memoryRecords)
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(-1)
                .setTimeoutMs(10)
        ).build()
        assertTrue(hasIdempotentRecords(request))
    }

    @Test
    fun testBuildWithOldMessageFormat() {
        val buffer = ByteBuffer.allocate(256)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V1,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        val requestBuilder = ProduceRequest.forMagic(
            magic = RecordBatch.MAGIC_VALUE_V1,
            data = ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(PartitionProduceData().setIndex(9).setRecords(builder.build())),
                                ),
                        ).iterator()
                    )
                )
                .setAcks(1)
                .setTimeoutMs(5000)
        )
        assertEquals(2, requestBuilder.oldestAllowedVersion)
        assertEquals(2, requestBuilder.latestAllowedVersion)
    }

    @Test
    fun testBuildWithCurrentMessageFormat() {
        val buffer = ByteBuffer.allocate(256)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        val requestBuilder = ProduceRequest.forMagic(
            RecordBatch.CURRENT_MAGIC_VALUE,
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData().setName("test").setPartitionData(
                                listOf(PartitionProduceData().setIndex(9).setRecords(builder.build()))
                            )
                        ).iterator()
                    )
                )
                .setAcks(1)
                .setTimeoutMs(5000)
        )
        assertEquals(3, requestBuilder.oldestAllowedVersion.toInt())
        assertEquals(ApiKeys.PRODUCE.latestVersion(), requestBuilder.latestAllowedVersion)
    }

    @Test
    fun testV3AndAboveShouldContainOnlyOneRecordBatch() {
        val buffer = ByteBuffer.allocate(256)
        var builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        builder.close()
        builder = MemoryRecords.builder(
            buffer = buffer,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 1L,
        )
        builder.append(11L, "1".toByteArray(), "b".toByteArray())
        builder.append(12L, null, "c".toByteArray())
        builder.close()
        buffer.flip()
        val requestBuilder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(MemoryRecords.readableRecords(buffer)),
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(1.toShort())
                .setTimeoutMs(5000)
        )
        assertThrowsForAllVersions<InvalidRecordException>(requestBuilder)
    }

    @Test
    fun testV3AndAboveCannotHaveNoRecordBatches() {
        val requestBuilder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(MemoryRecords.EMPTY)
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(1.toShort())
                .setTimeoutMs(5000)
        )
        assertThrowsForAllVersions<InvalidRecordException>(requestBuilder)
    }

    @Test
    fun testV3AndAboveCannotUseMagicV0() {
        val buffer = ByteBuffer.allocate(256)
        val builder = MemoryRecords.builder(
            buffer, RecordBatch.MAGIC_VALUE_V0, CompressionType.NONE,
            TimestampType.NO_TIMESTAMP_TYPE, 0L
        )
        builder.append(10L, null, "a".toByteArray())
        val requestBuilder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(builder.build())
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(1.toShort())
                .setTimeoutMs(5000)
        )
        assertThrowsForAllVersions<InvalidRecordException>(requestBuilder)
    }

    @Test
    fun testV3AndAboveCannotUseMagicV1() {
        val buffer = ByteBuffer.allocate(256)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V1,
            compressionType = CompressionType.NONE,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        val requestBuilder = ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("test")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(builder.build())
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(1)
                .setTimeoutMs(5000)
        )
        assertThrowsForAllVersions<InvalidRecordException>(requestBuilder)
    }

    @Test
    fun testV6AndBelowCannotUseZStdCompression() {
        val buffer = ByteBuffer.allocate(256)
        val builder = MemoryRecords.builder(
            buffer = buffer,
            magic = RecordBatch.MAGIC_VALUE_V2,
            compressionType = CompressionType.ZSTD,
            timestampType = TimestampType.CREATE_TIME,
            baseOffset = 0L,
        )
        builder.append(10L, null, "a".toByteArray())
        val produceData = ProduceRequestData()
            .setTopicData(
                TopicProduceDataCollection(
                    listOf(
                        TopicProduceData()
                            .setName("test")
                            .setPartitionData(
                                listOf(
                                    PartitionProduceData()
                                        .setIndex(0)
                                        .setRecords(builder.build())
                                )
                            )
                    ).iterator()
                )
            )
            .setAcks(1)
            .setTimeoutMs(1000)
        
        // Can't create ProduceRequest instance with version within [3, 7)
        for (version in 3..6) {
            val requestBuilder = ProduceRequest.Builder(
                minVersion = version.toShort(),
                maxVersion = version.toShort(),
                data = produceData,
            )
            assertThrowsForAllVersions<UnsupportedCompressionTypeException>(requestBuilder)
        }

        // Works fine with current version (>= 7)
        ProduceRequest.forCurrentMagic(produceData)
    }

    @Test
    fun testMixedTransactionalData() {
        val producerId = 15L
        val producerEpoch: Short = 5
        val sequence = 10
        val nonTxnRecords = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "foo".toByteArray())),
        )
        val txnRecords = MemoryRecords.withTransactionalRecords(
            compressionType = CompressionType.NONE,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = sequence,
            records = arrayOf(SimpleRecord(value = "bar".toByteArray())),
        )
        val builder = ProduceRequest.forMagic(
            magic = RecordBatch.CURRENT_MAGIC_VALUE,
            data = ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData().setName("foo").setPartitionData(
                                listOf(PartitionProduceData().setIndex(0).setRecords(txnRecords))
                            ),
                            TopicProduceData().setName("foo").setPartitionData(
                                listOf(PartitionProduceData().setIndex(1).setRecords(nonTxnRecords))
                            )
                        ).iterator()
                    )
                )
                .setAcks(-1)
                .setTimeoutMs(5000)
        )
        val request = builder.build()
        assertTrue(hasTransactionalRecords(request))
        assertTrue(hasIdempotentRecords(request))
    }

    @Test
    fun testMixedIdempotentData() {
        val producerId = 15L
        val producerEpoch: Short = 5
        val sequence = 10
        val nonTxnRecords = MemoryRecords.withRecords(
            compressionType = CompressionType.NONE,
            records = arrayOf(SimpleRecord(value = "foo".toByteArray()))
        )
        val txnRecords = MemoryRecords.withIdempotentRecords(
            compressionType = CompressionType.NONE,
            producerId = producerId,
            producerEpoch = producerEpoch,
            baseSequence = sequence,
            records = arrayOf(SimpleRecord(value = "bar".toByteArray())),
        )
        val builder = ProduceRequest.forMagic(
            RecordVersion.current().value,
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData().setName("foo").setPartitionData(
                                listOf(PartitionProduceData().setIndex(0).setRecords(txnRecords))
                            ),
                            TopicProduceData().setName("foo").setPartitionData(
                                listOf(PartitionProduceData().setIndex(1).setRecords(nonTxnRecords))
                            ),
                        ).iterator()
                    )
                )
                .setAcks(-1)
                .setTimeoutMs(5000)
        )
        val request = builder.build()
        assertFalse(hasTransactionalRecords(request))
        assertTrue(hasIdempotentRecords(request))
    }

    private fun createNonIdempotentNonTransactionalRecords(): ProduceRequest {
        return ProduceRequest.forCurrentMagic(
            ProduceRequestData()
                .setTopicData(
                    TopicProduceDataCollection(
                        listOf(
                            TopicProduceData()
                                .setName("topic")
                                .setPartitionData(
                                    listOf(
                                        PartitionProduceData()
                                            .setIndex(1)
                                            .setRecords(
                                                MemoryRecords.withRecords(
                                                    compressionType = CompressionType.NONE,
                                                    records = arrayOf(simpleRecord),
                                                )
                                            )
                                    )
                                )
                        ).iterator()
                    )
                )
                .setAcks(-1)
                .setTimeoutMs(10)
        ).build()
    }

    companion object {

        private inline fun <reified T : Throwable> assertThrowsForAllVersions(builder: ProduceRequest.Builder) {
            (builder.oldestAllowedVersion.toInt() until builder.latestAllowedVersion + 1)
                .forEach { version: Int ->
                    assertFailsWith<T> { builder.build(version.toShort()).serialize() }
                }
        }
    }
}
