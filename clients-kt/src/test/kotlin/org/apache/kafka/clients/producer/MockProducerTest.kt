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

package org.apache.kafka.clients.producer

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.test.MockSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

class MockProducerTest {

    private val topic = "topic"

    private lateinit var producer: MockProducer<ByteArray?, ByteArray?>

    private val record1: ProducerRecord<ByteArray?, ByteArray?> = ProducerRecord(
        topic = topic,
        key = "key1".toByteArray(),
        value = "value1".toByteArray(),
    )

    private val record2: ProducerRecord<ByteArray?, ByteArray?> = ProducerRecord(
        topic = topic,
        key = "key2".toByteArray(),
        value = "value2".toByteArray(),
    )

    private val groupId = "group"

    private fun buildMockProducer(autoComplete: Boolean) {
        producer = MockProducer(
            autoComplete = autoComplete,
            keySerializer = MockSerializer(),
            valueSerializer = MockSerializer(),
        )
    }

    @AfterEach
    fun cleanup() {
        if (::producer.isInitialized && !producer.closed) producer.close()
    }

    @Test
    @Throws(Exception::class)
    fun testAutoCompleteMock() {
        buildMockProducer(true)
        val metadata = producer.send(record1)

        assertTrue(metadata.isDone, "Send should be immediately complete")
        assertFalse(isError(metadata), "Send should be successful")
        assertEquals(0L, metadata.get().offset, "Offset should be 0")
        assertEquals(topic, metadata.get().topic)
        assertEquals(listOf(record1), producer.history(), "We should have the record in our history")

        producer.clear()

        assertEquals(0, producer.history().size, "Clear should erase our history")
    }

    @Test
    @Throws(Exception::class)
    fun testPartitioner() {
        val partitionInfo0 = PartitionInfo(
            topic = topic,
            partition = 0,
            leader = null,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val partitionInfo1 = PartitionInfo(
            topic = topic,
            partition = 1,
            leader = null,
            replicas = emptyList(),
            inSyncReplicas = emptyList(),
        )
        val cluster = Cluster(
            clusterId = null,
            nodes = emptyList(),
            partitions = listOf(partitionInfo0, partitionInfo1),
            unauthorizedTopics = emptySet(),
            invalidTopics = emptySet()
        )
        val producer = MockProducer(
            cluster = cluster,
            autoComplete = true,
            keySerializer = StringSerializer(),
            valueSerializer = StringSerializer(),
        )
        val record = ProducerRecord(
            topic = topic,
            key = "key",
            value = "value",
        )
        val metadata = producer.send(record)

        assertEquals(1, metadata.get().partition, "Partition should be correct")

        producer.clear()

        assertEquals(0, producer.history().size, "Clear should erase our history")

        producer.close()
    }

    @Test
    @Throws(Exception::class)
    fun testManualCompletion() {
        buildMockProducer(false)
        val md1 = producer.send(record1)

        assertFalse(md1.isDone, "Send shouldn't have completed")

        val md2 = producer.send(record2)

        assertFalse(md2.isDone, "Send shouldn't have completed")
        assertTrue(producer.completeNext(), "Complete the first request")
        assertFalse(isError(md1), "Requst should be successful")
        assertFalse(md2.isDone, "Second request still incomplete")

        val e = IllegalArgumentException("blah")

        assertTrue(producer.errorNext(e), "Complete the second request with an error")

        try {
            md2.get()
            fail("Expected error to be thrown")
        } catch (err: ExecutionException) {
            assertEquals(e, err.cause)
        }
        assertFalse(producer.completeNext(), "No more requests to complete")
        val md3 = producer.send(record1)
        val md4 = producer.send(record2)
        assertTrue(!md3.isDone && !md4.isDone, "Requests should not be completed.")
        producer.flush()
        assertTrue(md3.isDone && md4.isDone, "Requests should be completed.")
    }

    @Test
    fun shouldInitTransactions() {
        buildMockProducer(true)
        producer.initTransactions()
        assertTrue(producer.transactionInitialized)
    }

    @Test
    fun shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions() {
        buildMockProducer(true)
        producer.initTransactions()

        assertFailsWith<IllegalStateException> { producer.initTransactions() }
    }

    @Test
    fun shouldThrowOnBeginTransactionIfTransactionsNotInitialized() {
        buildMockProducer(true)

        assertFailsWith<IllegalStateException> { producer.beginTransaction() }
    }

    @Test
    fun shouldBeginTransactions() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertTrue(producer.transactionInFlight)
    }

    @Test
    fun shouldThrowOnBeginTransactionsIfTransactionInflight() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertFailsWith<IllegalStateException> { producer.beginTransaction() }
    }

    @Test
    fun shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized() {
        buildMockProducer(true)

        assertFailsWith<IllegalStateException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
        }
    }

    @Test
    fun shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true)
        producer.initTransactions()

        assertFailsWith<IllegalStateException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
        }
    }

    @Test
    fun shouldThrowOnCommitIfTransactionsNotInitialized() {
        buildMockProducer(true)

        assertFailsWith<IllegalStateException> { producer.commitTransaction() }
    }

    @Test
    fun shouldThrowOnCommitTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true)
        producer.initTransactions()

        assertFailsWith<IllegalStateException> { producer.commitTransaction() }
    }

    @Test
    fun shouldCommitEmptyTransaction() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.commitTransaction()

        assertFalse(producer.transactionInFlight)
        assertTrue(producer.transactionCommitted)
        assertFalse(producer.transactionAborted)
    }

    @Test
    fun shouldCountCommittedTransaction() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertEquals(0L, producer.commitCount)

        producer.commitTransaction()

        assertEquals(1L, producer.commitCount)
    }

    @Test
    fun shouldNotCountAbortedTransaction() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.abortTransaction()
        producer.beginTransaction()
        producer.commitTransaction()

        assertEquals(1L, producer.commitCount)
    }

    @Test
    fun shouldThrowOnAbortIfTransactionsNotInitialized() {
        buildMockProducer(true)

        assertFailsWith<IllegalStateException> { producer.abortTransaction() }
    }

    @Test
    fun shouldThrowOnAbortTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true)
        producer.initTransactions()

        assertFailsWith<IllegalStateException> { producer.abortTransaction() }
    }

    @Test
    fun shouldAbortEmptyTransaction() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.abortTransaction()

        assertFalse(producer.transactionInFlight)
        assertTrue(producer.transactionAborted)
        assertFalse(producer.transactionCommitted)
    }

    @Test
    fun shouldThrowFenceProducerIfTransactionsNotInitialized() {
        buildMockProducer(true)

        assertFailsWith<IllegalStateException> { producer.fenceProducer() }
    }

    @Test
    fun shouldThrowOnBeginTransactionsIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        assertFailsWith<ProducerFencedException> { producer.beginTransaction() }
    }

    @Test
    fun shouldThrowOnSendIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        val e = assertFailsWith<KafkaException> {
            producer.send(ProducerRecord(topic = "any", value = "any".toByteArray()))
        }

        assertTrue(
            actual = e.cause is ProducerFencedException,
            message = "The root cause of the exception should be ProducerFenced",
        )
    }

    @Test
    fun shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        assertFailsWith<ProducerFencedException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
        }
    }

    @Test
    fun shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        assertFailsWith<ProducerFencedException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
        }
    }

    @Test
    fun shouldThrowOnCommitTransactionIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        assertFailsWith<ProducerFencedException> { producer.commitTransaction() }
    }

    @Test
    fun shouldThrowOnAbortTransactionIfProducerGotFenced() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.fenceProducer()

        assertFailsWith<ProducerFencedException> { producer.abortTransaction() }
    }

    @Test
    fun shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(record1)
        producer.send(record2)

        assertTrue(producer.history().isEmpty())

        producer.commitTransaction()
        val expectedResult = listOf(record1, record2)

        assertEquals(expectedResult, producer.history())
    }

    @Test
    fun shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled() {
        buildMockProducer(false)
        producer.initTransactions()
        producer.beginTransaction()
        val md1 = producer.send(record1)
        val md2 = producer.send(record2)

        assertFalse(md1.isDone)
        assertFalse(md2.isDone)

        producer.commitTransaction()

        assertTrue(md1.isDone)
        assertTrue(md2.isDone)
    }

    @Test
    fun shouldDropMessagesOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(record1)
        producer.send(record2)
        producer.abortTransaction()

        assertTrue(producer.history().isEmpty())

        producer.beginTransaction()
        producer.commitTransaction()

        assertTrue(producer.history().isEmpty())
    }

    @Test
    fun shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled() {
        buildMockProducer(false)
        producer.initTransactions()
        producer.beginTransaction()
        val md1 = producer.send(record1)

        assertFalse(md1.isDone)

        producer.abortTransaction()

        assertTrue(md1.isDone)
    }

    @Test
    fun shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(record1)
        producer.send(record2)
        producer.commitTransaction()
        producer.beginTransaction()
        producer.abortTransaction()
        val expectedResult = listOf(record1, record2)

        assertEquals(expectedResult, producer.history())
    }

    @Test
    fun shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        val group1 = "g1"
        val group1Commit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 73L, leaderEpoch = null),
        )
        val group2 = "g2"
        val group2Commit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 101L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 21L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(group1Commit, ConsumerGroupMetadata(group1))
        producer.sendOffsetsToTransaction(group2Commit, ConsumerGroupMetadata(group2))

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty())

        val expectedResult = mapOf(
            group1 to group1Commit,
            group2 to group2Commit,
        )
        producer.commitTransaction()

        assertEquals(listOf(expectedResult), producer.consumerGroupOffsetsHistory())
    }

    @Deprecated("")
    @Test
    @Disabled("Kotlin Migration: Consumer group ID is not nullable")
    fun shouldThrowOnNullConsumerGroupIdWhenSendOffsetsToTransaction() {
//        buildMockProducer(true)
//        producer.initTransactions()
//        producer.beginTransaction()
//
//        assertFailsWith<NullPointerException> {
//            producer.sendOffsetsToTransaction(
//                offsets = emptyMap(),
//                consumerGroupId = null,
//            )
//        }
    }

    @Test
    @Disabled("Kotlin Migration: Consumer group metadata group ID is not nullable")
    fun shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction() {
//        buildMockProducer(true)
//        producer.initTransactions()
//        producer.beginTransaction()
//
//        assertFailsWith<NullPointerException> {
//            producer.sendOffsetsToTransaction(
//                offsets = emptyMap(),
//                groupMetadata = ConsumerGroupMetadata(groupId = null),
//            )
//        }
    }

    @Deprecated("")
    @Test
    fun shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupId() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.sendOffsetsToTransaction(
            offsets = emptyMap(),
            consumerGroupId = "groupId",
        )

        assertFalse(producer.sentOffsets)
    }

    @Test
    fun shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()
        producer.sendOffsetsToTransaction(
            offsets = emptyMap(),
            groupMetadata = ConsumerGroupMetadata("groupId"),
        )

        assertFalse(producer.sentOffsets)
    }

    @Deprecated("")
    @Test
    fun shouldAddOffsetsWhenSendOffsetsToTransactionByGroupId() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertFalse(producer.sentOffsets)

        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(offsets = groupCommit, consumerGroupId = "groupId")

        assertTrue(producer.sentOffsets)
    }

    @Test
    fun shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertFalse(producer.sentOffsets)

        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit,
            groupMetadata = ConsumerGroupMetadata("groupId"),
        )

        assertTrue(producer.sentOffsets)
    }

    @Test
    fun shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        assertFalse(producer.sentOffsets)

        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit,
            groupMetadata = ConsumerGroupMetadata("groupId"),
        )
        producer.commitTransaction() // commit should not reset "sentOffsets" flag

        assertTrue(producer.sentOffsets)

        producer.beginTransaction()

        assertFalse(producer.sentOffsets)

        producer.sendOffsetsToTransaction(
            offsets = groupCommit,
            groupMetadata = ConsumerGroupMetadata("groupId"),
        )
        producer.commitTransaction() // commit should not reset "sentOffsets" flag

        assertTrue(producer.sentOffsets)

        producer.beginTransaction()

        assertFalse(producer.sentOffsets)
    }

    @Test
    fun shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        val group = "g"
        val groupCommit1 = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 73L, leaderEpoch = null),
        )
        val groupCommit2 = mapOf(
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 101L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 2) to OffsetAndMetadata(offset = 21L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit1,
            groupMetadata = ConsumerGroupMetadata(group),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit2,
            groupMetadata = ConsumerGroupMetadata(group),
        )

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty())

        val expectedResult = mapOf(
            group to mapOf(
                TopicPartition(topic, 0) to OffsetAndMetadata(42L, null),
                TopicPartition(topic, 1) to OffsetAndMetadata(101L, null),
                TopicPartition(topic, 2) to OffsetAndMetadata(21L, null),
            )
        )
        producer.commitTransaction()

        assertEquals(listOf(expectedResult), producer.consumerGroupOffsetsHistory())
    }

    @Test
    fun shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        val group = "g"
        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 73L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit,
            groupMetadata = ConsumerGroupMetadata(group),
        )
        producer.abortTransaction()
        producer.beginTransaction()
        producer.commitTransaction()

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty())

        producer.beginTransaction()
        producer.sendOffsetsToTransaction(
            offsets = groupCommit,
            groupMetadata = ConsumerGroupMetadata(group),
        )
        producer.abortTransaction()
        producer.beginTransaction()
        producer.commitTransaction()

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty())
    }

    @Test
    fun shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        val group = "g"
        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 73L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(groupCommit, ConsumerGroupMetadata(group))
        producer.commitTransaction()
        producer.beginTransaction()
        producer.abortTransaction()
        val expectedResult = mapOf(group to groupCommit)

        assertEquals(listOf(expectedResult), producer.consumerGroupOffsetsHistory())
    }

    @Test
    fun shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true)
        producer.initTransactions()
        producer.beginTransaction()

        val group = "g"
        val groupCommit = mapOf(
            TopicPartition(topic = topic, partition = 0) to OffsetAndMetadata(offset = 42L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 1) to OffsetAndMetadata(offset = 73L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(groupCommit, ConsumerGroupMetadata(group))
        producer.commitTransaction()
        producer.beginTransaction()

        val group2 = "g2"
        val groupCommit2 = mapOf(
            TopicPartition(topic = topic, partition = 2) to OffsetAndMetadata(offset = 53L, leaderEpoch = null),
            TopicPartition(topic = topic, partition = 3) to OffsetAndMetadata(offset = 84L, leaderEpoch = null),
        )
        producer.sendOffsetsToTransaction(
            offsets = groupCommit2,
            groupMetadata = ConsumerGroupMetadata(group2),
        )
        producer.abortTransaction()
        val expectedResult = mapOf(group to groupCommit)

        assertEquals(listOf(expectedResult), producer.consumerGroupOffsetsHistory())
    }

    @Test
    fun shouldThrowOnInitTransactionIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.initTransactions() }
    }

    @Test
    fun shouldThrowOnSendIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> {
            producer.send(ProducerRecord(topic = "any", value = "any".toByteArray()))
        }
    }

    @Test
    fun shouldThrowOnBeginTransactionIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.beginTransaction() }
    }

    @Test
    fun shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()
        assertFailsWith<IllegalStateException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId),
            )
        }
    }

    @Test
    fun shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> {
            producer.sendOffsetsToTransaction(
                offsets = emptyMap(),
                groupMetadata = ConsumerGroupMetadata(groupId)
            )
        }
    }

    @Test
    fun shouldThrowOnCommitTransactionIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.commitTransaction() }
    }

    @Test
    fun shouldThrowOnAbortTransactionIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.abortTransaction() }
    }

    @Test
    fun shouldThrowOnFenceProducerIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.fenceProducer() }
    }

    @Test
    fun shouldThrowOnFlushProducerIfProducerIsClosed() {
        buildMockProducer(true)
        producer.close()

        assertFailsWith<IllegalStateException> { producer.flush() }
    }

    @Test
    @Disabled("Kotlin Migration: Classes are propagated and therefore cannot mismatch")
    fun shouldThrowClassCastException() {
//        MockProducer(
//            autoComplete = true,
//            keySerializer = IntegerSerializer(),
//            valueSerializer = StringSerializer(),
//        ).use { customProducer ->
//            assertFailsWith<ClassCastException> {
//                customProducer.send(
//                    ProducerRecord(
//                        topic = topic,
//                        key = "key1",
//                        value = "value1",
//                    )
//                )
//            }
//        }
    }

    @Test
    fun shouldBeFlushedIfNoBufferedRecords() {
        buildMockProducer(true)

        assertTrue(producer.flushed)
    }

    @Test
    fun shouldBeFlushedWithAutoCompleteIfBufferedRecords() {
        buildMockProducer(true)
        producer.send(record1)

        assertTrue(producer.flushed)
    }

    @Test
    fun shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords() {
        buildMockProducer(false)
        producer.send(record1)

        assertFalse(producer.flushed)
    }

    @Test
    fun shouldNotBeFlushedAfterFlush() {
        buildMockProducer(false)
        producer.send(record1)
        producer.flush()

        assertTrue(producer.flushed)
    }

    @Test
    @Throws(InterruptedException::class)
    fun testMetadataOnException() {
        buildMockProducer(false)
        val metadata = producer.send(
            record = record2,
            callback = object : Callback {
                override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                    assertNotNull(metadata)
                    assertEquals(
                        expected = -1L,
                        actual = metadata.offset,
                        message = "Invalid offset",
                    )
                    assertEquals(
                        expected = RecordBatch.NO_TIMESTAMP,
                        actual = metadata.timestamp,
                        message = "Invalid timestamp",
                    )
                    assertEquals(
                        expected = -1L,
                        actual = metadata.serializedKeySize.toLong(),
                        message = "Invalid Serialized Key size",
                    )
                    assertEquals(
                        expected = -1L,
                        actual = metadata.serializedValueSize.toLong(),
                        message = "Invalid Serialized value size",
                    )
                }
            },
        )
        val e = IllegalArgumentException("dummy exception")

        assertTrue(producer.errorNext(e), "Complete the second request with an error")

        try {
            metadata.get()
            fail("Something went wrong, expected an error")
        } catch (err: ExecutionException) {
            assertEquals(e, err.cause)
        }
    }

    private fun isError(future: Future<*>): Boolean {
        return try {
            future.get()
            false
        } catch (e: Exception) {
            true
        }
    }
}
