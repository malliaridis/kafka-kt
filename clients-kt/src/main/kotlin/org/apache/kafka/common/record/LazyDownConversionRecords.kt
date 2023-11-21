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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.Time
import java.util.*
import kotlin.math.max

/**
 * Encapsulation for holding records that require down-conversion in a lazy, chunked manner
 * (KIP-283). See [LazyDownConversionRecordsSend] for the actual chunked send implementation.
 *
 * @property topicPartition The topic-partition to which records belong
 * @property records Records to lazily down-convert
 * @property toMagic Magic version to down-convert to
 * @property firstOffset The starting offset for down-converted records. This only impacts some
 * cases. See [RecordsUtil.downConvert] for an explanation.
 * @property time The time instance to use
 *
 * @throws org.apache.kafka.common.errors.UnsupportedCompressionTypeException If the first batch to
 * down-convert has a compression type which we do not support down-conversion for.
 */
class LazyDownConversionRecords(
    val topicPartition: TopicPartition,
    private val records: Records,
    private val toMagic: Byte,
    private val firstOffset: Long,
    private val time: Time,
) : BaseRecords {

    private var firstConvertedBatch: ConvertedRecords<*>? = null

    private var sizeInBytes = 0

    init {
        // To make progress, kafka consumers require at least one full record batch per partition,
        // i.e. we need to ensure we can accommodate one full batch of down-converted messages. We
        // achieve this by having `sizeInBytes` factor in the size of the first down-converted batch
        // and we return at least that many bytes.
        val it = iterator(0)
        if (it.hasNext()) {
            firstConvertedBatch = it.next()
            sizeInBytes = max(records.sizeInBytes(), firstConvertedBatch!!.records.sizeInBytes())
        } else {
            // If there are messages before down-conversion and no messages after down-conversion,
            // make sure we are able to send at least an overflow message to the consumer so that it
            // can throw a RecordTooLargeException. Typically, the consumer would need to increase
            // the fetch size in such cases. If there are no messages before down-conversion, we
            // return an empty record batch.
            firstConvertedBatch = null
            sizeInBytes = if (records.batches().iterator().hasNext())
                LazyDownConversionRecordsSend.MIN_OVERFLOW_MESSAGE_LENGTH
            else 0
        }
    }

    override fun sizeInBytes(): Int = sizeInBytes

    override fun toSend(): LazyDownConversionRecordsSend =
        LazyDownConversionRecordsSend(this)

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("topicPartition"),
    )
    fun topicPartition(): TopicPartition = topicPartition

    override fun equals(other: Any?): Boolean {
        if (other is LazyDownConversionRecords) {
            return toMagic == other.toMagic
                    && firstOffset == other.firstOffset
                    && topicPartition == other.topicPartition
                    && records == other.records
        }
        return false
    }

    override fun hashCode(): Int {
        var result = toMagic.toInt()
        result = 31 * result + java.lang.Long.hashCode(firstOffset)
        result = 31 * result + topicPartition.hashCode()
        result = 31 * result + records.hashCode()
        return result
    }

    override fun toString(): String {
        return "LazyDownConversionRecords(size=$sizeInBytes" +
                ", underlying=$records" +
                ", toMagic=$toMagic" +
                ", firstOffset=$firstOffset" +
                ")"
    }

    fun iterator(maximumReadSize: Long): kotlin.collections.Iterator<ConvertedRecords<*>> {
        // We typically expect only one iterator instance to be created, so null out the first
        // converted batch after first use to make it available for GC.
        val firstBatch = firstConvertedBatch
        firstConvertedBatch = null
        return Iterator(records, maximumReadSize, firstBatch)
    }

    /**
     * Implementation for being able to iterate over down-converted records. Goal of this
     * implementation is to keep it as memory-efficient as possible by not having to maintain all
     * down-converted records in-memory. Maintains a view into batches of down-converted records.
     *
     * @param recordsToDownConvert Records that require down-conversion
     * @property maximumReadSize Maximum possible size of underlying records that will be
     * down-converted in each call to [makeNext]. This is a soft limit as [makeNext] will always
     * convert and return at least one full message batch.
     */
    private inner class Iterator(
        recordsToDownConvert: Records,
        private val maximumReadSize: Long,
        private var firstConvertedBatch: ConvertedRecords<*>?
    ) : AbstractIterator<ConvertedRecords<*>>() {

        private val batchIterator: AbstractIterator<out RecordBatch>

        init {
            batchIterator = recordsToDownConvert.batchIterator()
            // If we already have the first down-converted batch, advance the underlying records
            // iterator to next batch
            if (firstConvertedBatch != null) batchIterator.next()
        }

        /**
         * Make next set of down-converted records
         * @return Down-converted records
         */
        override fun makeNext(): ConvertedRecords<*>? {
            // If we have cached the first down-converted batch, return that now
            if (firstConvertedBatch != null) {
                val convertedBatch: ConvertedRecords<*> = firstConvertedBatch!!
                firstConvertedBatch = null
                return convertedBatch
            }

            while (batchIterator.hasNext()) {
                val batches: MutableList<RecordBatch> = ArrayList()
                var isFirstBatch = true
                var sizeSoFar: Long = 0

                // Figure out batches we should down-convert based on the size constraints
                while (
                    batchIterator.hasNext()
                    && (isFirstBatch
                            || batchIterator.peek().sizeInBytes() + sizeSoFar <= maximumReadSize)
                ) {
                    val currentBatch = batchIterator.next()
                    batches.add(currentBatch)
                    sizeSoFar += currentBatch.sizeInBytes().toLong()
                    isFirstBatch = false
                }
                val convertedRecords: ConvertedRecords<*> = RecordsUtil.downConvert(
                    batches,
                    toMagic,
                    firstOffset, time
                )
                // During conversion, it is possible that we drop certain batches because they do
                // not have an equivalent representation in the message format we want to convert
                // to. For example, V0 and V1 message formats have no notion of transaction markers
                // which were introduced in V2, so they get dropped during conversion. We return
                // converted records only when we have at least one valid batch of messages after
                // conversion.
                if (convertedRecords.records.sizeInBytes() > 0) return convertedRecords
            }
            return allDone()
        }
    }
}
