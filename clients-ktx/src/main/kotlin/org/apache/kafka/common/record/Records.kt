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

import org.apache.kafka.common.utils.AbstractIterator
import org.apache.kafka.common.utils.Time

/**
 * Interface for accessing the records contained in a log. The log itself is represented as a
 * sequence of record batches (see [RecordBatch]).
 *
 * For magic versions 1 and below, each batch consists of an 8 byte offset, a 4 byte record size,
 * and a "shallow" [record][Record]. If the batch is not compressed, then each batch will have only
 * the shallow record contained inside it. If it is compressed, the batch contains "deep" records,
 * which are packed into the value field of the shallow record. To iterate over the shallow batches,
 * use [Records.batches]; for the deep records, use [Records.records]. Note that the deep iterator
 * handles both compressed and non-compressed batches:
 * if the batch is not compressed, the shallow record is returned; otherwise, the shallow batch is
 * decompressed and the deep records are returned.
 *
 * For magic version 2, every batch contains 1 or more log record, regardless of compression. You
 * can iterate over the batches directly using [Records.batches]. Records can be iterated either
 * directly from an individual batch or through [Records.records]. Just as in previous versions,
 * iterating over the records typically involves decompression and should therefore be used with
 * caution.
 *
 * See [MemoryRecords] for the in-memory representation and [FileRecords] for the on-disk
 * representation.
 */
interface Records : TransferableRecords {

    /**
     * Get the record batches. Note that the signature allows subclasses
     * to return a more specific batch type. This enables optimizations such as in-place offset
     * assignment (see for example [DefaultRecordBatch]), and partial reading of
     * record data (see [FileLogInputStream.FileChannelRecordBatch.magic].
     *
     * @return An iterator over the record batches of the log
     */
    fun batches(): Iterable<RecordBatch>

    /**
     * Get an iterator over the record batches. This is similar to [batches] but returns an
     * [AbstractIterator] instead of [Iterator], so that clients can use methods like
     * [peek][AbstractIterator.peek].
     *
     * @return An iterator over the record batches of the log
     */
    fun batchIterator(): AbstractIterator<out RecordBatch>

    /**
     * Check whether all batches in this buffer have a certain magic value.
     *
     * @param magic The magic value to check
     * @return `true` if all record batches have a matching magic value, `false` otherwise
     */
    fun hasMatchingMagic(magic: Byte): Boolean

    /**
     * Convert all batches in this buffer to the format passed as a parameter. Note that this
     * requires deep iteration since all of the deep records must also be converted to the desired
     * format.
     *
     * @param toMagic The magic value to convert to
     * @param firstOffset The starting offset for returned records. This only impacts some cases.
     * See [RecordsUtil.downConvert] for an explanation.
     * @param time instance used for reporting stats
     * @return A [ConvertedRecords] instance which may or may not contain the same instance in its
     * records field.
     */
    fun downConvert(toMagic: Byte, firstOffset: Long, time: Time?): ConvertedRecords<out Records>

    /**
     * Get an iterator over the records in this log. Note that this generally requires
     * decompression, and should therefore be used with care.
     *
     * @return The record iterator
     */
    fun records(): Iterable<Record>

    companion object {
        const val OFFSET_OFFSET = 0
        const val OFFSET_LENGTH = 8
        const val SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH
        const val SIZE_LENGTH = 4
        const val LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH

        // the magic offset is at the same offset for all current message formats, but the 4 bytes
        // between the size and the magic is dependent on the version.
        const val MAGIC_OFFSET = 16
        const val MAGIC_LENGTH = 1
        const val HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH
    }
}
