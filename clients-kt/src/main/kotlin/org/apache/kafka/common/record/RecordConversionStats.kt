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

data class RecordConversionStats(
    var temporaryMemoryBytes: Long = 0,
    var numRecordsConverted: Int = 0,
    var conversionTimeNanos: Long = 0
) {

    /**
     * Creates a copy of this [RecordConversionStats] with the [stats]' data added.
     */
    operator fun plus(stats: RecordConversionStats) = copy(
        temporaryMemoryBytes = this.temporaryMemoryBytes + stats.temporaryMemoryBytes,
        numRecordsConverted = this.numRecordsConverted + stats.numRecordsConverted,
        conversionTimeNanos = this.conversionTimeNanos + stats.conversionTimeNanos,
    )

    /**
     * Adds the [stats] data to this object.
     */
    fun add(stats: RecordConversionStats) {
        temporaryMemoryBytes += stats.temporaryMemoryBytes
        numRecordsConverted += stats.numRecordsConverted
        conversionTimeNanos += stats.conversionTimeNanos
    }

    /**
     * Returns the number of temporary memory bytes allocated to process the records.
     * This size depends on whether the records need decompression and/or conversion:
     *
     * - Non compressed, no conversion: zero
     * - Non compressed, with conversion: size of the converted buffer
     * - Compressed, no conversion: size of the original buffer after decompression
     * - Compressed, with conversion: size of the original buffer after decompression + size of the
     *   converted buffer uncompressed
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("temporaryMemoryBytes"),
    )
    fun temporaryMemoryBytes(): Long = temporaryMemoryBytes

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("numRecordsConverted"),
    )
    fun numRecordsConverted(): Int = numRecordsConverted

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("conversionTimeNanos"),
    )
    fun conversionTimeNanos(): Long = conversionTimeNanos

    override fun toString(): String = "RecordConversionStats(" +
            "temporaryMemoryBytes=$temporaryMemoryBytes" +
            ", numRecordsConverted=$numRecordsConverted" +
            ", conversionTimeNanos=$conversionTimeNanos)"

    companion object {
        val EMPTY = RecordConversionStats()
    }
}
