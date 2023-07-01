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
import org.apache.kafka.common.header.Header

/**
 * A log record is a tuple consisting of a unique offset in the log, a sequence number assigned by
 * the producer, a timestamp, a key and a value.
 */
interface Record {

    /**
     * The offset of this record in the log
     * @return the offset
     */
    fun offset(): Long

    /**
     * Get the sequence number assigned by the producer.
     * @return the sequence number
     */
    fun sequence(): Int

    /**
     * Get the size in bytes of this record.
     *
     * @return the size of the record in bytes
     */
    fun sizeInBytes(): Int

    /**
     * Get the record's timestamp.
     *
     * @return the record's timestamp
     */
    fun timestamp(): Long

    /**
     * Raise a [org.apache.kafka.common.errors.CorruptRecordException] if the record does not have
     * a valid checksum.
     */
    fun ensureValid()

    /**
     * Get the size in bytes of the key.
     * @return the size of the key, or `-1` if there is no key
     */
    fun keySize(): Int

    /**
     * Check whether this record has a key
     * @return `true` if there is a key, `false` otherwise
     */
    fun hasKey(): Boolean

    /**
     * Get the record's key.
     * @return the key or `null` if there is none
     */
    fun key(): ByteBuffer?

    /**
     * Get the size in bytes of the value.
     *
     * @return the size of the value, or `-1` if the value is `null`
     */
    fun valueSize(): Int

    /**
     * Check whether a value is present (i.e. if the value is not null)
     * @return `true` if so, `false` otherwise
     */
    fun hasValue(): Boolean

    /**
     * Get the record's value
     * @return the (nullable) value
     */
    fun value(): ByteBuffer?

    /**
     * Check whether the record has a particular magic. For versions prior to 2, the record contains its own magic,
     * so this function can be used to check whether it matches a particular value. For version 2 and above, this
     * method returns true if the passed magic is greater than or equal to 2.
     *
     * @param magic the magic value to check
     * @return true if the record has a magic field (versions prior to 2) and the value matches
     */
    fun hasMagic(magic: Byte): Boolean

    /**
     * For versions prior to 2, check whether the record is compressed (and therefore
     * has nested record content). For versions 2 and above, this always returns false.
     *
     * @return true if the magic is lower than 2 and the record is compressed
     */
    val isCompressed: Boolean

    /**
     * For versions prior to 2, the record contained a timestamp type attribute. This method can be
     * used to check whether the value of that attribute matches a particular timestamp type. For versions
     * 2 and above, this will always be false.
     *
     * @param timestampType the timestamp type to compare
     * @return true if the version is lower than 2 and the timestamp type matches
     */
    fun hasTimestampType(timestampType: TimestampType): Boolean

    /**
     * Get the headers. For magic versions 1 and below, this always returns an empty array.
     *
     * @return the array of headers
     */
    fun headers(): Array<Header>

    companion object {
        val EMPTY_HEADERS = emptyArray<Header>()
    }
}
