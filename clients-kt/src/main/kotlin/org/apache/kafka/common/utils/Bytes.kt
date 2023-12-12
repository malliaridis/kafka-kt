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

import java.io.Serializable

/**
 * Utility class that handles immutable byte arrays.
 *
 * @constructor Create a Bytes using the byte array.
 * @property bytes This array becomes the backing storage for the object.
 */
class Bytes(private val bytes: ByteArray) : Comparable<Bytes> {

    // cache the hash code for the string, default to 0
    private var hashCode = 0

    /**
     * Get the data from the Bytes.
     *
     * @return The underlying byte array
     */
    fun get(): ByteArray = bytes

    /**
     * The hashcode is cached except for the case where it is computed as 0, in which case we
     * compute the hashcode on every call.
     *
     * @return the hashcode
     */
    override fun hashCode(): Int {
        if (hashCode == 0) {
            hashCode = bytes.contentHashCode()
        }
        return hashCode
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false

        // we intentionally use the function to compute hashcode here
        if (this.hashCode() != other.hashCode()) return false
        return if (other is Bytes) bytes.contentEquals(other.get()) else false
    }

    override operator fun compareTo(other: Bytes): Int {
        return BYTES_LEXICO_COMPARATOR.compare(bytes, other.bytes)
    }

    override fun toString(): String = toString(bytes, 0, bytes.size)

    interface ByteArrayComparator : Comparator<ByteArray>, Serializable {

        fun compare(
            buffer1: ByteArray,
            offset1: Int,
            length1: Int,
            buffer2: ByteArray,
            offset2: Int,
            length2: Int,
        ): Int
    }

    private class LexicographicByteArrayComparator : ByteArrayComparator {

        override fun compare(buffer1: ByteArray, buffer2: ByteArray): Int =
            compare(buffer1, 0, buffer1.size, buffer2, 0, buffer2.size)

        override fun compare(
            buffer1: ByteArray,
            offset1: Int,
            length1: Int,
            buffer2: ByteArray,
            offset2: Int,
            length2: Int,
        ): Int {
            // short circuit equal case
            if (buffer1.contentEquals(buffer2) && offset1 == offset2 && length1 == length2) return 0

            // similar to Arrays.compare() but considers offset and length
            val end1 = offset1 + length1
            val end2 = offset2 + length2
            var i = offset1
            var j = offset2
            while (i < end1 && j < end2) {
                val a = buffer1[i].toInt() and 0xff
                val b = buffer2[j].toInt() and 0xff
                if (a != b) return a - b
                i++
                j++
            }
            return length1 - length2
        }
    }

    companion object {

        val EMPTY = ByteArray(0)

        private val HEX_CHARS_UPPER = charArrayOf(
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
            '9',
            'A',
            'B',
            'C',
            'D',
            'E',
            'F'
        )

        /**
         * A byte array comparator based on lexicographic ordering.
         */
        val BYTES_LEXICO_COMPARATOR: ByteArrayComparator = LexicographicByteArrayComparator()

        fun wrap(bytes: ByteArray?): Bytes? = bytes?.let { Bytes(it) }

        /**
         * Write a printable representation of a byte array. Non-printable characters are hex
         * escaped in the format \\x%02X, eg: \x00 \x05 etc.
         *
         * This function is brought from org.apache.hadoop.hbase.util.Bytes.
         *
         * @param bytes array to write out
         * @param offset offset to start at
         * @param length length to write
         * @return string output
         */
        private fun toString(bytes: ByteArray?, offset: Int, length: Int): String {
            var len = length
            val result = StringBuilder()
            if (bytes == null) return result.toString()

            // just in case we are passed a 'len' that is > buffer length...
            if (offset >= bytes.size) return result.toString()
            if (offset + len > bytes.size) len = bytes.size - offset
            for (i in offset until offset + len) {
                val ch = bytes[i].toInt() and 0xFF
                if (ch >= ' '.code && ch <= '~'.code && ch != '\\'.code) {
                    result.append(ch.toChar())
                } else {
                    result.append("\\x")
                    result.append(HEX_CHARS_UPPER[ch / 0x10])
                    result.append(HEX_CHARS_UPPER[ch % 0x10])
                }
            }
            return result.toString()
        }

        /**
         * Increment the underlying byte array by adding 1. Throws an IndexOutOfBoundsException if
         * incrementing would cause the underlying input byte array to overflow.
         *
         * @param input - The byte array to increment
         * @return A new copy of the incremented byte array.
         */
        @Throws(IndexOutOfBoundsException::class)
        fun increment(input: Bytes): Bytes? {
            val inputArr = input.get()
            val ret = ByteArray(inputArr.size)
            var carry = 1
            for (i in inputArr.indices.reversed()) {
                if (inputArr[i] == 0xFF.toByte() && carry == 1) {
                    ret[i] = 0x00.toByte()
                } else {
                    ret[i] = (inputArr[i] + carry).toByte()
                    carry = 0
                }
            }

            return if (carry == 0) wrap(ret)
            else throw IndexOutOfBoundsException()
        }
    }
}
