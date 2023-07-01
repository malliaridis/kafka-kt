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

package org.apache.kafka.common.protocol

/**
 * Helper class which facilitates zero-copy network transmission. See [SendBuilder].
 */
class MessageSizeAccumulator {

    /**
     * The total size of the message.
     */
    var totalSize = 0
        private set

    /**
     * The total "zero-copy" size of the message. This is the summed total of all fields which have
     * either have a type of 'bytes' with 'zeroCopy' enabled, or a type of 'records'.
     */
    var zeroCopySize = 0
        private set

    /**
     * Get the total size of the message.
     *
     * @return total size in bytes
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("totalSize")
    )
    fun totalSize(): Int = totalSize

    /**
     * Size excluding zero copy fields as specified by [.zeroCopySize]. This is typically the size
     * of the byte buffer used to serialize messages.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("sizeExcludingZeroCopy")
    )
    fun sizeExcludingZeroCopy(): Int {
        return totalSize - zeroCopySize
    }

    /**
     * Size excluding zero copy fields as specified by [.zeroCopySize]. This is typically the size
     * of the byte buffer used to serialize messages.
     */
    val sizeExcludingZeroCopy: Int
        get() = totalSize - zeroCopySize

    /**
     * Get the total "zero-copy" size of the message. This is the summed total of all fields which
     * have either have a type of 'bytes' with 'zeroCopy' enabled, or a type of 'records'
     *
     * @return total size of zero-copy data in the message
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("zeroCopySize")
    )
    fun zeroCopySize(): Int = zeroCopySize

    fun addZeroCopyBytes(size: Int) {
        zeroCopySize += size
        totalSize += size
    }

    fun addBytes(size: Int) {
        totalSize += size
    }

    fun add(size: MessageSizeAccumulator) {
        totalSize += size.totalSize
        zeroCopySize += size.zeroCopySize
    }
}
