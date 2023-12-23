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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.kafka.common.utils.Time

/**
 * A PayloadGenerator which generates a timestamped constant payload.
 *
 * The timestamp used for this class is in milliseconds since epoch, encoded directly to the first several bytes
 * of the payload.
 *
 * This should be used in conjunction with TimestampRecordProcessor in the Consumer to measure true end-to-end latency
 * of a system.
 *
 * `size` - The size in bytes of each message.
 *
 * Here is an example spec:
 *
 * ```json
 * {
 *     "type": "timestampConstant",
 *     "size": 512
 * }
 * ```
 *
 * This will generate a 512-byte message with the first several bytes encoded with the timestamp.
 */
class TimestampConstantPayloadGenerator @JsonCreator constructor(
    @param:JsonProperty("size") private val size: Int,
) : PayloadGenerator {

    private val buffer: ByteBuffer

    init {
        if (size < Long.SIZE_BYTES) throw RuntimeException(
            "The size of the payload must be greater than or equal to ${Long.SIZE_BYTES}."
        )

        buffer = ByteBuffer.allocate(Long.SIZE_BYTES)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
    }

    @JsonProperty
    fun size(): Int = size

    @Synchronized
    override fun generate(position: Long): ByteArray {
        // Generate the byte array before the timestamp generation.
        val result = ByteArray(size)

        // Do the timestamp generation as the very last task.
        buffer.clear()
        buffer.putLong(Time.SYSTEM.milliseconds())
        buffer.rewind()
        System.arraycopy(buffer.array(), 0, result, 0, Long.SIZE_BYTES)
        return result
    }
}
