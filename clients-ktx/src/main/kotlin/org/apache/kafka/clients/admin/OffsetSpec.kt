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

package org.apache.kafka.clients.admin

/**
 * This class allows to specify the desired offsets when using [KafkaAdminClient.listOffsets]
 */
sealed class OffsetSpec {

    object EarliestSpec : OffsetSpec()

    object LatestSpec : OffsetSpec()

    object MaxTimestampSpec : OffsetSpec()

    data class TimestampSpec internal constructor(val timestamp: Long) : OffsetSpec() {
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("timestamp"),
        )
        fun timestamp(): Long = timestamp
    }

    companion object {

        /**
         * Used to retrieve the latest offset of a partition
         */
        fun latest(): OffsetSpec = LatestSpec

        /**
         * Used to retrieve the earliest offset of a partition
         */
        fun earliest(): OffsetSpec = EarliestSpec

        /**
         * Used to retrieve the earliest offset whose timestamp is greater than
         * or equal to the given timestamp in the corresponding partition
         * @param timestamp in milliseconds
         */
        fun forTimestamp(timestamp: Long): OffsetSpec = TimestampSpec(timestamp)

        /**
         * Used to retrieve the offset with the largest timestamp of a partition
         * as message timestamps can be specified client side this may not match
         * the log end offset returned by LatestSpec
         */
        fun maxTimestamp(): OffsetSpec = MaxTimestampSpec
    }
}
