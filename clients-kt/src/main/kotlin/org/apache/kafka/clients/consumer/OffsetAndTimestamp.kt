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

package org.apache.kafka.clients.consumer

/**
 * A container class for offset and timestamp.
 */
data class OffsetAndTimestamp(
    val offset: Long,
    val timestamp: Long,
    val leaderEpoch: Int? = null,
) {

    init {
        require(offset >= 0) { "Invalid negative offset" }
        require(timestamp >= 0) { "Invalid negative timestamp" }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("timestamp"),
    )
    fun timestamp(): Long = timestamp

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("offset"),
    )
    fun offset(): Long = offset

    /**
     * Get the leader epoch corresponding to the offset that was found (if one exists). This can be
     * provided to seek() to ensure that the log hasn't been truncated prior to fetching.
     *
     * @return The leader epoch or empty if it is not known
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("leaderEpoch"),
    )
    fun leaderEpoch(): Int? = leaderEpoch

    override fun toString(): String =
        "(timestamp=$timestamp, leaderEpoch=$leaderEpoch, offset=$offset)"
}
