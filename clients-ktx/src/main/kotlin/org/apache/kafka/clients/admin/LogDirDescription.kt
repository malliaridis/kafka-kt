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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.requests.DescribeLogDirsResponse

/**
 * A description of a log directory on a particular broker.
 */
class LogDirDescription(
    private val error: ApiException,
    private val replicaInfos: Map<TopicPartition, ReplicaInfo>,
    totalBytes: Long = DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES,
    usableBytes: Long = DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES,
) {
    val totalBytes: Long?
    val usableBytes: Long?

    init {
        this.totalBytes =
            if (totalBytes == DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES) null
            else totalBytes

        this.usableBytes =
            if (usableBytes == DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES) null
            else usableBytes
    }

    /**
     * Returns `ApiException` if the log directory is offline or an error occurred, otherwise
     * returns `null`.
     *
     * - `KafkaStorageException` - The log directory is offline.
     * - `UnknownServerException` - The server experienced an unexpected error when processing the
     * request.
     */
    fun error(): ApiException = error

    /**
     * A map from topic partition to replica information for that partition in this log directory.
     */
    fun replicaInfos(): Map<TopicPartition, ReplicaInfo> = replicaInfos

    /**
     * The total size of the volume this log directory is on or empty if the broker did not return a
     * value.
     * For volumes larger than [Long.MAX_VALUE], [Long.MAX_VALUE] is returned.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("totalBytes"),
    )
    fun totalBytes(): Long? = totalBytes

    /**
     * The usable size on the volume this log directory is on or empty if the broker did not return
     * a value. For usable sizes larger than [Long.MAX_VALUE], [Long.MAX_VALUE] is returned.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("usableBytes"),
    )
    fun usableBytes(): Long? = usableBytes

    override fun toString(): String = "LogDirDescription(" +
            "replicaInfos=$replicaInfos" +
            ", error=$error" +
            ", totalBytes=$totalBytes" +
            ", usableBytes=$usableBytes" +
            ')'
}
