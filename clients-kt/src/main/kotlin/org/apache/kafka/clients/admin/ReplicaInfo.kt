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
 * A description of a replica on a particular broker.
 *
 * @property isFuture Whether this replica has been created by a AlterReplicaLogDirsRequest
 * but not yet replaced the current replica on the broker.
 *
 * @return true if this log is created by AlterReplicaLogDirsRequest and will replace the current
 * log of the replica at some time in the future.
 */

data class ReplicaInfo(
    val size: Long,
    val offsetLag: Long,
    val isFuture: Boolean,
) {

    /**
     * The total size of the log segments in this replica in bytes.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("size"),
    )
    fun size(): Long = size

    /**
     * The lag of the log's LEO with respect to the partition's
     * high watermark (if it is the current log for the partition)
     * or the current replica's LEO (if it is the [future log][.isFuture]
     * for the partition).
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("offsetLag"),
    )
    fun offsetLag(): Long = offsetLag

    override fun toString(): String {
        return "ReplicaInfo(" +
                "size=$size" +
                ", offsetLag=$offsetLag" +
                ", isFuture=$isFuture" +
                ')'
    }
}
