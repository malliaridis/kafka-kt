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

package org.apache.kafka.common.replica

import org.apache.kafka.common.Node
import kotlin.Comparator

/**
 * View of a replica used by [ReplicaSelector] to determine a preferred replica.
 */
interface ReplicaView {

    /**
     * The endpoint information for this replica (hostname, port, rack, etc)
     */
    fun endpoint(): Node

    /**
     * The log end offset for this replica
     */
    fun logEndOffset(): Long

    /**
     * The number of milliseconds (if any) since the last time this replica was caught up to the
     * high watermark. For a leader replica, this is always zero.
     */
    fun timeSinceLastCaughtUpMs(): Long

    class DefaultReplicaView(
        private val endpoint: Node,
        private val logEndOffset: Long,
        private val timeSinceLastCaughtUpMs: Long,
    ) : ReplicaView {

        override fun endpoint(): Node = endpoint

        override fun logEndOffset(): Long = logEndOffset

        override fun timeSinceLastCaughtUpMs(): Long = timeSinceLastCaughtUpMs

        override fun toString(): String {
            return "DefaultReplicaView{" +
                    "endpoint=$endpoint" +
                    ", logEndOffset=$logEndOffset" +
                    ", timeSinceLastCaughtUpMs=$timeSinceLastCaughtUpMs" +
                    '}'
        }
    }

    companion object {

        /**
         * Comparator for ReplicaView that returns in the order of "most caught up". This is used
         * for deterministic selection of a replica when there is a tie from a selector.
         */
        fun comparator(): Comparator<ReplicaView> {
            return Comparator.comparing(ReplicaView::logEndOffset)
                .thenComparing(
                    Comparator.comparingLong(ReplicaView::timeSinceLastCaughtUpMs).reversed()
                )
                .thenComparing { replicaInfo -> replicaInfo.endpoint().id }
        }
    }
}
