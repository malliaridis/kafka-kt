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

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of [AdminClient.alterPartitionReassignments].
 *
 * The API of this class is evolving. See [AdminClient] for details.
 */
@Evolving
class AlterPartitionReassignmentsResult internal constructor(
    val futures: Map<TopicPartition, KafkaFuture<Unit>>,
) {
    /**
     * Return a map from partitions to futures which can be used to check the status of the reassignment.
     *
     * Possible error codes:
     *
     * - INVALID_REPLICA_ASSIGNMENT (39) -  if the specified replica assignment was not valid -- for
     * example, if it included negative numbers, repeated numbers, or specified a broker ID that the
     * controller was not aware of.
     * - NO_REASSIGNMENT_IN_PROGRESS (85) - if the request wants to cancel reassignments but none exist
     * - UNKNOWN (-1)
     *
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("futures")
    )
    fun values(): Map<TopicPartition, KafkaFuture<Unit>> {
        return futures
    }

    /**
     * Return a future which succeeds only if all the reassignments were successfully initiated.
     */
    fun all(): KafkaFuture<Unit> {
        return KafkaFuture.allOf(*futures.values.toTypedArray())
    }
}