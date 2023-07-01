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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Describes new partitions for a particular topic in a call to [Admin.createPartitions].
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property totalCount The total number of partitions after the operation succeeds.
 * @property newAssignments The replica assignments for the new partitions, or `null` if the
 * assignment will be done by the controller.
 */
@Evolving
class NewPartitions private constructor(
    val totalCount: Int,
    val newAssignments: List<List<Int>>? = null,
) {

    /**
     * The total number of partitions after the operation succeeds.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("totalCount"),
    )
    fun totalCount(): Int = totalCount

    /**
     * The replica assignments for the new partitions, or `null` if the assignment will be done by
     * the controller.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("newAssignments"),
    )
    fun assignments(): List<List<Int>>? = newAssignments

    override fun toString(): String = "(totalCount=$totalCount, newAssignments=$newAssignments)"

    companion object {

        /**
         * Increase the partition count for a topic to the given `totalCount`. The assignment of new
         * replicas to brokers will be decided by the broker.
         *
         * @param totalCount The total number of partitions after the operation succeeds.
         */
        fun increaseTo(totalCount: Int): NewPartitions = NewPartitions(totalCount)

        /**
         * Increase the partition count for a topic to the given `totalCount` assigning the new
         * partitions according to the given `newAssignments`. The length of the given
         * `newAssignments` should equal `totalCount - oldCount`, since the assignment of existing
         * partitions are not changed. Each inner list of `newAssignments` should have a length
         * equal to the topic's replication factor.
         *
         * The first broker id in each inner list is the "preferred replica".
         *
         * For example, suppose a topic currently has a replication factor of 2, and has 3
         * partitions. The number of partitions can be increased to 6 using a `NewPartition`
         * constructed like this:
         *
         * ```java
         * NewPartitions.increaseTo(6, asList(asList(1, 2),
         * asList(2, 3),
         * asList(3, 1)))
         * ```
         *
         * In this example partition 3's preferred leader will be broker 1, partition 4's preferred
         * leader will be broker 2 and partition 5's preferred leader will be broker 3.
         *
         * @param totalCount The total number of partitions after the operation succeeds.
         * @param newAssignments The replica assignments for the new partitions.
         */
        fun increaseTo(totalCount: Int, newAssignments: List<List<Int>>?): NewPartitions =
            NewPartitions(totalCount, newAssignments)
    }
}
