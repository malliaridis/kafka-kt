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

import org.apache.kafka.common.TopicPartition

/**
 * Returns a replica whose rack id is equal to the rack id specified in the client request metadata. If no such replica
 * is found, returns the leader.
 */
class RackAwareReplicaSelector : ReplicaSelector {

    override fun select(
        topicPartition: TopicPartition,
        clientMetadata: ClientMetadata,
        partitionView: PartitionView
    ): ReplicaView {
        return if (clientMetadata.rackId().isNotEmpty()) {
            val sameRackReplicas = partitionView.replicas()
                .filter { replicaInfo -> clientMetadata.rackId() == replicaInfo.endpoint().rack() }
                .toSet()

            if (sameRackReplicas.isEmpty()) partitionView.leader()
            // Use the leader if it's in this rack
            else if (sameRackReplicas.contains(partitionView.leader())) partitionView.leader()
            // Otherwise, get the most caught-up replica
            else sameRackReplicas.maxWith(ReplicaView.comparator())
        } else partitionView.leader()
    }
}
