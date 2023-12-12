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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol
import org.apache.kafka.common.TopicPartition

open class MockPartitionAssignor internal constructor(
    private val supportedProtocols: List<RebalanceProtocol>,
) : AbstractPartitionAssignor() {

    private var numAssignment = 0

    private var result: MutableMap<String, MutableList<TopicPartition>>? = null

    override fun assign(
        partitionsPerTopic: Map<String, Int>,
        subscriptions: Map<String, ConsumerPartitionAssignor.Subscription>,
    ): MutableMap<String, MutableList<TopicPartition>> {
        return checkNotNull(result) { "Call to assign with no result prepared" }
    }

    override fun name(): String = "consumer-mock-assignor"

    override fun supportedProtocols(): List<RebalanceProtocol> = supportedProtocols

    fun clear() {
        result = null
    }

    fun prepare(result: MutableMap<String, MutableList<TopicPartition>>?) {
        this.result = result
    }

    override fun onAssignment(assignment: ConsumerPartitionAssignor.Assignment?, metadata: ConsumerGroupMetadata?) {
        numAssignment += 1
    }

    fun numAssignment(): Int = numAssignment
}
