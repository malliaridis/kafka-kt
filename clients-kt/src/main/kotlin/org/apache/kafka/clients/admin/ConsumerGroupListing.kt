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

import org.apache.kafka.common.ConsumerGroupState

/**
 * A listing of a consumer group in the cluster.
 *
 * @property groupId Group ID
 * @property isSimpleConsumerGroup If Consumer Group is simple or not.
 * @property state The state of the consumer group
 */
data class ConsumerGroupListing(
    val groupId: String,
    val isSimpleConsumerGroup: Boolean,
    val state: ConsumerGroupState? = null
) {

    override fun toString(): String {
        return "(" +
                "groupId='$groupId'" +
                ", isSimpleConsumerGroup=$isSimpleConsumerGroup" +
                ", state=$state" +
                ')'
    }
}
