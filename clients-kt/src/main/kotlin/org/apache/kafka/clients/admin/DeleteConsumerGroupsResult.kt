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
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [Admin.deleteConsumerGroups] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DeleteConsumerGroupsResult internal constructor(
    private val futures: Map<String, KafkaFuture<Unit>>,
) {

    /**
     * Return a map from group id to futures which can be used to check the status of
     * individual deletions.
     */
    fun deletedGroups(): Map<String, KafkaFuture<Unit>> {
        val deletedGroups: MutableMap<String, KafkaFuture<Unit>> = HashMap(
            futures.size
        )
        futures.forEach { (key: String, future: KafkaFuture<Unit>) ->
            deletedGroups[key] = future
        }
        return deletedGroups
    }

    /**
     * Return a future which succeeds only if all the consumer group deletions succeed.
     */
    fun all(): KafkaFuture<Unit> {
        return KafkaFuture.allOf(*futures.values.toTypedArray())
    }
}
