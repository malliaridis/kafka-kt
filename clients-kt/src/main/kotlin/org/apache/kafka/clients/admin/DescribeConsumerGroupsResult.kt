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

import java.util.concurrent.ExecutionException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [KafkaAdminClient.describeConsumerGroups]} call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeConsumerGroupsResult(
    private val futures: Map<String, KafkaFuture<ConsumerGroupDescription>>,
) {

    /**
     * Return a map from group id to futures which yield group descriptions.
     */
    fun describedGroups(): Map<String, KafkaFuture<ConsumerGroupDescription>> {
        return futures.toMap()
    }

    /**
     * Return a future which yields all ConsumerGroupDescription objects, if all the describes succeed.
     */
    fun all(): KafkaFuture<Map<String, ConsumerGroupDescription>> {
        return KafkaFuture.allOf(*futures.values.toTypedArray()).thenApply {
            val descriptions: MutableMap<String, ConsumerGroupDescription> = HashMap(futures.size)
            futures.forEach { (key: String, future: KafkaFuture<ConsumerGroupDescription>) ->
                try {
                    descriptions[key] = future.get()
                } catch (e: InterruptedException) {
                    // This should be unreachable, since the KafkaFuture#allOf already ensured
                    // that all the futures completed successfully.
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
            }
            descriptions
        }
    }
}
