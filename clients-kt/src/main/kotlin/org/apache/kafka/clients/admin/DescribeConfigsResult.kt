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
import org.apache.kafka.common.config.ConfigResource
import java.util.concurrent.ExecutionException

/**
 * The result of the [KafkaAdminClient.describeConfigs] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeConfigsResult(
    private val futures: Map<ConfigResource, KafkaFuture<Config>>,
) {

    /**
     * Return a map from resources to futures which can be used to check the status of the
     * configuration for each resource.
     */
    fun values(): Map<ConfigResource, KafkaFuture<Config>> = futures

    /**
     * Return a future which succeeds only if all the config descriptions succeed.
     */
    fun all(): KafkaFuture<Map<ConfigResource, Config>> {
        return KafkaFuture.allOf(futures.values).thenApply {
            futures.mapValues { (_, value) ->
                try {
                    value.get()
                } catch (e: InterruptedException) {
                    // This should be unreachable, because allOf ensured that all the futures
                    // completed successfully.
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
            }
        }
    }
}
