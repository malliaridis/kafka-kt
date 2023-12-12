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

package org.apache.kafka.server.policy

import org.apache.kafka.common.Configurable
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.PolicyViolationException

/**
 * An interface for enforcing a policy on alter configs requests.
 *
 * Common use cases are requiring that the replication factor, `min.insync.replicas` and/or
 * retention settings for a topic remain within an allowable range.
 *
 * If `alter.config.policy.class.name` is defined, Kafka will create an instance of the specified
 * class using the default constructor and will then pass the broker configs to its `configure()`
 * method. During broker shutdown, the `close()` method will be invoked so that resources can be
 * released (if necessary).
 */
interface AlterConfigPolicy : Configurable, AutoCloseable {

    /**
     * Class containing the create request parameters.
     *
     * @constructor Creates an instance of this class with the provided parameters. This constructor is public to make
     * testing of [AlterConfigPolicy] implementations easier.
     */
    data class RequestMetadata(
        val resource: ConfigResource,
        val configs: Map<String, String>,
    ) {

        /**
         * Return the configs in the request.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("configs"),
        )
        fun configs(): Map<String, String> = configs

        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("resource"),
        )
        fun resource(): ConfigResource = resource

        override fun toString(): String {
            return "AlterConfigPolicy.RequestMetadata(resource=$resource" +
                    ", configs=$configs)"
        }
    }

    /**
     * Validate the request parameters and throw a `PolicyViolationException` with a suitable error
     * message if the alter configs request parameters for the provided resource do not satisfy this
     * policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message. Note
     * that validation failure only affects the relevant resource, other resources in the request
     * will still be processed.
     *
     * @param requestMetadata the alter configs request parameters for the provided resource (topic
     * is the only resource type whose configs can be updated currently).
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    @Throws(PolicyViolationException::class)
    fun validate(requestMetadata: RequestMetadata)
}
