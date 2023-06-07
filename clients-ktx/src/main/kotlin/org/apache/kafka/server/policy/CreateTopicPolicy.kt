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
import org.apache.kafka.common.errors.PolicyViolationException

/**
 * An interface for enforcing a policy on create topics requests.
 *
 * Common use cases are requiring that the replication factor, `min.insync.replicas` and/or
 * retention settings for a topic are within an allowable range.
 *
 * If `create.topic.policy.class.name` is defined, Kafka will create an instance of the specified
 * class using the default constructor and will then pass the broker configs to its `configure()`
 * method. During broker shutdown, the `close()` method will be invoked so that resources can be
 * released (if necessary).
 *
 * Create an instance of this class with the provided parameters.
 *
 * This constructor is public to make testing of `CreateTopicPolicy` implementations easier.
 */
interface CreateTopicPolicy : Configurable, AutoCloseable {

    /**
     * Class containing the create request parameters.
     *
     * @property topic the name of the topic to create.
     * @property numPartitions the number of partitions to create or null if replicasAssignments is
     * set.
     * @property replicationFactor the replication factor for the topic or null if
     * replicaAssignments is set.
     * @property replicasAssignments replica assignments or null if numPartitions and
     * replicationFactor is set. The assignment is a map from partition id to replica (broker) ids.
     * @property configs topic configs for the topic to be created, not including broker defaults.
     * Broker configs are passed via the `configure()` method of the policy implementation.
     */
    data class RequestMetadata(
        val topic: String,
        val numPartitions: Int,
        val replicationFactor: Short,
        val replicasAssignments: Map<Int, List<Int>>?,
        val configs: Map<String, String>
    ) {

        /**
         * Return the name of the topic to create.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("topic"),
        )
        fun topic(): String = topic

        /**
         * Return the number of partitions to create or `null` if `replicaAssignments` is not `null`.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("numPartitions"),
        )
        fun numPartitions(): Int = numPartitions

        /**
         * Return the number of replicas to create or `null` if `replicaAssignments` is not `null`.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("replicationFactor"),
        )
        fun replicationFactor(): Short = replicationFactor

        /**
         * Return a map from partition id to replica (broker) ids or `null` if [numPartitions] and
         * [replicationFactor] are set instead.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("replicasAssignments"),
        )
        fun replicasAssignments(): Map<Int, List<Int>>? = replicasAssignments

        /**
         * Return topic configs in the request, not including broker defaults. Broker configs are
         * passed via the `configure()` method of the policy implementation.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("configs"),
        )
        fun configs(): Map<String, String> = configs

        override fun toString(): String {
            return "CreateTopicPolicy.RequestMetadata(topic=$topic" +
                    ", numPartitions=$numPartitions" +
                    ", replicationFactor=$replicationFactor" +
                    ", replicasAssignments=$replicasAssignments" +
                    ", configs=$configs)"
        }
    }

    /**
     * Validate the request parameters and throw a `PolicyViolationException` with a suitable error
     * message if the create topics request parameters for the provided topic do not satisfy this
     * policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message. Note
     * that validation failure only affects the relevant topic, other topics in the request will
     * still be processed.
     *
     * @param requestMetadata the create topics request parameters for the provided topic.
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    @Throws(PolicyViolationException::class)
    fun validate(requestMetadata: RequestMetadata)
}
