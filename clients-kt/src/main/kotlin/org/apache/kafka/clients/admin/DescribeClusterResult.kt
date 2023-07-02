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
import org.apache.kafka.common.Node
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [KafkaAdminClient.describeCluster] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property nodes A future which yields a collection of nodes.
 * @property controller A future which yields the current controller id. Note that this may yield
 * `null`, if the controller ID is not yet known.
 * @property clusterId A future which yields the current cluster id. The future value is non-null
 * if the broker version is 0.10.1.0 or higher and null otherwise.
 * @property authorizedOperations A future which yields authorized operations. The future value is
 * non-null if the broker supplied this information, and `null` otherwise.
 */
@Evolving
class DescribeClusterResult internal constructor(
    val nodes: KafkaFuture<Collection<Node>>,
    val controller: KafkaFuture<Node?>,
    val clusterId: KafkaFuture<String>,
    val authorizedOperations: KafkaFuture<Set<AclOperation>?>,
) {

    /**
     * Returns a future which yields a collection of nodes.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("nodes"),
    )
    fun nodes(): KafkaFuture<Collection<Node>> = nodes

    /**
     * Returns a future which yields the current controller id. Note that this may yield `null`, if
     * the controller ID is not yet known.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("controller"),
    )
    fun controller(): KafkaFuture<Node?> = controller

    /**
     * Returns a future which yields the current cluster id. The future value will be non-null if
     * the broker version is 0.10.1.0 or higher and null otherwise.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("clusterId"),
    )
    fun clusterId(): KafkaFuture<String> = clusterId

    /**
     * Returns a future which yields authorized operations. The future value will be non-null if the
     * broker supplied this information, and `null` otherwise.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("authorizedOperations"),
    )
    fun authorizedOperations(): KafkaFuture<Set<AclOperation>?> = authorizedOperations
}
