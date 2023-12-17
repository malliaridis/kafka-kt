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

package org.apache.kafka.server.network

import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.Endpoint
import org.apache.kafka.server.authorizer.AuthorizerServerInfo

/**
 * Runtime broker configuration metadata provided to authorizers during start up.
 */
class KafkaAuthorizerServerInfo(
    private val clusterResource: ClusterResource,
    private val brokerId: Int,
    endpoints: Collection<Endpoint>,
    private val interbrokerEndpoint: Endpoint,
    earlyStartListeners: Collection<String>,
) : AuthorizerServerInfo {

    private val endpoints: Collection<Endpoint> = endpoints.toList()

    private val earlyStartListeners: Collection<String> = earlyStartListeners.toList()

    override fun clusterResource(): ClusterResource = clusterResource

    override fun brokerId(): Int = brokerId

    override fun endpoints(): Collection<Endpoint> = endpoints

    override fun interBrokerEndpoint(): Endpoint = interbrokerEndpoint

    override fun earlyStartListeners(): Collection<String> = earlyStartListeners

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KafkaAuthorizerServerInfo

        if (clusterResource != other.clusterResource) return false
        if (brokerId != other.brokerId) return false
        if (interbrokerEndpoint != other.interbrokerEndpoint) return false
        if (endpoints != other.endpoints) return false
        if (earlyStartListeners != other.earlyStartListeners) return false

        return true
    }

    override fun hashCode(): Int {
        var result = clusterResource.hashCode()
        result = 31 * result + brokerId
        result = 31 * result + interbrokerEndpoint.hashCode()
        result = 31 * result + endpoints.hashCode()
        result = 31 * result + earlyStartListeners.hashCode()
        return result
    }

    override fun toString(): String = "KafkaAuthorizerServerInfo(" +
            "clusterResource=$clusterResource" +
            ", brokerId=$brokerId" +
            ", endpoints=$endpoints" +
            ", earlyStartListeners=$earlyStartListeners" +
            ")"
}
