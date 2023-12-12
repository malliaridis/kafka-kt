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

package org.apache.kafka.common

import java.util.Optional
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.security.auth.SecurityProtocol

/**
 * Represents a broker endpoint.
 */
@Evolving
data class Endpoint(
    val listenerName: String?,
    val securityProtocol: SecurityProtocol,
    val host: String,
    val port: Int
) {

    /**
     * Returns the listener name of this endpoint. This is non-empty for endpoints provided
     * to broker plugins, but may be empty when used in clients.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("listenerName")
    )
    fun listenerName(): String? = listenerName

    /**
     * Returns the security protocol of this endpoint.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("securityProtocol")
    )
    fun securityProtocol(): SecurityProtocol {
        return securityProtocol
    }

    /**
     * Returns advertised host name of this endpoint.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("host")
    )
    fun host(): String {
        return host
    }

    /**
     * Returns the port to which the listener is bound.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("port")
    )
    fun port(): Int {
        return port
    }

    override fun toString(): String {
        return "Endpoint(" +
                "listenerName='$listenerName'" +
                ", securityProtocol=$securityProtocol" +
                ", host='$host'" +
                ", port=$port" +
                ')'
    }
}
