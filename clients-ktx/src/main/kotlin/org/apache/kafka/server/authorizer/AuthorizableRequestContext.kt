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

package org.apache.kafka.server.authorizer

import java.net.InetAddress
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol

/**
 * Request context interface that provides data from request header as well as connection and
 * authentication information to plugins.
 */
@Evolving
interface AuthorizableRequestContext {

    /**
     * Returns name of listener on which request was received.
     */
    fun listenerName(): String?

    /**
     * Returns the security protocol for the listener on which request was received.
     */
    fun securityProtocol(): SecurityProtocol

    /**
     * Returns authenticated principal for the connection on which request was received.
     */
    fun principal(): KafkaPrincipal

    /**
     * Returns client IP address from which request was sent.
     */
    fun clientAddress(): InetAddress

    /**
     * 16-bit API key of the request from the request header. See
     * [protocol api keys](https://kafka.apache.org/protocol#protocol_api_keys) for request types.
     */
    fun requestType(): Int

    /**
     * Returns the request version from the request header.
     */
    fun requestVersion(): Int

    /**
     * Returns the client id from the request header.
     */
    fun clientId(): String

    /**
     * Returns the correlation id from the request header.
     */
    fun correlationId(): Int
}
