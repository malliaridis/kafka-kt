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

package org.apache.kafka.common.security.auth

import java.net.InetAddress
import javax.net.ssl.SSLSession
import javax.security.sasl.SaslServer

class SaslAuthenticationContext(
    val server: SaslServer,
    private val securityProtocol: SecurityProtocol,
    private val clientAddress: InetAddress,
    private val listenerName: String?,
    val sslSession: SSLSession? = null
) : AuthenticationContext {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("server"),
    )
    fun server(): SaslServer = server

    /**
     * Returns SSL session for the connection if security protocol is SASL_SSL. If SSL
     * mutual client authentication is enabled for the listener, peer principal can be
     * determined using [SSLSession.getPeerPrincipal].
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("sslSession"),
    )
    fun sslSession(): SSLSession? = sslSession

    override fun securityProtocol(): SecurityProtocol = securityProtocol

    override fun clientAddress(): InetAddress = clientAddress

    override fun listenerName(): String? = listenerName
}
