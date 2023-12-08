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

import javax.security.auth.Subject
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException

/**
 * Login interface for authentication.
 */
interface Login {

    /**
     * Configures this login instance.
     * @param configs Key-value pairs containing the parsed configuration options of the client or
     * broker. Note that these are the Kafka configuration options and not the JAAS configuration
     * options. The JAAS options may be obtained from `jaasConfiguration`.
     * @param contextName JAAS context name for this login which may be used to obtain the login
     * context from `jaasConfiguration`.
     * @param jaasConfiguration JAAS configuration containing the login context named `contextName`.
     * If static JAAS configuration is used, this `Configuration` may also contain other login
     * contexts.
     * @param loginCallbackHandler Login callback handler instance to use for this Login. Login
     * callback handler class may be configured using
     * [org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS].
     */
    fun configure(
        configs: Map<String, *>,
        contextName: String,
        jaasConfiguration: Configuration,
        loginCallbackHandler: AuthenticateCallbackHandler,
    )

    /**
     * Performs login for each login module specified for the login context of this instance.
     */
    @Throws(LoginException::class)
    fun login(): LoginContext?

    /**
     * Returns the authenticated subject of this login context.
     */
    fun subject(): Subject?

    /**
     * Returns the service name to be used for SASL.
     */
    fun serviceName(): String?

    /**
     * Closes this instance.
     */
    fun close()
}
