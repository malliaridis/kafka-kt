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

package org.apache.kafka.common.security.plain

import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.spi.LoginModule
import org.apache.kafka.common.security.plain.internals.PlainSaslServerProvider

open class PlainLoginModule : LoginModule {

    override fun initialize(
        subject: Subject,
        callbackHandler: CallbackHandler,
        sharedState: Map<String, *>,
        options: Map<String, *>
    ) {
        val username = options[USERNAME_CONFIG] as String?
        if (username != null) subject.publicCredentials.add(username)

        val password = options[PASSWORD_CONFIG] as String?
        if (password != null) subject.privateCredentials.add(password)
    }

    override fun login(): Boolean = true

    override fun logout(): Boolean = true

    override fun commit(): Boolean = true

    override fun abort(): Boolean = false

    companion object {

        private const val USERNAME_CONFIG = "username"

        private const val PASSWORD_CONFIG = "password"

        init {
            PlainSaslServerProvider.initialize()
        }
    }
}
