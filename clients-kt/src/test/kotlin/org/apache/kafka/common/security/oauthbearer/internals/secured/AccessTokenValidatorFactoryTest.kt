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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.io.IOException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
import org.junit.jupiter.api.Test

class AccessTokenValidatorFactoryTest : OAuthBearerTest() {

    @Test
    fun testConfigureThrowsExceptionOnAccessTokenValidatorInit() {
        val handler = OAuthBearerLoginCallbackHandler()
        val accessTokenRetriever = object : AccessTokenRetriever {

            @Throws(IOException::class)
            override fun init() = throw IOException("My init had an error!")

            override fun retrieve(): String = "dummy"
        }
        val configs = saslConfigs
        val accessTokenValidator = AccessTokenValidatorFactory.create(configs)
        assertThrowsWithMessage(
            clazz = KafkaException::class.java,
            executable = { handler.init(accessTokenRetriever, accessTokenValidator) },
            substring = "encountered an error when initializing",
        )
    }

    @Test
    fun testConfigureThrowsExceptionOnAccessTokenValidatorClose() {
        val handler = OAuthBearerLoginCallbackHandler()
        val accessTokenRetriever = object : AccessTokenRetriever {

            @Throws(IOException::class)
            override fun close() = throw IOException("My close had an error!")

            override fun retrieve(): String = "dummy"
        }
        val configs = saslConfigs
        val accessTokenValidator = AccessTokenValidatorFactory.create(configs)
        handler.init(accessTokenRetriever, accessTokenValidator)

        // Basically asserting this doesn't throw an exception :(
        handler.close()
    }
}
