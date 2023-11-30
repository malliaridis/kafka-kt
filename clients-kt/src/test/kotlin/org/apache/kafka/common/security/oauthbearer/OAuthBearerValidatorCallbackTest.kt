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

package org.apache.kafka.common.security.oauthbearer

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame

class OAuthBearerValidatorCallbackTest {

    @Test
    fun testError() {
        val errorStatus = "errorStatus"
        val errorScope = "errorScope"
        val errorOpenIDConfiguration = "errorOpenIDConfiguration"
        val callback = OAuthBearerValidatorCallback(TOKEN.value()!!)
        callback.error(errorStatus, errorScope, errorOpenIDConfiguration)
        assertEquals(errorStatus, callback.errorStatus)
        assertEquals(errorScope, callback.errorScope)
        assertEquals(errorOpenIDConfiguration, callback.errorOpenIDConfiguration)
        assertNull(callback.token)
    }

    @Test
    fun testToken() {
        val callback = OAuthBearerValidatorCallback(TOKEN.value()!!).apply { setToken(TOKEN) }
        assertSame(TOKEN, callback.token)
        assertNull(callback.errorStatus)
        assertNull(callback.errorScope)
        assertNull(callback.errorOpenIDConfiguration)
    }

    companion object {
        private val TOKEN: OAuthBearerToken = object : OAuthBearerToken {
            override fun value(): String = "value"
            override fun startTimeMs(): Long? = null
            override fun scope(): Set<String> = emptySet()
            override fun principalName(): String = "principalName"
            override fun lifetimeMs(): Long = 0
        }
    }
}
