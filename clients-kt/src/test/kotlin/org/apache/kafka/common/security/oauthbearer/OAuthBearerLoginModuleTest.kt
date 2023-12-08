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

import java.io.IOException
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.LoginException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

class OAuthBearerLoginModuleTest {

    private class TestCallbackHandler(
        private val tokens: Array<OAuthBearerToken>,
        private val extensions: Array<SaslExtensions?>,
    ) : AuthenticateCallbackHandler {
        
        private var index = 0
        
        private var extensionsIndex = 0

        @Throws(IOException::class, UnsupportedCallbackException::class)
        override fun handle(callbacks: Array<Callback>) {
            for (callback in callbacks) {
                when (callback) {
                    is OAuthBearerTokenCallback -> try {
                        handleCallback(callback)
                    } catch (e: KafkaException) {
                        throw IOException(e.message, e)
                    }

                    is SaslExtensionsCallback -> {
                        try {
                            handleExtensionsCallback(callback)
                        } catch (e: KafkaException) {
                            throw IOException(e.message, e)
                        }
                    }

                    else -> throw UnsupportedCallbackException(callback)
                }
            }
        }

        override fun configure(
            configs: Map<String, *>,
            saslMechanism: String,
            jaasConfigEntries: List<AppConfigurationEntry>
        ) = Unit

        override fun close() = Unit

        @Throws(IOException::class)
        private fun handleCallback(callback: OAuthBearerTokenCallback) {
            require(callback.token == null) { "Callback had a token already" }
            if (tokens.size > index) callback.token = tokens[index++]
            else throw IOException("no more tokens")
        }

        @Throws(IOException::class, UnsupportedCallbackException::class)
        private fun handleExtensionsCallback(callback: SaslExtensionsCallback) {
            if (extensions.size > extensionsIndex) {
                val extension = extensions[extensionsIndex++]
                if (extension === RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG)
                    throw UnsupportedCallbackException(callback)
                callback.extensions = extension!!
            } else throw IOException("no more extensions")
        }
    }

    @Test
    @Throws(LoginException::class)
    fun login1Commit1Login2Commit2Logout1Login3Commit3Logout2() {
        /*
         * Invoke login()/commit() on loginModule1; invoke login/commit() on
         * loginModule2; invoke logout() on loginModule1; invoke login()/commit() on
         * loginModule3; invoke logout() on loginModule2
         */
        val subject = Subject()
        val privateCredentials = subject.privateCredentials
        val publicCredentials = subject.publicCredentials

        // Create callback handler
        val tokens = arrayOf(
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
        )
        val extensions = arrayOf<SaslExtensions?>(
            saslExtensions(),
            saslExtensions(),
            saslExtensions()
        )
        val testTokenCallbackHandler = TestCallbackHandler(tokens, extensions)

        // Create login modules
        val loginModule1 = OAuthBearerLoginModule()
        loginModule1.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        val loginModule2 = OAuthBearerLoginModule()
        loginModule2.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        val loginModule3 = OAuthBearerLoginModule()
        loginModule3.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )

        // Should start with nothing
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.commit()

        // Now we should have the first token and extensions
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertSame(extensions[0], publicCredentials.first())

        // Now login on loginModule2 to get the second token
        // loginModule2 does not support the extensions callback and will raise UnsupportedCallbackException
        loginModule2.login()

        // Should still have just the first token and extensions
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertSame(extensions[0], publicCredentials.first())
        loginModule2.commit()

        // Should have the first and second tokens at this point
        assertEquals(2, privateCredentials.size)
        assertEquals(2, publicCredentials.size)
        var iterator = privateCredentials.iterator()
        var publicIterator = publicCredentials.iterator()
        assertNotSame(tokens[2], iterator.next())
        assertNotSame(tokens[2], iterator.next())
        assertNotSame(extensions[2], publicIterator.next())
        assertNotSame(extensions[2], publicIterator.next())

        // finally logout() on loginModule1
        loginModule1.logout()

        // Now we should have just the second token and extension
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[1], privateCredentials.first())
        assertSame(extensions[1], publicCredentials.first())

        // Now login on loginModule3 to get the third token
        loginModule3.login()

        // Should still have just the second token and extensions
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[1], privateCredentials.first())
        assertSame(extensions[1], publicCredentials.first())
        loginModule3.commit()

        // Should have the second and third tokens at this point
        assertEquals(2, privateCredentials.size)
        assertEquals(2, publicCredentials.size)
        iterator = privateCredentials.iterator()
        publicIterator = publicCredentials.iterator()
        assertNotSame(tokens[0], iterator.next())
        assertNotSame(tokens[0], iterator.next())
        assertNotSame(extensions[0], publicIterator.next())
        assertNotSame(extensions[0], publicIterator.next())

        // finally logout() on loginModule2
        loginModule2.logout()

        // Now we should have just the third token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[2], privateCredentials.first())
        assertSame(extensions[2], publicCredentials.first())
        verifyNoInteractions(*tokens)
    }

    @Test
    @Throws(LoginException::class)
    fun login1Commit1Logout1Login2Commit2Logout2() {
        /*
         * Invoke login()/commit() on loginModule1; invoke logout() on loginModule1;
         * invoke login()/commit() on loginModule2; invoke logout() on loginModule2
         */
        val subject = Subject()
        val privateCredentials = subject.privateCredentials
        val publicCredentials = subject.publicCredentials

        // Create callback handler
        val tokens = arrayOf(
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
        )
        val extensions = arrayOf<SaslExtensions?>(
            saslExtensions(),
            saslExtensions()
        )
        val testTokenCallbackHandler = TestCallbackHandler(tokens, extensions)

        // Create login modules
        val loginModule1 = OAuthBearerLoginModule()
        loginModule1.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        val loginModule2 = OAuthBearerLoginModule()
        loginModule2.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )

        // Should start with nothing
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.commit()

        // Now we should have the first token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertSame(extensions[0], publicCredentials.first())
        loginModule1.logout()

        // Should have nothing again
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule2.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule2.commit()

        // Now we should have the second token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[1], privateCredentials.first())
        assertSame(extensions[1], publicCredentials.first())
        loginModule2.logout()

        // Should have nothing again
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        verifyNoInteractions(*tokens)
    }

    @Test
    @Throws(LoginException::class)
    fun loginAbortLoginCommitLogout() {
        /*
         * Invoke login(); invoke abort(); invoke login(); logout()
         */
        val subject = Subject()
        val privateCredentials = subject.privateCredentials
        val publicCredentials = subject.publicCredentials

        // Create callback handler
        val tokens = arrayOf(
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
        )
        val extensions = arrayOf<SaslExtensions?>(
            saslExtensions(),
            saslExtensions()
        )
        val testTokenCallbackHandler = TestCallbackHandler(tokens, extensions)

        // Create login module
        val loginModule = OAuthBearerLoginModule()
        loginModule.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )

        // Should start with nothing
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule.abort()

        // Should still have nothing since we aborted
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule.commit()

        // Now we should have the second token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[1], privateCredentials.first())
        assertSame(extensions[1], publicCredentials.first())
        loginModule.logout()

        // Should have nothing again
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        verifyNoInteractions(*tokens)
    }

    @Test
    @Throws(LoginException::class)
    fun login1Commit1Login2Abort2Login3Commit3Logout3() {
        /*
         * Invoke login()/commit() on loginModule1; invoke login()/abort() on
         * loginModule2; invoke login()/commit()/logout() on loginModule3
         */
        val subject = Subject()
        val privateCredentials = subject.privateCredentials
        val publicCredentials = subject.publicCredentials

        // Create callback handler
        val tokens = arrayOf(
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
        )
        val extensions = arrayOf<SaslExtensions?>(
            saslExtensions(),
            saslExtensions(),
            saslExtensions(),
        )
        val testTokenCallbackHandler = TestCallbackHandler(tokens, extensions)

        // Create login modules
        val loginModule1 = OAuthBearerLoginModule()
        loginModule1.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        val loginModule2 = OAuthBearerLoginModule()
        loginModule2.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        val loginModule3 = OAuthBearerLoginModule()
        loginModule3.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )

        // Should start with nothing
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.login()

        // Should still have nothing until commit() is called
        assertEquals(0, privateCredentials.size)
        assertEquals(0, publicCredentials.size)
        loginModule1.commit()

        // Now we should have the first token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertSame(extensions[0], publicCredentials.first())

        // Now go get the second token
        loginModule2.login()

        // Should still have first token
        assertEquals(1, privateCredentials.size)
        assertEquals(1, publicCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertSame(extensions[0], publicCredentials.first())
        loginModule2.abort()

        // Should still have just the first token because we aborted
        assertEquals(1, privateCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertEquals(1, publicCredentials.size)
        assertSame(extensions[0], publicCredentials.first())

        // Now go get the third token
        loginModule2.login()

        // Should still have first token
        assertEquals(1, privateCredentials.size)
        assertSame(tokens[0], privateCredentials.first())
        assertEquals(1, publicCredentials.size)
        assertSame(extensions[0], publicCredentials.first())
        loginModule2.commit()

        // Should have first and third tokens at this point
        assertEquals(2, privateCredentials.size)
        val iterator = privateCredentials.iterator()
        assertNotSame(tokens[1], iterator.next())
        assertNotSame(tokens[1], iterator.next())
        assertEquals(2, publicCredentials.size)
        val publicIterator = publicCredentials.iterator()
        assertNotSame(extensions[1], publicIterator.next())
        assertNotSame(extensions[1], publicIterator.next())
        loginModule1.logout()

        // Now we should have just the third token
        assertEquals(1, privateCredentials.size)
        assertSame(tokens[2], privateCredentials.first())
        assertEquals(1, publicCredentials.size)
        assertSame(extensions[2], publicCredentials.first())
        verifyNoInteractions(*tokens)
    }

    /**
     * 2.1.0 added customizable SASL extensions and a new callback type.
     * Ensure that old, custom-written callbackHandlers that do not handle the callback work
     */
    @Test
    @Throws(LoginException::class)
    fun commitDoesNotThrowOnUnsupportedExtensionsCallback() {
        val subject = Subject()

        // Create callback handler
        val tokens = arrayOf(
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
            mock<OAuthBearerToken>(),
        )
        val testTokenCallbackHandler = TestCallbackHandler(
            tokens = tokens,
            extensions = arrayOf(RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG),
        )

        // Create login modules
        val loginModule1 = OAuthBearerLoginModule()
        loginModule1.initialize(
            subject = subject,
            callbackHandler = testTokenCallbackHandler,
            sharedState = emptyMap<String, Any>(),
            options = emptyMap<String, Any>(),
        )
        loginModule1.login()
        // Should populate public credentials with SaslExtensions and not throw an exception
        loginModule1.commit()
        val extensions = subject.getPublicCredentials(SaslExtensions::class.java).first()
        assertNotNull(extensions)
        assertTrue(extensions.map().isEmpty())
        verifyNoInteractions(*tokens)
    }

    /**
     * We don't want to use mocks for our tests as we need to make sure to test
     * [SaslExtensions]' [SaslExtensions.equals] and
     * [SaslExtensions.hashCode] methods.
     *
     * We need to make distinct calls to this method (vs. caching the result and reusing it
     * multiple times) because we need to ensure the [SaslExtensions] instances are unique.
     * This properly mimics the behavior that is used during the token refresh logic.
     *
     * @return Unique, newly-created [SaslExtensions] instance
     */
    private fun saslExtensions(): SaslExtensions = SaslExtensions.empty()

    companion object {
        val RAISE_UNSUPPORTED_CB_EXCEPTION_FLAG: SaslExtensions? = null
    }
}
