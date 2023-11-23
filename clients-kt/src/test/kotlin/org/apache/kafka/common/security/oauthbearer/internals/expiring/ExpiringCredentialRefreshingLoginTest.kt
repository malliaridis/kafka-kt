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

package org.apache.kafka.common.security.oauthbearer.internals.expiring

import java.util.Date
import java.util.Objects
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin.LoginContextFactory
import org.apache.kafka.common.utils.MockScheduler
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.`when`
import org.mockito.internal.util.MockUtil.isMock
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.mock
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExpiringCredentialRefreshingLoginTest {

    /*
     * An ExpiringCredentialRefreshingLogin that we can tell explicitly to
     * create/remove an expiring credential with specific
     * create/expire/absoluteLastRefresh times
     */
    private open class TestExpiringCredentialRefreshingLogin(
        refreshConfig: ExpiringCredentialRefreshConfig?,
        loginContextFactory: LoginContextFactory?,
        private val time: Time,
        private val lifetimeMillis: Long,
        private val absoluteLastRefreshTimeMs: Long,
        private val clientReloginAllowedBeforeLogout: Boolean,
    ) : ExpiringCredentialRefreshingLogin(
        contextName = "contextName",
        configuration = EMPTY_WILDCARD_CONFIGURATION,
        expiringCredentialRefreshConfig = refreshConfig!!,
        callbackHandler = object : AuthenticateCallbackHandler {
            override fun configure(
                configs: Map<String, *>,
                saslMechanism: String,
                jaasConfigEntries: List<AppConfigurationEntry>,
            ) = Unit

            override fun close() = Unit
            override fun handle(callbacks: Array<out Callback>?) = Unit
        },
        mandatoryClassToSynchronizeOnPriorToRefresh = TestExpiringCredentialRefreshingLogin::class.java,
        loginContextFactory = loginContextFactory!!,
        time = Objects.requireNonNull(time),
    ) {

        private var expiringCredential: ExpiringCredential? = null

        private var tmpExpiringCredential: ExpiringCredential? = null

        open val createMs: Long = time.milliseconds()

        val expireTimeMs: Long = time.milliseconds() + lifetimeMillis

        // Invoke at login time
        fun createNewExpiringCredential() {
            if (!clientReloginAllowedBeforeLogout)
            // Was preceded by logout
                expiringCredential = internalNewExpiringCredential() else {
                val initialLogin = expiringCredential == null

                // no logout immediately after the initial login
                if (initialLogin) expiringCredential = internalNewExpiringCredential()
                // This is at least the second invocation of login; we will move the credential
                // over upon logout, which should be invoked next
                else tmpExpiringCredential = internalNewExpiringCredential()
            }
        }

        // Invoke at logout time
        fun clearExpiringCredential() {
            expiringCredential =
                if (!clientReloginAllowedBeforeLogout) null // Have not yet invoked login
                else tmpExpiringCredential // login has already been invoked
        }

        override fun expiringCredential(): ExpiringCredential? {
            return expiringCredential
        }

        private fun internalNewExpiringCredential(): ExpiringCredential {

            return object : ExpiringCredential {

                private val createMs = this@TestExpiringCredentialRefreshingLogin.createMs

                private val expireTimeMs = this@TestExpiringCredentialRefreshingLogin.expireTimeMs

                override fun principalName(): String = "Created at " + Date(createMs)

                override fun startTimeMs(): Long = createMs

                override fun expireTimeMs(): Long = expireTimeMs

                override fun absoluteLastRefreshTimeMs(): Long = absoluteLastRefreshTimeMs

                override fun toString() = "startTimeMs=${startTimeMs()}, " +
                        "expireTimeMs=${expireTimeMs()}, " +
                        "absoluteLastRefreshTimeMs=${absoluteLastRefreshTimeMs()}"
            }
        }
    }

    /*
     * A class that will forward all login/logout/getSubject() calls to a mock while
     * also telling an instance of TestExpiringCredentialRefreshingLogin to
     * create/remove an expiring credential upon login/logout(). Basically we are
     * getting the functionality of a mock while simultaneously in the same method
     * call performing creation/removal of expiring credentials.
     */
    private class TestLoginContext(
        private val testExpiringCredentialRefreshingLogin: TestExpiringCredentialRefreshingLogin,
        private val mockLoginContext: LoginContext,
    ) : LoginContext("contextName", null, null, EMPTY_WILDCARD_CONFIGURATION) {

        init {
            // sanity check to make sure it is likely a mock
            require(isMock(mockLoginContext))
        }

        @Throws(LoginException::class)
        override fun login() {
            // Here is where we get the functionality of a mock while simultaneously
            // performing the creation of an expiring credential
            mockLoginContext.login()
            testExpiringCredentialRefreshingLogin.createNewExpiringCredential()
        }

        @Throws(LoginException::class)
        override fun logout() {
            // Here is where we get the functionality of a mock while simultaneously
            // performing the removal of an expiring credential
            mockLoginContext.logout()
            testExpiringCredentialRefreshingLogin.clearExpiringCredential()
        }

        override fun getSubject(): Subject {
            // here we just need the functionality of a mock
            return mockLoginContext.getSubject()
        }
    }

    // An implementation of LoginContextFactory that returns an instance of TestLoginContext
    private class TestLoginContextFactory : LoginContextFactory() {

        private val refresherThreadStartedFuture = KafkaFutureImpl<Any?>()

        private val refresherThreadDoneFuture = KafkaFutureImpl<Any?>()

        private lateinit var testLoginContext: TestLoginContext

        @Throws(LoginException::class)
        fun configure(
            mockLoginContext: LoginContext,
            testExpiringCredentialRefreshingLogin: TestExpiringCredentialRefreshingLogin,
        ) {
            // sanity check to make sure it is likely a mock
            require(isMock(mockLoginContext))
            testLoginContext = TestLoginContext(testExpiringCredentialRefreshingLogin, mockLoginContext)
        }

        @Throws(LoginException::class)
        override fun createLoginContext(
            expiringCredentialRefreshingLogin: ExpiringCredentialRefreshingLogin,
        ): LoginContext {
            return object : LoginContext("", null, null, EMPTY_WILDCARD_CONFIGURATION) {

                private var loginSuccess = false

                @Throws(LoginException::class)
                override fun login() {
                    testLoginContext.login()
                    loginSuccess = true
                }

                @Throws(LoginException::class)
                override fun logout() {
                    // will cause the refresher thread to exit
                    check(loginSuccess) { "logout called without a successful login" }
                    testLoginContext.logout()
                }

                override fun getSubject(): Subject = testLoginContext.subject
            }
        }

        override fun refresherThreadStarted() {
            refresherThreadStartedFuture.complete(null)
        }

        override fun refresherThreadDone() {
            refresherThreadDoneFuture.complete(null)
        }

        fun refresherThreadStartedFuture(): Future<*> = refresherThreadStartedFuture

        fun refresherThreadDoneFuture(): Future<*> = refresherThreadDoneFuture
    }

    @Test
    @Throws(Exception::class)
    fun testRefresh() {
        for (numExpectedRefreshes in intArrayOf(0, 1, 2)) {
            for (clientReloginAllowedBeforeLogout in booleanArrayOf(true, false)) {
                val subject = Subject()
                val mockLoginContext = mock<LoginContext>()

                `when`(mockLoginContext.getSubject()).thenReturn(subject)
                val mockTime = MockTime()
                val startMs = mockTime.milliseconds()

                // Identify the lifetime of each expiring credential
                val lifetimeMinutes = 100L

                // Identify the point at which refresh will occur in that lifetime
                val refreshEveryMinutes = 80L

                // Set an absolute last refresh time that will cause the login thread to exit
                // after a certain number of re-logins (by adding an extra half of a refresh interval).
                val absoluteLastRefreshMs =
                    (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                            - 1000 * 60 * refreshEveryMinutes / 2)

                // Identify buffer time on either side for the refresh algorithm
                val minPeriodSeconds = 0.toShort()

                // Define some listeners so we can keep track of who gets done and when. All
                // added listeners should end up done except the last, extra one, which should not.
                val mockScheduler = MockScheduler(mockTime)
                val waiters = addWaiters(
                    mockScheduler = mockScheduler,
                    refreshEveryMillis = 1000 * 60 * refreshEveryMinutes,
                    numWaiters = numExpectedRefreshes + 1,
                )

                // Create the ExpiringCredentialRefreshingLogin instance under test
                val testLoginContextFactory = TestLoginContextFactory()
                val testExpiringCredentialRefreshingLogin = TestExpiringCredentialRefreshingLogin(
                    refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                        refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                        minPeriodSeconds = minPeriodSeconds,
                        bufferSeconds = minPeriodSeconds,
                        clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
                    ),
                    loginContextFactory = testLoginContextFactory,
                    time = mockTime,
                    lifetimeMillis = 1000 * 60 * lifetimeMinutes,
                    absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
                    clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
                )
                testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

                // Perform the login, wait up to a certain amount of time for the refresher
                // thread to exit, and make sure the correct calls happened at the correct times
                val expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes
                assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
                assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
                testExpiringCredentialRefreshingLogin.login()
                assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
                testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
                assertEquals(expectedFinalMs, mockTime.milliseconds())
                for (i in (0 until numExpectedRefreshes)) {
                    val waiter = waiters[i]
                    assertTrue(waiter.isDone)
                    assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get() - startMs)
                }
                assertFalse(waiters[numExpectedRefreshes].isDone)

                // We expect login() to be invoked followed by getSubject() and then ultimately followed by
                // numExpectedRefreshes pairs of either login()/logout() or logout()/login() calls
                val inOrder = inOrder(mockLoginContext)
                inOrder.verify(mockLoginContext).login()
                inOrder.verify(mockLoginContext).getSubject()
                for (i in (0 until numExpectedRefreshes)) {
                    if (clientReloginAllowedBeforeLogout) {
                        inOrder.verify(mockLoginContext).login()
                        inOrder.verify(mockLoginContext).logout()
                    } else {
                        inOrder.verify(mockLoginContext).logout()
                        inOrder.verify(mockLoginContext).login()
                    }
                }
                testExpiringCredentialRefreshingLogin.close()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRefreshWithExpirationSmallerThanConfiguredBuffers() {
        val numExpectedRefreshes = 1
        val clientReloginAllowedBeforeLogout = true
        val mockLoginContext = mock<LoginContext>()
        val subject = Subject()
        `when`(mockLoginContext.getSubject()).thenReturn(subject)
        val mockTime = MockTime()
        val startMs = mockTime.milliseconds()

        // Identify the lifetime of each expiring credential
        val lifetimeMinutes = 10L

        // Identify the point at which refresh will occur in that lifetime
        val refreshEveryMinutes = 8L

        // Set an absolute last refresh time that will cause the login thread to exit
        // after a certain number of re-logins (by adding an extra half of a refresh interval).
        val absoluteLastRefreshMs =
            (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                    - 1000 * 60 * refreshEveryMinutes / 2)

        // Identify buffer time on either side for the refresh algorithm that will cause
        // the entire lifetime to be taken up. In other words, make sure there is no way to honor the buffers.
        val minPeriodSeconds = (1 + lifetimeMinutes * 60 / 2).toShort()

        // Define some listeners so we can keep track of who gets done and when. All
        // added listeners should end up done except the last, extra one, which should not.
        val mockScheduler = MockScheduler(mockTime)
        val waiters = addWaiters(
            mockScheduler = mockScheduler,
            refreshEveryMillis = 1000 * 60 * refreshEveryMinutes,
            numWaiters = numExpectedRefreshes + 1,
        )

        // Create the ExpiringCredentialRefreshingLogin instance under test
        val testLoginContextFactory = TestLoginContextFactory()
        val testExpiringCredentialRefreshingLogin = TestExpiringCredentialRefreshingLogin(
            refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                minPeriodSeconds = minPeriodSeconds,
                bufferSeconds = minPeriodSeconds,
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            ),
            loginContextFactory = testLoginContextFactory,
            time = mockTime,
            lifetimeMillis = 1000 * 60 * lifetimeMinutes,
            absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
            clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
        )
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

        /*
         * Perform the login, wait up to a certain amount of time for the refresher
         * thread to exit, and make sure the correct calls happened at the correct times
         */
        val expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
        testExpiringCredentialRefreshingLogin.login()
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
        assertEquals(expectedFinalMs, mockTime.milliseconds())
        for (i in (0 until numExpectedRefreshes)) {
            val waiter = waiters[i]
            assertTrue(waiter.isDone)
            assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get() - startMs)
        }
        assertFalse(waiters[numExpectedRefreshes].isDone)
        val inOrder = inOrder(mockLoginContext)
        inOrder.verify(mockLoginContext).login()
        for (i in (0 until numExpectedRefreshes)) {
            inOrder.verify(mockLoginContext).login()
            inOrder.verify(mockLoginContext).logout()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRefreshWithExpirationSmallerThanConfiguredBuffersAndOlderCreateTime() {
        val numExpectedRefreshes = 1
        val clientReloginAllowedBeforeLogout = true
        val mockLoginContext = mock<LoginContext>()
        val subject = Subject()
        `when`(mockLoginContext.getSubject()).thenReturn(subject)
        val mockTime = MockTime()
        val startMs = mockTime.milliseconds()

        // Identify the lifetime of each expiring credential
        val lifetimeMinutes = 10L

        // Identify the point at which refresh will occur in that lifetime
        val refreshEveryMinutes = 8L

        // Set an absolute last refresh time that will cause the login thread to exit
        // after a certain number of re-logins (by adding an extra half of a refresh interval).
        val absoluteLastRefreshMs = (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                - 1000 * 60 * refreshEveryMinutes / 2)
        // Identify buffer time on either side for the refresh algorithm that will cause
        // the entire lifetime to be taken up. In other words, make sure there is no way to honor the buffers.
        val minPeriodSeconds = (1 + lifetimeMinutes * 60 / 2).toShort()

        // Define some listeners so we can keep track of who gets done and when. All
        // added listeners should end up done except the last, extra one, which should not.
        val mockScheduler = MockScheduler(mockTime)
        val waiters = addWaiters(
            mockScheduler = mockScheduler,
            refreshEveryMillis = 1000 * 60 * refreshEveryMinutes,
            numWaiters = numExpectedRefreshes + 1,
        )

        // Create the ExpiringCredentialRefreshingLogin instance under test
        val testLoginContextFactory = TestLoginContextFactory()
        val testExpiringCredentialRefreshingLogin = object : TestExpiringCredentialRefreshingLogin(
            refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                minPeriodSeconds = minPeriodSeconds,
                bufferSeconds = minPeriodSeconds,
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            ),
            loginContextFactory = testLoginContextFactory,
            time = mockTime,
            lifetimeMillis = 1000 * 60 * lifetimeMinutes,
            absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
            clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
        ) {
            override val createMs: Long = super.createMs - 1000 * 60 * 60 // distant past
        }
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

        // Perform the login, wait up to a certain amount of time for the refresher
        // thread to exit, and make sure the correct calls happened at the correct times
        val expectedFinalMs = startMs + numExpectedRefreshes * 1000 * 60 * refreshEveryMinutes
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
        testExpiringCredentialRefreshingLogin.login()
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
        assertEquals(expectedFinalMs, mockTime.milliseconds())
        for (i in (0 until numExpectedRefreshes)) {
            val waiter = waiters[i]
            assertTrue(waiter.isDone)
            assertEquals((i + 1) * 1000 * 60 * refreshEveryMinutes, waiter.get() - startMs)
        }
        assertFalse(waiters[numExpectedRefreshes].isDone)
        val inOrder = inOrder(mockLoginContext)
        inOrder.verify(mockLoginContext).login()
        for (i in (0 until numExpectedRefreshes)) {
            inOrder.verify(mockLoginContext).login()
            inOrder.verify(mockLoginContext).logout()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRefreshWithMinPeriodIntrusion() {
        val numExpectedRefreshes = 1
        val clientReloginAllowedBeforeLogout = true
        val subject = Subject()
        val mockLoginContext = mock<LoginContext>()
        `when`(mockLoginContext.getSubject()).thenReturn(subject)
        val mockTime = MockTime()
        val startMs = mockTime.milliseconds()

        // Identify the lifetime of each expiring credential
        val lifetimeMinutes = 10L

        // Identify the point at which refresh will occur in that lifetime
        val refreshEveryMinutes = 8L
        // Set an absolute last refresh time that will cause the login thread to exit
        // after a certain number of re-logins (by adding an extra half of a refresh interval).
        val absoluteLastRefreshMs =
            (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                    - 1000 * 60 * refreshEveryMinutes / 2)

        // Identify a minimum period that will cause the refresh time to be delayed a bit.
        val bufferIntrusionSeconds = 1
        val minPeriodSeconds = (refreshEveryMinutes * 60 + bufferIntrusionSeconds).toShort()
        val bufferSeconds = 0.toShort()

        // Define some listeners so we can keep track of who gets done and when. All
        // added listeners should end up done except the last, extra one, which should not.
        val mockScheduler = MockScheduler(mockTime)
        val waiters = addWaiters(
            mockScheduler = mockScheduler,
            refreshEveryMillis = 1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds),
            numWaiters = numExpectedRefreshes + 1,
        )

        // Create the ExpiringCredentialRefreshingLogin instance under test
        val testLoginContextFactory = TestLoginContextFactory()
        val testExpiringCredentialRefreshingLogin = TestExpiringCredentialRefreshingLogin(
            refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                minPeriodSeconds = minPeriodSeconds,
                bufferSeconds = bufferSeconds,
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            ),
            loginContextFactory = testLoginContextFactory,
            time = mockTime,
            lifetimeMillis = 1000 * 60 * lifetimeMinutes,
            absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
            clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
        )
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

        // Perform the login, wait up to a certain amount of time for the refresher
        // thread to exit, and make sure the correct calls happened at the correct times
        val expectedFinalMs =
            (startMs + numExpectedRefreshes * 1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds))
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
        testExpiringCredentialRefreshingLogin.login()
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
        assertEquals(expectedFinalMs, mockTime.milliseconds())

        for (i in (0 until numExpectedRefreshes)) {
            val waiter = waiters[i]
            assertTrue(waiter.isDone)
            assertEquals(
                expected = (i + 1) * 1000 * (60 * refreshEveryMinutes + bufferIntrusionSeconds),
                actual = waiter.get() - startMs,
            )
        }
        assertFalse(waiters[numExpectedRefreshes].isDone)
        val inOrder = inOrder(mockLoginContext)
        inOrder.verify(mockLoginContext).login()

        for (i in (0 until numExpectedRefreshes)) {
            inOrder.verify(mockLoginContext).login()
            inOrder.verify(mockLoginContext).logout()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRefreshWithPreExpirationBufferIntrusion() {
        val numExpectedRefreshes = 1
        val clientReloginAllowedBeforeLogout = true
        val subject = Subject()
        val mockLoginContext = mock<LoginContext>()
        `when`(mockLoginContext.getSubject()).thenReturn(subject)
        val mockTime = MockTime()
        val startMs = mockTime.milliseconds()

        // Identify the lifetime of each expiring credential
        val lifetimeMinutes = 10L

        // Identify the point at which refresh will occur in that lifetime
        val refreshEveryMinutes = 8L

        // Set an absolute last refresh time that will cause the login thread to exit
        // after a certain number of re-logins (by adding an extra half of a refresh interval).
        val absoluteLastRefreshMs =
            (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                    - 1000 * 60 * refreshEveryMinutes / 2)

        // Identify a minimum period that will cause the refresh time to be delayed a bit.
        val bufferIntrusionSeconds = 1
        val bufferSeconds = ((lifetimeMinutes - refreshEveryMinutes) * 60 + bufferIntrusionSeconds).toShort()
        val minPeriodSeconds = 0.toShort()

        // Define some listeners, so we can keep track of who gets done and when. All
        // added listeners should end up done except the last, extra one, which should not.
        val mockScheduler = MockScheduler(mockTime)
        val waiters = addWaiters(
            mockScheduler = mockScheduler,
            refreshEveryMillis = 1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds),
            numWaiters = numExpectedRefreshes + 1,
        )

        // Create the ExpiringCredentialRefreshingLogin instance under test
        val testLoginContextFactory = TestLoginContextFactory()
        val testExpiringCredentialRefreshingLogin = TestExpiringCredentialRefreshingLogin(
            refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                minPeriodSeconds = minPeriodSeconds,
                bufferSeconds = bufferSeconds,
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            ),
            loginContextFactory = testLoginContextFactory,
            time = mockTime,
            lifetimeMillis = 1000 * 60 * lifetimeMinutes,
            absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
            clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
        )
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

        // Perform the login, wait up to a certain amount of time for the refresher
        // thread to exit, and make sure the correct calls happened at the correct times
        val expectedFinalMs =
            (startMs + numExpectedRefreshes * 1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds))
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
        testExpiringCredentialRefreshingLogin.login()
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
        assertEquals(expectedFinalMs, mockTime.milliseconds())

        for (i in (0 until numExpectedRefreshes)) {
            val waiter = waiters[i]
            assertTrue(waiter.isDone)
            assertEquals(
                expected = (i + 1) * 1000 * (60 * refreshEveryMinutes - bufferIntrusionSeconds),
                actual = waiter.get() - startMs
            )
        }
        assertFalse(waiters[numExpectedRefreshes].isDone)
        val inOrder = inOrder(mockLoginContext)
        inOrder.verify(mockLoginContext).login()

        for (i in (0 until numExpectedRefreshes)) {
            inOrder.verify(mockLoginContext).login()
            inOrder.verify(mockLoginContext).logout()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testLoginExceptionCausesCorrectLogout() {
        val numExpectedRefreshes = 3
        val clientReloginAllowedBeforeLogout = true
        val subject = Subject()
        val mockLoginContext = mock<LoginContext>()
        `when`(mockLoginContext.getSubject()).thenReturn(subject)
        doNothing().doThrow(LoginException()).doNothing().`when`(mockLoginContext).login()
        val mockTime = MockTime()
        val startMs = mockTime.milliseconds()

        // Identify the lifetime of each expiring credential
        val lifetimeMinutes = 100L

        // Identify the point at which refresh will occur in that lifetime
        val refreshEveryMinutes = 80L

        // Set an absolute last refresh time that will cause the login thread to exit
        // after a certain number of re-logins (by adding an extra half of a refresh interval).
        val absoluteLastRefreshMs =
            (startMs + (1 + numExpectedRefreshes) * 1000 * 60 * refreshEveryMinutes
                    - 1000 * 60 * refreshEveryMinutes / 2)

        // Identify buffer time on either side for the refresh algorithm
        val minPeriodSeconds = 0.toShort()

        // Create the ExpiringCredentialRefreshingLogin instance under test
        val testLoginContextFactory = TestLoginContextFactory()
        val testExpiringCredentialRefreshingLogin = TestExpiringCredentialRefreshingLogin(
            refreshConfig = refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
                refreshWindowFactor = 1.0 * refreshEveryMinutes / lifetimeMinutes,
                minPeriodSeconds = minPeriodSeconds,
                bufferSeconds = minPeriodSeconds,
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            ),
            loginContextFactory = testLoginContextFactory,
            time = mockTime,
            lifetimeMillis = 1000 * 60 * lifetimeMinutes,
            absoluteLastRefreshTimeMs = absoluteLastRefreshMs,
            clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
        )
        testLoginContextFactory.configure(mockLoginContext, testExpiringCredentialRefreshingLogin)

        /* Perform the login and wait up to a certain amount of time for the refresher
         * thread to exit.  A timeout indicates the thread died due to logout()
         * being invoked on an instance where the login() invocation had failed.
         */
        assertFalse(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        assertFalse(testLoginContextFactory.refresherThreadDoneFuture().isDone)
        testExpiringCredentialRefreshingLogin.login()
        assertTrue(testLoginContextFactory.refresherThreadStartedFuture().isDone)
        testLoginContextFactory.refresherThreadDoneFuture()[1L, TimeUnit.SECONDS]
    }

    companion object {
        private val EMPTY_WILDCARD_CONFIGURATION: Configuration = object : Configuration() {
            override fun getAppConfigurationEntry(name: String): Array<AppConfigurationEntry> =
                emptyArray() // match any name
        }

        private fun addWaiters(
            mockScheduler: MockScheduler,
            refreshEveryMillis: Long,
            numWaiters: Int,
        ): List<KafkaFutureImpl<Long>> {
            val retvalWaiters: MutableList<KafkaFutureImpl<Long>> = ArrayList(numWaiters)
            for (i in (1..numWaiters)) {
                val waiter = KafkaFutureImpl<Long>()
                mockScheduler.addWaiter(i * refreshEveryMillis, waiter)
                retvalWaiters.add(waiter)
            }
            return retvalWaiters
        }

        private fun refreshConfigThatPerformsReloginEveryGivenPercentageOfLifetime(
            refreshWindowFactor: Double,
            minPeriodSeconds: Short,
            bufferSeconds: Short,
            clientReloginAllowedBeforeLogout: Boolean,
        ): ExpiringCredentialRefreshConfig {
            val configs = mapOf(
                SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR to refreshWindowFactor,
                SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER to 0,
                SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS to minPeriodSeconds,
                SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS to bufferSeconds,
            )
            return ExpiringCredentialRefreshConfig(
                configs = ConfigDef().withClientSaslSupport().parse(configs),
                clientReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout,
            )
        }
    }
}
