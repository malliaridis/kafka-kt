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

import java.util.*
import javax.security.auth.Subject
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

/**
 * This class is responsible for refreshing logins for both Kafka client and server when the login
 * is a type that has a limited lifetime/will expire. The credentials for the login must implement
 * [ExpiringCredential].
 */
abstract class ExpiringCredentialRefreshingLogin(
    val contextName: String,
    val configuration: Configuration,
    private val expiringCredentialRefreshConfig: ExpiringCredentialRefreshConfig,
    val callbackHandler: AuthenticateCallbackHandler,
    private val mandatoryClassToSynchronizeOnPriorToRefresh: Class<*>,
    private val loginContextFactory: LoginContextFactory = LoginContextFactory(),
    private val time: Time = Time.SYSTEM,
) : AutoCloseable {

    private var refresherThread: Thread? = null

    // mark volatile due to existence of public visibility
    @Volatile
    var subject: Subject? = null

    private var hasExpiringCredential = false

    private var principalName: String? = null

    private var loginContext: LoginContext? = null

    private var expiringCredential: ExpiringCredential? = null

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("subject")
    )
    fun subject(): Subject? = subject // field requires volatile keyword

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("contextName")
    )
    fun contextName(): String = contextName

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("configuration")
    )
    fun configuration(): Configuration = configuration

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("callbackHandler")
    )
    fun callbackHandler(): AuthenticateCallbackHandler = callbackHandler

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("serviceName")
    )
    fun serviceName(): String = "kafka"

    val serviceName = "kafka"

    /**
     * Performs login for each login module specified for the login context of this instance and
     * starts the thread used to periodically re-login.
     *
     * The synchronized keyword is not necessary because an implementation of [Login] will delegate
     * to this code (e.g. OAuthBearerRefreshingLogin), and the [login] method on the delegating
     * class will itself be synchronized if necessary.
     */
    @Throws(LoginException::class)
    fun login(): LoginContext? {
        val tmpLoginContext = loginContextFactory.createLoginContext(this)
        tmpLoginContext.login()
        log.info("Successfully logged in.")
        loginContext = tmpLoginContext
        subject = tmpLoginContext.subject
        expiringCredential = expiringCredential()
        hasExpiringCredential = expiringCredential != null

        val expiringCredential = expiringCredential ?: run {
            // do not bother with re-logins.
            log.debug("No Expiring Credential")
            principalName = null
            refresherThread = null
            return loginContext
        }

        principalName = expiringCredential.principalName()

        // Check for a clock skew problem
        val expireTimeMs = expiringCredential.expireTimeMs()
        val nowMs = currentMs()
        if (nowMs > expireTimeMs) {
            log.error(
                "[Principal={}]: Current clock: {} is later than expiry {}. This may indicate a " +
                        "clock skew problem. Check that this host's and remote host's clocks are " +
                        "in sync. Not starting refresh thread. This process is likely unable to " +
                        "authenticate SASL connections (for example, it is unlikely to be able to " +
                        "authenticate a connection with a Kafka Broker).",
                principalLogText(),
                Date(nowMs),
                Date(expireTimeMs),
            )
            return loginContext
        }

        if (log.isDebugEnabled) log.debug(
            "[Principal={}]: It is an expiring credential",
            principalLogText(),
        )

        // Re-login periodically. How often is determined by the expiration date of the credential
        // and refresh-related configuration values.
        refresherThread = KafkaThread.daemon(
            String.format("kafka-expiring-relogin-thread-%s", principalName),
            Refresher(),
        ).also { it.start() }

        loginContextFactory.refresherThreadStarted()
        return loginContext
    }

    override fun close() {
        val refresherThread = refresherThread ?: return

        if (refresherThread.isAlive) {
            refresherThread.interrupt()
            try {
                refresherThread.join()
            } catch (exception: InterruptedException) {
                log.warn(
                    "[Principal={}]: Interrupted while waiting for re-login thread to shutdown.",
                    principalLogText(),
                    exception,
                )
                Thread.currentThread().interrupt()
            }
        }
    }

    abstract fun expiringCredential(): ExpiringCredential?

    /**
     * Determine when to sleep until before performing a refresh.
     *
     * @param relativeToMs the point (in terms of number of milliseconds since the epoch) at which
     * to perform the calculation
     * @return `null` if no refresh should occur, otherwise the time to sleep until (in terms of the
     * number of milliseconds since the epoch) before performing a refresh.
     */
    private fun refreshMs(relativeToMs: Long): Long? {
        val expiringCredential = expiringCredential ?: run {
            // Re-login failed because our login() invocation did not generate a credential but also
            // did not generate an exception. Try logging in again after some delay (it seems likely
            // to be a bug, but it doesn't hurt to keep trying to refresh).
            val retvalNextRefreshMs =
                relativeToMs + DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS * 1000L

            log.warn(
                "[Principal={}]: No Expiring credential found: will try again at {}",
                principalLogText(),
                Date(retvalNextRefreshMs),
            )
            return retvalNextRefreshMs
        }

        val expireTimeMs = expiringCredential.expireTimeMs()
        if (relativeToMs > expireTimeMs) {
            return if (isLogoutRequiredBeforeLoggingBackIn) {
                log.error(
                    "[Principal={}]: Current clock: {} is later than expiry {}. This may " +
                            "indicate a clock skew problem. Check that this host's and remote " +
                            "host's clocks are in sync. Exiting refresh thread.",
                    principalLogText(),
                    Date(relativeToMs),
                    Date(expireTimeMs),
                )
                null
            } else {
                // Since the current soon-to-expire credential isn't logged out until we have a new
                // credential with a refreshed lifetime, it is possible that the current credential
                // could expire if the re-login continually fails over and over again making us
                // unable to get the new credential. Therefore keep trying rather than exiting.
                val retvalNextRefreshMs =
                    relativeToMs + DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS * 1000L
                log.warn(
                    "[Principal={}]: Expiring credential already expired at {}: will try to " +
                            "refresh again at {}",
                    principalLogText(),
                    Date(expireTimeMs),
                    Date(retvalNextRefreshMs),
                )
                retvalNextRefreshMs
            }
        }

        val absoluteLastRefreshTimeMs = expiringCredential.absoluteLastRefreshTimeMs()
        if (absoluteLastRefreshTimeMs != null && absoluteLastRefreshTimeMs < expireTimeMs) {
            log.warn(
                ("[Principal={}]: Expiring credential refresh thread exiting because the " +
                        "expiring credential's current expiration time ({}) exceeds the latest " +
                        "possible refresh time ({}). This process will not be able to " +
                        "authenticate new SASL connections after that time (for example, it will " +
                        "not be able to authenticate a new connection with a Kafka Broker)."),
                principalLogText(),
                Date(expireTimeMs),
                Date(absoluteLastRefreshTimeMs),
            )
            return null
        }

        val optionalStartTime = expiringCredential.startTimeMs()
        val startMs = optionalStartTime ?: relativeToMs

        log.info(
            "[Principal={}]: Expiring credential valid from {} to {}",
            expiringCredential.principalName(),
            Date(startMs),
            Date(expireTimeMs)
        )
        val pct = expiringCredentialRefreshConfig.loginRefreshWindowFactor +
                (expiringCredentialRefreshConfig.loginRefreshWindowJitter * RNG.nextDouble())

        // Ignore buffer times if the credential's remaining lifetime is less than their sum.
        val refreshMinPeriodSeconds =
            expiringCredentialRefreshConfig.loginRefreshMinPeriodSeconds.toLong()

        val clientRefreshBufferSeconds =
            expiringCredentialRefreshConfig.loginRefreshBufferSeconds.toLong()

        if (relativeToMs + 1000L * (refreshMinPeriodSeconds + clientRefreshBufferSeconds) > expireTimeMs) {
            val retvalRefreshMs = relativeToMs + ((expireTimeMs - relativeToMs) * pct).toLong()
            log.warn(
                "[Principal={}]: Expiring credential expires at {}, so buffer times of {} and {} " +
                        "seconds at the front and back, respectively, cannot be accommodated. We " +
                        "will refresh at {}.",
                principalLogText(),
                Date(expireTimeMs),
                refreshMinPeriodSeconds,
                clientRefreshBufferSeconds,
                Date(retvalRefreshMs),
            )
            return retvalRefreshMs
        }

        val proposedRefreshMs = startMs + ((expireTimeMs - startMs) * pct).toLong()
        // Don't let it violate the requested end buffer time
        val beginningOfEndBufferTimeMs = expireTimeMs - clientRefreshBufferSeconds * 1000

        if (proposedRefreshMs > beginningOfEndBufferTimeMs) {
            log.info(
                "[Principal={}]: Proposed refresh time of {} extends into the desired buffer " +
                        "time of {} seconds before expiration, so refresh it at the desired " +
                        "buffer begin point, at {}",
                expiringCredential.principalName(),
                Date(proposedRefreshMs),
                clientRefreshBufferSeconds,
                Date(beginningOfEndBufferTimeMs),
            )
            return beginningOfEndBufferTimeMs
        }

        // Don't let it violate the minimum refresh period
        val endOfMinRefreshBufferTime = relativeToMs + 1000 * refreshMinPeriodSeconds
        if (proposedRefreshMs < endOfMinRefreshBufferTime) {
            log.info(
                "[Principal={}]: Expiring credential re-login thread time adjusted from {} to {} " +
                        "since the former is sooner than the minimum refresh interval ({} " +
                        "seconds from now).",
                principalLogText(),
                Date(proposedRefreshMs),
                Date(endOfMinRefreshBufferTime),
                refreshMinPeriodSeconds,
            )
            return endOfMinRefreshBufferTime
        }
        // Proposed refresh time doesn't violate any constraints
        return proposedRefreshMs
    }

    @Throws(
        LoginException::class,
        ExitRefresherThreadDueToIllegalStateException::class
    )
    private fun reLogin() = synchronized(mandatoryClassToSynchronizeOnPriorToRefresh) {

        // Only perform one refresh of a particular type at a time
        val logoutRequiredBeforeLoggingBackIn: Boolean = isLogoutRequiredBeforeLoggingBackIn

        if (hasExpiringCredential && logoutRequiredBeforeLoggingBackIn) {
            val principalLogTextPriorToLogout: String? = principalLogText()
            log.info(
                "Initiating logout for {}",
                principalLogTextPriorToLogout
            )
            loginContext!!.logout()

            // Make absolutely sure we were logged out
            val expiringCredential = expiringCredential()
            this.expiringCredential = expiringCredential
            hasExpiringCredential = expiringCredential != null

            if (expiringCredential != null)
            // We can't force the removal because we don't know how to do it, so abort
                throw ExitRefresherThreadDueToIllegalStateException(
                    String.format(
                        "Subject's private credentials still contains an instance of %s even " +
                                "though logout() was invoked; exiting refresh thread",
                        expiringCredential.javaClass.name
                    )
                )
        }

        // Perform a login, making note of any credential that might need a logout() afterward
        val optionalCredentialToLogout: ExpiringCredential? = expiringCredential
        val optionalLoginContextToLogout: LoginContext? = loginContext
        var cleanLogin = false // remember to restore the original if necessary
        try {
            loginContext = loginContextFactory.createLoginContext(
                expiringCredentialRefreshingLogin = this@ExpiringCredentialRefreshingLogin
            ).also {
                log.info(
                    "Initiating re-login for {}, logout() still needs to be called on a previous " +
                            "login = {}",
                    principalName,
                    optionalCredentialToLogout != null
                )
                it.login()
            }

            cleanLogin = true // no need to restore the original

            // Perform a logout() on any original credential if necessary
            if (optionalCredentialToLogout != null) optionalLoginContextToLogout!!.logout()
        } finally {
            if (!cleanLogin) loginContext = optionalLoginContextToLogout // restore the original
        }

        // Get the new credential and make sure it is not any old one that required a  logout()
        // after the login()
        val expiringCredential = expiringCredential()
        this.expiringCredential = expiringCredential
        hasExpiringCredential = expiringCredential != null

        if (expiringCredential == null) {
            // Re-login has failed because our login() invocation has not generated a credential but
            // has also not generated an exception. We won't exit here; instead we will allow login
            // retries in case we can somehow fix the issue (it seems likely to be a bug, but it
            // doesn't hurt to keep trying to refresh).
            log.error("No Expiring Credential after a supposedly-successful re-login")
            principalName = null
        } else {
            if (expiringCredential === optionalCredentialToLogout)
                // The login() didn't identify a new credential; we still have the old one. We don't
                // know how to fix this, so abort.
                throw ExitRefresherThreadDueToIllegalStateException(
                String.format(
                    "Subject's private credentials still contains the previous, soon-to-expire " +
                            "instance of %s even though login() followed by logout() was " +
                            "invoked; exiting refresh thread",
                    expiringCredential.javaClass.name
                )
            )

            principalName = expiringCredential.principalName()
            if (log.isDebugEnabled) log.debug(
                "[Principal={}]: It is an expiring credential after re-login as expected",
                principalLogText()
            )
        }
    }

    private fun principalLogText(): String? =
        expiringCredential?.let { it.javaClass.simpleName + ":" + principalName } ?: principalName

    private fun currentMs(): Long = time.milliseconds()

    private val isLogoutRequiredBeforeLoggingBackIn: Boolean
        get() = !expiringCredentialRefreshConfig.loginRefreshReloginAllowedBeforeLogout

    /**
     * Class that can be overridden for testing.
     */
    open class LoginContextFactory internal constructor() {

        @Throws(LoginException::class)
        fun createLoginContext(
            expiringCredentialRefreshingLogin: ExpiringCredentialRefreshingLogin,
        ): LoginContext {
            return with(expiringCredentialRefreshingLogin) {
                LoginContext(contextName, subject, callbackHandler, configuration)
            }
        }

        open fun refresherThreadStarted() = Unit
        open fun refresherThreadDone() = Unit
    }

    private class ExitRefresherThreadDueToIllegalStateException(
        message: String?,
    ) : Exception(message) {

        companion object {
            private const val serialVersionUID = -6108495378411920380L
        }
    }

    private inner class Refresher : Runnable {

        override fun run() {
            log.info(
                "[Principal={}]: Expiring credential re-login thread started.",
                principalLogText(),
            )
            while (true) {

                // Refresh thread's main loop. Each expiring credential lives for one iteration
                // of the loop. Thread will exit if the loop exits from here.
                val nowMs = currentMs()
                var nextRefreshMs = refreshMs(nowMs)
                if (nextRefreshMs == null) {
                    loginContextFactory.refresherThreadDone()
                    return
                }

                // safety check motivated by KAFKA-7945,
                // should generally never happen except due to a bug
                if (nextRefreshMs < nowMs) {
                    log.warn(
                        "[Principal={}]: Expiring credential re-login sleep time was calculated " +
                                "to be in the past! Will explicitly adjust. ({})",
                        principalLogText(),
                        Date(nextRefreshMs)
                    )
                    // refresh in 10 seconds
                    nextRefreshMs = nowMs + 10 * 1000
                }
                log.info(
                    "[Principal={}]: Expiring credential re-login sleeping until: {}",
                    principalLogText(),
                    Date(nextRefreshMs)
                )
                time.sleep(nextRefreshMs - nowMs)

                if (Thread.currentThread().isInterrupted) {
                    log.info(
                        "[Principal={}]: Expiring credential re-login thread has been " +
                                "interrupted and will exit.",
                        principalLogText(),
                    )
                    loginContextFactory.refresherThreadDone()
                    return
                }
                while (true) {
                    // Perform a re-login over and over again with some intervening delay
                    // unless/until either the refresh succeeds or we are interrupted.
                    try {
                        reLogin()
                        break // success
                    } catch (e: ExitRefresherThreadDueToIllegalStateException) {
                        log.error(e.message, e)
                        loginContextFactory.refresherThreadDone()
                        return
                    } catch (loginException: LoginException) {
                        log.warn(
                            String.format(
                                "[Principal=%s]: LoginException during login retry; will sleep %d " +
                                        "seconds before trying again.",
                                principalLogText(),
                                DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS,
                            ),
                            loginException,
                        )
                        // Sleep and allow loop to run/try again unless interrupted
                        time.sleep(DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS * 1000)
                        if (Thread.currentThread().isInterrupted) {
                            log.error(
                                "[Principal={}]: Interrupted while trying to perform a subsequent " +
                                        "expiring credential re-login after one or more initial " +
                                        "re-login failures: re-login thread exiting now: {}",
                                principalLogText(),
                                loginException.message.toString(),
                            )
                            loginContextFactory.refresherThreadDone()
                            return
                        }
                    }
                }
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(ExpiringCredentialRefreshingLogin::class.java)

        private const val DELAY_SECONDS_BEFORE_NEXT_RETRY_WHEN_RELOGIN_FAILS = 10L

        private val RNG = Random()
    }
}
