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

package org.apache.kafka.common.security.kerberors

import java.util.*
import javax.security.auth.Subject
import javax.security.auth.kerberos.KerberosTicket
import javax.security.auth.login.Configuration
import javax.security.auth.login.LoginContext
import javax.security.auth.login.LoginException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.authenticator.AbstractLogin
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.Shell
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

/**
 * This class is responsible for refreshing Kerberos credentials for logins for both Kafka client
 * and server.
 */
class KerberosLogin : AbstractLogin() {

    private val time = Time.SYSTEM

    private var thread: Thread? = null

    private var isKrbTicket = false

    private var isUsingTicketCache = false

    private var principal: String? = null

    // LoginThread will sleep until 80% of time from last refresh to ticket's expiry has been
    // reached, at which time it will wake and try to renew the ticket.
    private var ticketRenewWindowFactor = 0.0

    /**
     * Percentage of random jitter added to the renewal time
     */
    private var ticketRenewJitter = 0.0

    // Regardless of ticketRenewWindowFactor setting above and the ticket expiry time,
    // thread will not sleep between refresh attempts any less than 1 minute (60*1000 milliseconds = 1 minute).
    // Change the '1' to e.g. 5, to change this to 5 minutes.
    private var minTimeBeforeRelogin: Long = 0

    private var kinitCmd: String? = null

    @Volatile
    private var subject: Subject? = null

    private lateinit var loginContext: LoginContext

    private var serviceName: String? = null

    private var lastLogin: Long = 0

    override fun configure(
        configs: Map<String, *>,
        contextName: String,
        jaasConfiguration: Configuration,
        loginCallbackHandler: AuthenticateCallbackHandler,
    ) {
        super.configure(configs, contextName, jaasConfiguration, loginCallbackHandler)

        ticketRenewWindowFactor =
            configs[SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR] as Double

        ticketRenewJitter = configs[SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER] as Double

        minTimeBeforeRelogin = configs[SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN] as Long

        kinitCmd = configs[SaslConfigs.SASL_KERBEROS_KINIT_CMD] as String?
        serviceName = getServiceName(configs, contextName, jaasConfiguration)
    }

    /**
     * Performs login for each login module specified for the login context of this instance and
     * starts the thread used to periodically re-login to the Kerberos Ticket Granting Server.
     */
    @Throws(LoginException::class)
    override fun login(): LoginContext {
        lastLogin = currentElapsedTime()
        loginContext = super.login()
        subject = loginContext.subject
        isKrbTicket = subject!!.getPrivateCredentials(KerberosTicket::class.java).isNotEmpty()

        val entries = configuration()!!.getAppConfigurationEntry(contextName())

        val entry = entries.firstOrNull()
        isUsingTicketCache = entry?.options?.get("useTicketCache") as String? == "true"
        principal = entry?.options?.get("principal") as String?

        if (!isKrbTicket) {
            log.debug("[Principal={}]: It is not a Kerberos ticket", principal)
            thread = null
            // if no TGT, do not bother with ticket management.
            return loginContext
        }

        log.debug("[Principal={}]: It is a Kerberos ticket", principal)

        // Refresh the Ticket Granting Ticket (TGT) periodically. How often to refresh is determined
        // by the TGT's existing expiry date and the configured minTimeBeforeRelogin. For testing
        // and development, you can decrease the interval of expiration of tickets (for example, to
        // 3 minutes) by running: "modprinc -maxlife 3mins <principal>" in kadmin.
        thread = KafkaThread.daemon(String.format("kafka-kerberos-refresh-thread-%s", principal)) {
            log.info(
                "[Principal={}]: TGT refresh thread started.",
                principal
            )
            // renewal thread's main loop. if it exits from here, thread will exit.
            while (true) {
                val tgt = tGT
                val now = currentWallTime()
                var nextRefresh: Long
                var nextRefreshDate: Date

                if (tgt == null) {
                    nextRefresh = now + minTimeBeforeRelogin
                    nextRefreshDate = Date(nextRefresh)
                    log.warn(
                        "[Principal={}]: No TGT found: will try again at {}",
                        principal,
                        nextRefreshDate
                    )
                } else {
                    nextRefresh = getRefreshTime(tgt)
                    val expiry = tgt.endTime.time
                    val expiryDate = Date(expiry)
                    if (isUsingTicketCache && tgt.renewTill != null && tgt.renewTill.time < expiry) {
                        log.warn(
                            "The TGT cannot be renewed beyond the next expiry date: {}." +
                                    "This process will not be able to authenticate new SASL " +
                                    "connections after that time (for example, it will not be " +
                                    "able to authenticate a new connection with a Kafka Broker). " +
                                    "Ask your system administrator to either increase the " +
                                    "'renew until' time by doing : 'modprinc -maxrenewlife {} ' " +
                                    "within kadmin, or instead, to generate a keytab for {}. " +
                                    "Because the TGT's expiry cannot be further extended by " +
                                    "refreshing, exiting refresh thread now.",
                            expiryDate,
                            principal,
                            principal,
                        )
                        return@daemon
                    }

                    // Determine how long to sleep from looking at ticket's expiry. We should not
                    // allow the ticket to expire, but we should take into consideration
                    // minTimeBeforeRelogin. Will not sleep less than minTimeBeforeRelogin, unless
                    // doing so would cause ticket expiration.
                    nextRefresh = if (nextRefresh > expiry || minTimeBeforeRelogin > expiry - now) {
                        // expiry is before next scheduled refresh).
                        log.info(
                            "[Principal={}]: Refreshing now because expiry is before next scheduled refresh time.",
                            principal
                        )
                        now
                    } else {
                        if (nextRefresh - now < minTimeBeforeRelogin) {
                            // next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
                            val until = Date(nextRefresh)
                            val newUntil =
                                Date(now + minTimeBeforeRelogin)
                            log.warn(
                                "[Principal={}]: TGT refresh thread time adjusted from {} to {} " +
                                        "since the former is sooner than the minimum refresh " +
                                        "interval ({} seconds) from now.",
                                principal,
                                until,
                                newUntil,
                                minTimeBeforeRelogin / 1000
                            )
                        }
                        nextRefresh.coerceAtLeast(now + minTimeBeforeRelogin)
                    }
                    nextRefreshDate = Date(nextRefresh)
                    if (nextRefresh > expiry) {
                        log.error(
                            "[Principal={}]: Next refresh: {} is later than expiry {}. This may " +
                                    "indicate a clock skew problem. Check that this host and the " +
                                    "KDC hosts' clocks are in sync. Exiting refresh thread.",
                            principal,
                            nextRefreshDate,
                            expiryDate
                        )
                        return@daemon
                    }
                }
                if (now < nextRefresh) {
                    val until = Date(nextRefresh)
                    log.info(
                        "[Principal={}]: TGT refresh sleeping until: {}",
                        principal,
                        until
                    )
                    try {
                        Thread.sleep(nextRefresh - now)
                    } catch (ie: InterruptedException) {
                        log.warn(
                            "[Principal={}]: TGT renewal thread has been interrupted and will exit.",
                            principal
                        )
                        return@daemon
                    }
                } else {
                    log.error(
                        "[Principal={}]: NextRefresh: {} is in the past: exiting refresh thread. " +
                                "Check clock sync between this host and KDC - (KDC's clock is " +
                                "likely ahead of this host). Manual intervention will be required " +
                                "for this client to successfully authenticate. Exiting refresh " +
                                "thread.",
                        principal,
                        nextRefreshDate
                    )
                    return@daemon
                }
                if (isUsingTicketCache) {
                    val kinitArgs = "-R"
                    var retry = 1
                    while (retry >= 0) {
                        try {
                            log.debug(
                                "[Principal={}]: Running ticket cache refresh command: {} {}",
                                principal,
                                kinitCmd,
                                kinitArgs
                            )
                            Shell.execCommand(kinitCmd, kinitArgs)
                            break
                        } catch (e: Exception) {
                            if (retry > 0) {
                                log.warn(
                                    "[Principal={}]: Error when trying to renew with TicketCache, " +
                                            "but will retry ",
                                    principal,
                                    e
                                )
                                retry = retry.dec()
                                // sleep for 10 seconds
                                try {
                                    Thread.sleep((10 * 1000).toLong())
                                } catch (ie: InterruptedException) {
                                    log.error(
                                        "[Principal={}]: Interrupted while renewing TGT, exiting " +
                                                "Login thread",
                                        principal
                                    )
                                    return@daemon
                                }
                            } else {
                                log.warn(
                                    "[Principal={}]: Could not renew TGT due to problem running " +
                                            "shell command: '{} {}'. Exiting refresh thread.",
                                    principal,
                                    kinitCmd,
                                    kinitArgs,
                                    e
                                )
                                return@daemon
                            }
                        }
                    }
                }
                try {
                    var retry = 1
                    while (retry >= 0) {
                        try {
                            reLogin()
                            break
                        } catch (le: LoginException) {
                            if (retry > 0) {
                                log.warn(
                                    "[Principal={}]: Error when trying to re-Login, but will retry ",
                                    principal,
                                    le
                                )
                                retry = retry.dec()
                                // sleep for 10 seconds.
                                try {
                                    Thread.sleep((10 * 1000).toLong())
                                } catch (e: InterruptedException) {
                                    log.error(
                                        "[Principal={}]: Interrupted during login retry after " +
                                                "LoginException:",
                                        principal,
                                        le
                                    )
                                    throw le
                                }
                            } else log.error(
                                "[Principal={}]: Could not refresh TGT.",
                                principal,
                                le
                            )
                        }
                    }
                } catch (le: LoginException) {
                    log.error(
                        "[Principal={}]: Failed to refresh TGT: refresh thread exiting now.",
                        principal,
                        le
                    )
                    return@daemon
                }
            }
        }
        thread!!.start()
        return loginContext
    }

    override fun close() {
        if (thread != null && thread!!.isAlive) {
            thread!!.interrupt()
            try {
                thread!!.join()
            } catch (e: InterruptedException) {
                log.warn(
                    "[Principal={}]: Error while waiting for Login thread to shutdown.",
                    principal,
                    e
                )
                Thread.currentThread().interrupt()
            }
        }
    }

    override fun subject(): Subject? = subject

    override fun serviceName(): String {
        return serviceName!!
    }

    private fun getRefreshTime(tgt: KerberosTicket): Long {
        val start = tgt.startTime.time
        val expires = tgt.endTime.time
        log.info("[Principal={}]: TGT valid starting at: {}", principal, tgt.startTime)
        log.info("[Principal={}]: TGT expires: {}", principal, tgt.endTime)
        val proposedRefresh = start + ((expires - start) *
                (ticketRenewWindowFactor + ticketRenewJitter * RNG.nextDouble())).toLong()
        return if (proposedRefresh > expires)
            // proposedRefresh is too far in the future: it's after ticket expires: simply return
            // now / current time.
            currentWallTime()
        else proposedRefresh
    }

    private val tGT: KerberosTicket?
        get() {
            val tickets = subject!!.getPrivateCredentials(KerberosTicket::class.java)

            tickets.forEach { ticket ->
                val server = ticket.server
                if (server.name == "krbtgt/${server.realm}@${server.realm}") {
                    log.debug(
                        "Found TGT with client principal '{}' and server principal '{}'.",
                        ticket.client.name,
                        ticket.server.name
                    )
                    return ticket
                }
            }
            return null
        }

    private fun hasSufficientTimeElapsed(): Boolean {
        val now = currentElapsedTime()
        if (now - lastLogin < minTimeBeforeRelogin) {
            log.warn(
                "[Principal={}]: Not attempting to re-login since the last re-login was attempted " +
                        "less than {} seconds before.",
                principal,
                minTimeBeforeRelogin / 1000
            )
            return false
        }
        return true
    }

    /**
     * Re-login a principal. This method assumes that [.login] has happened already.
     *
     * @throws javax.security.auth.login.LoginException on a failure
     */
    @Throws(LoginException::class)
    internal fun reLogin() {
        if (!isKrbTicket) return
        if (::loginContext.isInitialized) throw LoginException("Login must be done first")
        if (!hasSufficientTimeElapsed()) return

        synchronized(KerberosLogin::class.java) {
            log.info("Initiating logout for {}", principal)
            // register most recent relogin attempt
            lastLogin = currentElapsedTime()

            // clear up the kerberos state. But the tokens are not cleared! As per the Java kerberos
            // login module code, only the kerberos credentials are cleared. If previous logout
            // succeeded but login failed, we shouldn't logout again since duplicate logout causes
            // NPE from Java 9 onwards.
            if (subject != null && subject!!.principals.isNotEmpty()) logout()

            // login and also update the subject field of this instance to have the new credentials
            // (pass it to the LoginContext constructor)
            loginContext = LoginContext(
                contextName(),
                subject,
                null,
                configuration()
            )
            log.info("Initiating re-login for {}", principal)
            login(loginContext)
        }
    }

    // Visibility to override for testing
    @Throws(LoginException::class)
    internal fun login(loginContext: LoginContext) = loginContext.login()

    // Visibility to override for testing
    @Throws(LoginException::class)
    internal fun logout() = loginContext.logout()

    private fun currentElapsedTime(): Long = time.hiResClockMs()

    private fun currentWallTime(): Long = time.milliseconds()

    companion object {

        private val log = LoggerFactory.getLogger(KerberosLogin::class.java)

        private val RNG = Random()

        private fun getServiceName(
            configs: Map<String, *>,
            contextName: String,
            configuration: Configuration
        ): String {
            val configEntries = configuration.getAppConfigurationEntry(contextName).toList()
            val jaasServiceName = JaasContext.configEntryOption(
                configurationEntries = configEntries,
                key = JaasUtils.SERVICE_NAME,
            )

            val configServiceName = configs[SaslConfigs.SASL_KERBEROS_SERVICE_NAME] as String?

            require(
                jaasServiceName == null
                        || configServiceName == null
                        || jaasServiceName == configServiceName
            ) {
                String.format(
                    "Conflicting serviceName values found in JAAS and Kafka configs " +
                            "value in JAAS file %s, value in Kafka config %s",
                    jaasServiceName,
                    configServiceName
                )
            }

            return requireNotNull(jaasServiceName ?: configServiceName) {
                "No serviceName defined in either JAAS or Kafka config"
            }
        }
    }
}
