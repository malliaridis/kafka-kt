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

import org.apache.kafka.common.config.SaslConfigs

/**
 * Immutable refresh-related configuration for expiring credentials that can be
 * parsed from a producer/consumer/broker config.
 *
 * Constructor based on producer/consumer/broker configs and the indicated value for whether or not
 * client relogin is allowed before logout.
 *
 * @param configs the mandatory (but possibly empty) producer/consumer/broker configs upon which to
 * build this instance.
 * @param clientReloginAllowedBeforeLogout if the `LoginModule` and `SaslClient` implementations
 * support multiple simultaneous login contexts on a single `Subject` at the same time. If `true`,
 * then upon refresh, logout will only be invoked on the original `LoginContext` after a new one
 * successfully logs in. This can be helpful if the original credential still has some lifetime left
 * when an attempt to refresh the credential fails; the client will still be able to create new
 * connections as long as the original credential remains valid. Otherwise, if logout is immediately
 * invoked prior to re-login, a re-login failure leaves the client without the ability to connect
 * until re-login does in fact succeed.
 */
class ExpiringCredentialRefreshConfig(
    configs: Map<String, *>,
    clientReloginAllowedBeforeLogout: Boolean,
) {

    /**
     * The login refresh window factor.
     *
     * Background login refresh thread will sleep until the specified window factor relative to the
     * credential's total lifetime has been reached, at which time it will try to refresh the
     * credential.
     */
    val loginRefreshWindowFactor: Double

    /**
     * Amount of random jitter added to the background login refresh thread's sleep time.
     */
    val loginRefreshWindowJitter: Double

    /**
     * The desired minimum time between checks by the background login refresh thread, in seconds.
     */
    val loginRefreshMinPeriodSeconds: Short

    /**
     * The amount of buffer time before expiration to maintain when refreshing, in seconds. If a
     * refresh is scheduled to occur closer to expiration than the number of seconds defined here
     * then the refresh will be moved up to maintain as much of the desired buffer as possible.
     */
    val loginRefreshBufferSeconds: Short

    /**
     * Whether the re-login is allowed prior to discarding an existing (presumably unexpired)
     * credential.
     *
     * If the LoginModule and SaslClient implementations support multiple simultaneous login
     * contexts on a single Subject at the same time. If true, then upon refresh, logout will only
     * be invoked on the original LoginContext after a new one successfully logs in. This can be
     * helpful if the original credential still has some lifetime left when an attempt to refresh
     * the credential fails; the client will still be able to create new connections as long as the
     * original credential remains valid. Otherwise, if logout is immediately invoked prior to
     * re-login, a re-login failure leaves the client without the ability to connect until re-login
     * does in fact succeed.
     */
    val loginRefreshReloginAllowedBeforeLogout: Boolean

    init {
        loginRefreshWindowFactor = configs[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] as Double
        loginRefreshWindowJitter = configs[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] as Double
        loginRefreshMinPeriodSeconds =
            configs[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] as Short
        loginRefreshBufferSeconds = configs[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] as Short
        loginRefreshReloginAllowedBeforeLogout = clientReloginAllowedBeforeLogout
    }

    /**
     * Background login refresh thread will sleep until the specified window factor
     * relative to the credential's total lifetime has been reached, at which time
     * it will try to refresh the credential.
     *
     * @return the login refresh window factor
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("loginRefreshWindowFactor")
    )
    fun loginRefreshWindowFactor(): Double = loginRefreshWindowFactor

    /**
     * Amount of random jitter added to the background login refresh thread's sleep
     * time.
     *
     * @return the login refresh window jitter
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("loginRefreshWindowJitter")
    )
    fun loginRefreshWindowJitter(): Double = loginRefreshWindowJitter

    /**
     * The desired minimum time between checks by the background login refresh
     * thread, in seconds
     *
     * @return the desired minimum refresh period, in seconds
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("loginRefreshMinPeriodSeconds")
    )
    fun loginRefreshMinPeriodSeconds(): Short = loginRefreshMinPeriodSeconds

    /**
     * The amount of buffer time before expiration to maintain when refreshing. If a
     * refresh is scheduled to occur closer to expiration than the number of seconds
     * defined here then the refresh will be moved up to maintain as much of the
     * desired buffer as possible.
     *
     * @return the refresh buffer, in seconds
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("loginRefreshBufferSeconds")
    )
    fun loginRefreshBufferSeconds(): Short = loginRefreshBufferSeconds

    /**
     * If the LoginModule and SaslClient implementations support multiple
     * simultaneous login contexts on a single Subject at the same time. If true,
     * then upon refresh, logout will only be invoked on the original LoginContext
     * after a new one successfully logs in. This can be helpful if the original
     * credential still has some lifetime left when an attempt to refresh the
     * credential fails; the client will still be able to create new connections as
     * long as the original credential remains valid. Otherwise, if logout is
     * immediately invoked prior to relogin, a relogin failure leaves the client
     * without the ability to connect until relogin does in fact succeed.
     *
     * @return true if relogin is allowed prior to discarding an existing
     * (presumably unexpired) credential, otherwise false
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("loginRefreshReloginAllowedBeforeLogout")
    )
    fun loginRefreshReloginAllowedBeforeLogout(): Boolean = loginRefreshReloginAllowedBeforeLogout
}
