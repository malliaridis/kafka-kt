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

import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.kafka.common.utils.Time
import org.jose4j.jwk.HttpsJwks
import org.jose4j.jwk.JsonWebKey
import org.jose4j.lang.JoseException
import org.slf4j.LoggerFactory

/**
 * Implementation of [HttpsJwks] that will periodically refresh the JWKS cache to reduce or even
 * prevent HTTP/HTTPS traffic in the hot path of validation. It is assumed that it's possible to
 * receive a JWT that contains a `kid` that points to yet-unknown JWK, thus requiring a connection
 * to the OAuth/OIDC provider to be made. Hopefully, in practice, keys are made available for some
 * amount of time before they're used within JWTs.
 *
 * This instance is created and provided to the
 * [org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver] that is used when using an
 * HTTP-/HTTPS-based [org.jose4j.keys.resolvers.VerificationKeyResolver], which is then provided to
 * the [ValidatorAccessTokenValidator] to use in validating the signature of a JWT.
 *
 * @constructor Creates a `RefreshingHttpsJwks` that will be used by the
 * [RefreshingHttpsJwksVerificationKeyResolver] to resolve new key IDs in JWTs.
 * @property time [Time] instance
 * @property httpsJwks [HttpsJwks] instance from which to retrieve the JWKS based on the OAuth/OIDC
 * standard
 * @property refreshMs The number of milliseconds between refresh passes to connect to the
 * OAuth/OIDC JWKS endpoint to retrieve the latest set
 * @property refreshRetryBackoffMs Time for delay after initial failed attempt to retrieve JWKS
 * @property refreshRetryBackoffMaxMs Maximum time to retrieve JWKS
 *
 * @see org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver
 * @see org.jose4j.keys.resolvers.VerificationKeyResolver
 * @see ValidatorAccessTokenValidator
 */
class RefreshingHttpsJwks(
    private val time: Time,
    private val httpsJwks: HttpsJwks,
    private val refreshMs: Long,
    private val refreshRetryBackoffMs: Long,
    private val refreshRetryBackoffMaxMs: Long,
) : Initable, Closeable {

    /*
     * [HttpsJwks] does the actual work of contacting the OAuth/OIDC endpoint to get the JWKS. In
     * some cases, the call to [HttpsJwks.getJsonWebKeys] will trigger a call to [HttpsJwks.refresh]
     * which will block the current thread in network I/O. We cache the JWKS ourselves (see
     * [jsonWebKeys]) to avoid the network I/O.
     *
     * We want to be very careful where we use the [HttpsJwks] instance so that we don't perform any
     * operation (directly or indirectly) that could cause blocking. This is because the JWKS logic
     * is part of the larger authentication logic which operates on Kafka's network thread. It's OK
     * to execute [HttpsJwks.getJsonWebKeys] (which calls [HttpsJwks.refresh]) from within [init] as
     * that method is called only at startup, and we can afford the blocking hit there.
     */

    private val executorService = Executors.newSingleThreadScheduledExecutor()

    /**
     * Protects [missingKeyIds] and [jsonWebKeys].
     */
    private val refreshLock: ReadWriteLock = ReentrantReadWriteLock()

    private val missingKeyIds = object : LinkedHashMap<String, Long>(
        MISSING_KEY_ID_CACHE_MAX_ENTRIES, .75f, true
    ) {
        override fun removeEldestEntry(eldest: Map.Entry<String, Long>): Boolean {
            return this.size > MISSING_KEY_ID_CACHE_MAX_ENTRIES
        }
    }

    /**
     * Flag to prevent concurrent refresh invocations.
     */
    private val refreshInProgressFlag = AtomicBoolean(false)

    /**
     * As mentioned in the comments for [httpsJwks], we cache the JWKS ourselves so that
     * we can return the list immediately without any network I/O. They are only cached within
     * calls to [refresh].
     */
    private var jsonWebKeys: List<JsonWebKey>? = null

    private var isInitialized = false

    init {
        require(refreshMs > 0) {
            "JWKS validation key refresh configuration value retryWaitMs value must be positive"
        }
    }

    @Throws(IOException::class)
    override fun init() {
        try {
            log.debug("init started")
            val localJWKs: List<JsonWebKey> = try {
                httpsJwks.jsonWebKeys
            } catch (e: JoseException) {
                throw IOException("Could not refresh JWKS", e)
            }
            jsonWebKeys = try {
                refreshLock.writeLock().lock()
                Collections.unmodifiableList(localJWKs)
            } finally {
                refreshLock.writeLock().unlock()
            }

            // Since we just grabbed the keys (which will have invoked a HttpsJwks.refresh()
            // internally), we can delay our first invocation by refreshMs.
            //
            // Note: we refer to this as a _scheduled_ refresh.
            executorService.scheduleAtFixedRate(
                { refresh() },
                refreshMs,
                refreshMs,
                TimeUnit.MILLISECONDS
            )
            log.info(
                "JWKS validation key refresh thread started with a refresh interval of {} ms",
                refreshMs
            )
        } finally {
            isInitialized = true
            log.debug("init completed")
        }
    }

    override fun close() {
        try {
            log.debug("close started")
            try {
                log.debug("JWKS validation key refresh thread shutting down")
                executorService.shutdown()
                if (!executorService.awaitTermination(
                        SHUTDOWN_TIMEOUT.toLong(),
                        SHUTDOWN_TIME_UNIT
                    )
                ) {
                    log.warn(
                        "JWKS validation key refresh thread termination did not end after {} {}",
                        SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT
                    )
                }
            } catch (e: InterruptedException) {
                log.warn("JWKS validation key refresh thread error during close", e)
            }
        } finally {
            log.debug("close completed")
        }
    }

    /**
     * Our implementation avoids the blocking call within [HttpsJwks.refresh] that is
     * sometimes called internal to [HttpsJwks.getJsonWebKeys]. We want to avoid any
     * blocking I/O as this code is running in the authentication path on the Kafka network thread.
     *
     * The list may be stale up to [refreshMs].
     *
     * @return [List] of [JsonWebKey] instances
     *
     * @throws JoseException Thrown if a problem is encountered parsing the JSON content into JWKs
     * @throws IOException Thrown f a problem is encountered making the HTTP request
     */
    @Throws(JoseException::class, IOException::class)
    fun getJsonWebKeys(): List<JsonWebKey>? {
        check(isInitialized) { "Please call init() first" }
        return try {
            refreshLock.readLock().lock()
            jsonWebKeys
        } finally {
            refreshLock.readLock().unlock()
        }
    }

    val location: String
        get() = httpsJwks.location

    /**
     * `refresh` is an internal method that will refresh the JWKS cache and is invoked in one of two
     * ways:
     * 1. Scheduled
     * 2. Expedited
     *
     * The *scheduled* refresh is scheduled in [init] and runs every [refreshMs] milliseconds. An
     * *expedited* refresh is performed when an incoming JWT refers to a key ID that isn't in our
     * JWKS cache ([jsonWebKeys]) and we try to perform a refresh sooner than the next scheduled
     * refresh.
     */
    private fun refresh() {
        if (!refreshInProgressFlag.compareAndSet(false, true)) {
            log.debug("OAuth JWKS refresh is already in progress; ignoring concurrent refresh")
            return
        }

        try {
            log.info("OAuth JWKS refresh of {} starting", httpsJwks.location)
            val retry: Retry<List<JsonWebKey>> = Retry(
                retryBackoffMs = refreshRetryBackoffMs,
                retryBackoffMaxMs = refreshRetryBackoffMaxMs,
            )
            val localJWKs = retry.execute(Retryable {
                try {
                    log.debug(
                        "JWKS validation key calling refresh of {} starting",
                        httpsJwks.location
                    )

                    // Call the *actual* refresh implementation that will more than likely issue
                    // HTTP(S) calls over the network.
                    httpsJwks.refresh()
                    val jwks = httpsJwks.jsonWebKeys

                    log.debug(
                        "JWKS validation key refresh of {} complete",
                        httpsJwks.location
                    )

                    return@Retryable jwks
                } catch (e: Exception) {
                    throw ExecutionException(e)
                }
            })
            jsonWebKeys = try {
                refreshLock.writeLock().lock()
                for (jwk in localJWKs) missingKeyIds.remove(jwk.keyId)
                Collections.unmodifiableList(localJWKs)
            } finally {
                refreshLock.writeLock().unlock()
            }
            log.info("OAuth JWKS refresh of {} complete", httpsJwks.location)
        } catch (e: ExecutionException) {
            log.warn(
                "OAuth JWKS refresh of {} encountered an error; not updating local JWKS cache",
                httpsJwks.location,
                e
            )
        } finally {
            refreshInProgressFlag.set(false)
        }
    }

    /**
     * `maybeExpediteRefresh` is a public method that will trigger a refresh of the JWKS cache if
     * all of the following conditions are met:
     *
     * - The given `keyId` parameter is  the [MISSING_KEY_ID_MAX_KEY_LENGTH]
     * - The key isn't in the process of being expedited already
     *
     * This *expedited* refresh is scheduled immediately.
     *
     * @param keyId JWT key ID
     * @return `true` if an expedited refresh was scheduled, `false` otherwise
     */
    fun maybeExpediteRefresh(keyId: String): Boolean {
        return if (keyId.length > MISSING_KEY_ID_MAX_KEY_LENGTH) {

            // Although there's no limit on the length of the key ID, they're generally
            // "reasonably" short. If we have a very long key ID length, we're going to assume
            // the JWT is malformed, and we will not actually try to resolve the key.
            //
            // In this case, let's prevent blowing out our memory in two ways:
            //
            //     1. Don't try to resolve the key as the large ID will sit in our cache
            //     2. Report the issue in the logs but include only the first N characters
            val actualLength = keyId.length
            val s = keyId.substring(0, MISSING_KEY_ID_MAX_KEY_LENGTH)
            val snippet = String.format(
                "%s (trimmed to first %s characters out of %s total)",
                s,
                MISSING_KEY_ID_MAX_KEY_LENGTH,
                actualLength
            )
            log.warn("Key ID {} was too long to cache", snippet)
            false
        } else {
            try {
                refreshLock.writeLock().lock()
                var nextCheckTime = missingKeyIds[keyId]
                val currTime = time.milliseconds()

                log.debug(
                    "For key ID {}, nextCheckTime: {}, currTime: {}",
                    keyId,
                    nextCheckTime,
                    currTime
                )

                if (nextCheckTime == null || nextCheckTime <= currTime) {
                    // If there's no entry in the missing key ID cache for the incoming key ID,
                    // or it has expired, schedule a refresh ASAP.
                    nextCheckTime = currTime + MISSING_KEY_ID_CACHE_IN_FLIGHT_MS
                    missingKeyIds[keyId] = nextCheckTime
                    executorService.schedule({ refresh() }, 0, TimeUnit.MILLISECONDS)
                    true
                } else false
            } finally {
                refreshLock.writeLock().unlock()
            }
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(RefreshingHttpsJwks::class.java)

        private const val MISSING_KEY_ID_CACHE_MAX_ENTRIES = 16

        const val MISSING_KEY_ID_CACHE_IN_FLIGHT_MS: Long = 60000

        const val MISSING_KEY_ID_MAX_KEY_LENGTH = 1000

        private const val SHUTDOWN_TIMEOUT = 10

        private val SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS
    }
}
