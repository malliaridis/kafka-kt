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

import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.jose4j.http.SimpleResponse
import org.jose4j.jwk.HttpsJwks
import org.junit.jupiter.api.Test
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RefreshingHttpsJwksTest : OAuthBearerTest() {
    
    /**
     * Test that a key not previously scheduled for refresh will be scheduled without a refresh.
     */
    @Test
    @Throws(Exception::class)
    fun testBasicScheduleRefresh() {
        val keyId = "abc123"
        val time: Time = MockTime()
        val httpsJwks = spyHttpsJwks()
        getRefreshingHttpsJwks(time, httpsJwks).use { refreshingHttpsJwks ->
            refreshingHttpsJwks.init()
            verify(httpsJwks, times(numInvocations = 1)).refresh()
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
            verify(httpsJwks, times(numInvocations = 1)).refresh()
        }
    }

    /**
     * Test that a key previously scheduled for refresh will **not** be scheduled a second time
     * if it's requested right away.
     */
    @Test
    @Throws(Exception::class)
    fun testMaybeExpediteRefreshNoDelay() {
        val keyId = "abc123"
        val time: Time = MockTime()
        val httpsJwks = spyHttpsJwks()
        getRefreshingHttpsJwks(time, httpsJwks).use { refreshingHttpsJwks ->
            refreshingHttpsJwks.init()
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
        }
    }

    /**
     * Test that a key previously scheduled for refresh **will** be scheduled a second time
     * if it's requested after the delay.
     */
    @Test
    @Throws(Exception::class)
    fun testMaybeExpediteRefreshDelays() {
        assertMaybeExpediteRefreshWithDelay(
            sleepDelay = RefreshingHttpsJwks.MISSING_KEY_ID_CACHE_IN_FLIGHT_MS - 1,
            shouldBeScheduled = false,
        )
        assertMaybeExpediteRefreshWithDelay(
            sleepDelay = RefreshingHttpsJwks.MISSING_KEY_ID_CACHE_IN_FLIGHT_MS,
            shouldBeScheduled = true,
        )
        assertMaybeExpediteRefreshWithDelay(
            sleepDelay = RefreshingHttpsJwks.MISSING_KEY_ID_CACHE_IN_FLIGHT_MS + 1,
            shouldBeScheduled = true,
        )
    }

    /**
     * Test that a "long key" will not be looked up because the key ID is too long.
     */
    @Test
    @Throws(Exception::class)
    fun testLongKey() {
        val keyIdChars = CharArray(RefreshingHttpsJwks.MISSING_KEY_ID_MAX_KEY_LENGTH + 1) { '0' }
        val keyId = String(keyIdChars)
        val time: Time = MockTime()
        val httpsJwks = spyHttpsJwks()
        getRefreshingHttpsJwks(time, httpsJwks).use { refreshingHttpsJwks ->
            refreshingHttpsJwks.init()
            verify(httpsJwks, times(1)).refresh()
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
            verify(httpsJwks, times(1)).refresh()
        }
    }

    /**
     * Test that if we ask to load a missing key, and then we wait past the sleep time that it will
     * call refresh to load the key.
     */
    @Test
    @Throws(Exception::class)
    fun testSecondaryRefreshAfterElapsedDelay() {
        val keyId = "abc123"
        val time: Time = Time.SYSTEM // Unfortunately, we can't mock time here because the
        // scheduled executor doesn't respect it.
        val httpsJwks = spyHttpsJwks()
        getRefreshingHttpsJwks(time, httpsJwks).use { refreshingHttpsJwks ->
            refreshingHttpsJwks.init()
            verify(httpsJwks, times(1)).refresh()
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
            time.sleep((REFRESH_MS + 1).toLong())
            verify(httpsJwks, times(3)).refresh()
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
        }
    }

    @Throws(Exception::class)
    private fun assertMaybeExpediteRefreshWithDelay(sleepDelay: Long, shouldBeScheduled: Boolean) {
        val keyId = "abc123"
        val time: Time = MockTime()
        val httpsJwks = spyHttpsJwks()
        getRefreshingHttpsJwks(time, httpsJwks).use { refreshingHttpsJwks ->
            refreshingHttpsJwks.init()
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId))
            time.sleep(sleepDelay)
            assertEquals(shouldBeScheduled, refreshingHttpsJwks.maybeExpediteRefresh(keyId))
        }
    }

    private fun getRefreshingHttpsJwks(time: Time, httpsJwks: HttpsJwks): RefreshingHttpsJwks {
        return RefreshingHttpsJwks(
            time = time,
            httpsJwks = httpsJwks,
            refreshMs = REFRESH_MS.toLong(),
            refreshRetryBackoffMs = RETRY_BACKOFF_MS.toLong(),
            refreshRetryBackoffMaxMs = RETRY_BACKOFF_MAX_MS.toLong(),
        )
    }

    /**
     * We *spy* (not *mock*) the [HttpsJwks] instance because we want to have it
     * _partially mocked_ to determine if it's calling its internal refresh method. We want to
     * make sure it *doesn't* do that when we call our getJsonWebKeys() method on
     * [RefreshingHttpsJwks].
     */
    private fun spyHttpsJwks(): HttpsJwks {
        val httpsJwks = HttpsJwks("https://www.example.com")
        val simpleResponse: SimpleResponse = object : SimpleResponse {
            override fun getStatusCode(): Int = 200

            override fun getStatusMessage(): String = "OK"

            override fun getHeaderNames(): Collection<String> = emptyList()

            override fun getHeaderValues(name: String): List<String> = emptyList()

            override fun getBody(): String = "{\"keys\": []}"
        }
        httpsJwks.setSimpleHttpGet { simpleResponse }
        return spy(httpsJwks)
    }

    companion object {

        private const val REFRESH_MS = 5000

        private const val RETRY_BACKOFF_MS = 50

        private const val RETRY_BACKOFF_MAX_MS = 2000
    }
}
