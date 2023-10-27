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

package org.apache.kafka.clients

import org.apache.kafka.clients.ClientUtils.filterPreferredAddresses
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.ClientUtils.resolve
import org.apache.kafka.common.config.ConfigException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class ClientUtilsTest {
    private val hostResolver: HostResolver = DefaultHostResolver()
    @Test
    fun testParseAndValidateAddresses() {
        checkWithoutLookup("127.0.0.1:8000")
        checkWithoutLookup("localhost:8080")
        checkWithoutLookup("[::1]:8000")
        checkWithoutLookup("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000")
        val validatedAddresses = checkWithoutLookup("localhost:10000")
        assertEquals(expected = 1, actual = validatedAddresses.size)
        val onlyAddress = validatedAddresses[0]
        assertEquals(expected = "localhost", actual = onlyAddress.hostName)
        assertEquals(expected = 10000, actual = onlyAddress.port)
    }

    @Test
    fun testParseAndValidateAddressesWithReverseLookup() {
        checkWithoutLookup("127.0.0.1:8000")
        checkWithoutLookup("localhost:8080")
        checkWithoutLookup("[::1]:8000")
        checkWithoutLookup("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000")

        // With lookup of example.com, either one or two addresses are expected depending on
        // whether ipv4 and ipv6 are enabled
        val validatedAddresses = checkWithLookup(mutableListOf("example.com:10000"))
        assertTrue(
            actual = validatedAddresses.isNotEmpty(),
            message = "Unexpected addresses $validatedAddresses"
        )
        val validatedHostNames = validatedAddresses.map(InetSocketAddress::getHostName)
        val expectedHostNames = listOf("93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946")
        assertTrue(
            actual = expectedHostNames.containsAll(validatedHostNames),
            message = "Unexpected addresses $validatedHostNames"
        )
        validatedAddresses.forEach { address ->
            assertEquals(expected = 10000, actual = address.port)
        }
    }

    @Test
    fun testInvalidConfig() {
        assertFailsWith<IllegalArgumentException> {
            parseAndValidateAddresses(listOf("localhost:10000"), "random.value")
        }
    }

    @Test
    fun testNoPort() {
        assertFailsWith<ConfigException> { checkWithoutLookup("127.0.0.1") }
    }

    @Test
    fun testOnlyBadHostname() {
        assertFailsWith<ConfigException> {
            checkWithoutLookup("some.invalid.hostname.foo.bar.local:9999")
        }
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testFilterPreferredAddresses() {
        val ipv4 = InetAddress.getByName("192.0.0.1")
        val ipv6 = InetAddress.getByName("::1")
        val ipv4First = arrayOf(ipv4, ipv6, ipv4)
        var result = filterPreferredAddresses(ipv4First)
        assertTrue(result.contains(ipv4))
        assertFalse(result.contains(ipv6))
        assertEquals(expected = 2, actual = result.size)
        val ipv6First = arrayOf(ipv6, ipv4, ipv4)
        result = filterPreferredAddresses(ipv6First)
        assertTrue(result.contains(ipv6))
        assertFalse(result.contains(ipv4))
        assertEquals(expected = 1, actual = result.size)
    }

    @Test
    fun testResolveUnknownHostException() {
        assertFailsWith<UnknownHostException> {
            resolve(host = "some.invalid.hostname.foo.bar.local", hostResolver = hostResolver)
        }
    }

    @Test
    @Throws(UnknownHostException::class)
    fun testResolveDnsLookup() {
        val addresses = arrayOf(
            InetAddress.getByName("198.51.100.0"),
            InetAddress.getByName("198.51.100.5"),
        )
        val hostResolver = AddressChangeHostResolver(addresses, addresses)
        assertEquals(
            expected = addresses.toList(),
            actual = resolve("kafka.apache.org", hostResolver)
        )
    }

    private fun checkWithoutLookup(vararg url: String): List<InetSocketAddress> {
        return parseAndValidateAddresses(url.toList(), ClientDnsLookup.USE_ALL_DNS_IPS)
    }

    private fun checkWithLookup(url: List<String>): List<InetSocketAddress> {
        return parseAndValidateAddresses(
            urls = url,
            clientDnsLookup = ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY,
        )
    }
}
