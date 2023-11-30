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

package org.apache.kafka.common.security.auth

import java.net.InetAddress
import java.security.Principal
import javax.net.ssl.SSLSession
import javax.security.auth.x500.X500Principal
import javax.security.sasl.SaslServer
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.ssl.SslPrincipalMapper
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals

class DefaultKafkaPrincipalBuilderTest {

    @Test
    @Throws(Exception::class)
    fun testReturnAnonymousPrincipalForPlaintext() {
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )
        assertEquals(
            expected = KafkaPrincipal.ANONYMOUS,
            actual = builder.build(
                PlaintextAuthenticationContext(
                    clientAddress = InetAddress.getLocalHost(),
                    listenerName = SecurityProtocol.PLAINTEXT.name,
                )
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun testUseSessionPeerPrincipalForSsl() {
        val session = mock<SSLSession>()
        whenever(session.peerPrincipal).thenReturn(DummyPrincipal("foo"))
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )
        val principal = builder.build(
            SslAuthenticationContext(
                session = session,
                clientAddress = InetAddress.getLocalHost(),
                listenerName = SecurityProtocol.PLAINTEXT.name,
            )
        )
        assertEquals(KafkaPrincipal.USER_TYPE, principal.principalType)
        assertEquals("foo", principal.getName())
        verify(session, atLeastOnce()).peerPrincipal
    }

    @Test
    @Throws(Exception::class)
    fun testPrincipalIfSSLPeerIsNotAuthenticated() {
        val session = mock<SSLSession>()
        whenever(session.peerPrincipal).thenReturn(KafkaPrincipal.ANONYMOUS)
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )
        val principal = builder.build(
            SslAuthenticationContext(
                session = session,
                clientAddress = InetAddress.getLocalHost(),
                listenerName = SecurityProtocol.PLAINTEXT.name,
            )
        )
        assertEquals(KafkaPrincipal.ANONYMOUS, principal)
        verify(session, atLeastOnce()).peerPrincipal
    }

    @Test
    @Throws(Exception::class)
    fun testPrincipalWithSslPrincipalMapper() {
        val session = mock<SSLSession>()
        whenever(session.peerPrincipal)
            .thenReturn(X500Principal("CN=Duke, OU=ServiceUsers, O=Org, C=US"))
            .thenReturn(X500Principal("CN=Duke, OU=SME, O=mycp, L=Fulton, ST=MD, C=US"))
            .thenReturn(X500Principal("CN=duke, OU=JavaSoft, O=Sun Microsystems"))
            .thenReturn(X500Principal("OU=JavaSoft, O=Sun Microsystems, C=US"))
        val rules = java.lang.String.join(
            ", ",
            "RULE:^CN=(.*),OU=ServiceUsers.*$/$1/L",
            "RULE:^CN=(.*),OU=(.*),O=(.*),L=(.*),ST=(.*),C=(.*)$/$1@$2/L",
            "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U",
            "DEFAULT"
        )
        val mapper = SslPrincipalMapper.fromRules(rules)
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = mapper,
        )
        val sslContext = SslAuthenticationContext(
            session = session,
            clientAddress = InetAddress.getLocalHost(),
            listenerName = SecurityProtocol.PLAINTEXT.name,
        )
        var principal = builder.build(sslContext)
        assertEquals("duke", principal.getName())
        principal = builder.build(sslContext)
        assertEquals("duke@sme", principal.getName())
        principal = builder.build(sslContext)
        assertEquals("DUKE", principal.getName())
        principal = builder.build(sslContext)
        assertEquals("OU=JavaSoft,O=Sun Microsystems,C=US", principal.getName())
        verify(session, times(4)).peerPrincipal
    }

    @Test
    @Throws(Exception::class)
    fun testPrincipalBuilderScram() {
        val server = mock<SaslServer>()
        whenever(server.mechanismName).thenReturn(ScramMechanism.SCRAM_SHA_256.mechanismName)
        whenever(server.authorizationID).thenReturn("foo")
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = null,
            sslPrincipalMapper = null,
        )
        val principal = builder.build(
            SaslAuthenticationContext(
                server = server,
                securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
                clientAddress = InetAddress.getLocalHost(),
                listenerName = SecurityProtocol.SASL_PLAINTEXT.name,
            )
        )
        assertEquals(KafkaPrincipal.USER_TYPE, principal.principalType)
        assertEquals("foo", principal.getName())
        verify(server, atLeastOnce()).mechanismName
        verify(server, atLeastOnce()).authorizationID
    }

    @Test
    @Throws(Exception::class)
    fun testPrincipalBuilderGssapi() {
        val server = mock<SaslServer>()
        val kerberosShortNamer = mock<KerberosShortNamer>()
        whenever(server.mechanismName).thenReturn(SaslConfigs.GSSAPI_MECHANISM)
        whenever(server.authorizationID).thenReturn("foo/host@REALM.COM")
        whenever(kerberosShortNamer.shortName(any())).thenReturn("foo")
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = kerberosShortNamer,
            sslPrincipalMapper = null,
        )
        val principal = builder.build(
            SaslAuthenticationContext(
                server = server,
                securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
                clientAddress = InetAddress.getLocalHost(),
                listenerName = SecurityProtocol.SASL_PLAINTEXT.name,
            )
        )
        assertEquals(KafkaPrincipal.USER_TYPE, principal.principalType)
        assertEquals("foo", principal.getName())
        verify(server, atLeastOnce()).mechanismName
        verify(server, atLeastOnce()).authorizationID
        verify(kerberosShortNamer, atLeastOnce()).shortName(any())
    }

    @Test
    @Throws(Exception::class)
    fun testPrincipalBuilderSerde() {
        val server = mock<SaslServer>()
        val kerberosShortNamer = mock<KerberosShortNamer>()
        whenever(server.mechanismName).thenReturn(SaslConfigs.GSSAPI_MECHANISM)
        whenever(server.authorizationID).thenReturn("foo/host@REALM.COM")
        whenever(kerberosShortNamer.shortName(any())).thenReturn("foo")
        val builder = DefaultKafkaPrincipalBuilder(
            kerberosShortNamer = kerberosShortNamer,
            sslPrincipalMapper = null,
        )
        val principal = builder.build(
            SaslAuthenticationContext(
                server = server,
                securityProtocol = SecurityProtocol.SASL_PLAINTEXT,
                clientAddress = InetAddress.getLocalHost(),
                listenerName = SecurityProtocol.SASL_PLAINTEXT.name,
            )
        )
        assertEquals(KafkaPrincipal.USER_TYPE, principal.principalType)
        assertEquals("foo", principal.getName())
        val serializedPrincipal = builder.serialize(principal)
        val deserializedPrincipal = builder.deserialize(serializedPrincipal)
        assertEquals(principal, deserializedPrincipal)
        verify(server, atLeastOnce()).mechanismName
        verify(server, atLeastOnce()).authorizationID
        verify(kerberosShortNamer, atLeastOnce()).shortName(any())
    }

    private class DummyPrincipal(private val name: String) : Principal {
        override fun getName(): String = name
    }
}
