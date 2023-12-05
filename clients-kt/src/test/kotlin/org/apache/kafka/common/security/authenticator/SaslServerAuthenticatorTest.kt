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

package org.apache.kafka.common.security.authenticator

import java.io.IOException
import java.net.InetAddress
import java.nio.Buffer
import java.nio.ByteBuffer
import java.time.Duration
import javax.security.auth.Subject
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.network.ChannelBuilders
import org.apache.kafka.common.network.ChannelBuilders.createPrincipalBuilder
import org.apache.kafka.common.network.ChannelMetadataRegistry
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.DefaultChannelMetadataRegistry
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.RequestTestUtils.serializeRequestHeader
import org.apache.kafka.common.requests.ResponseHeader
import org.apache.kafka.common.requests.SaslAuthenticateRequest
import org.apache.kafka.common.requests.SaslAuthenticateResponse
import org.apache.kafka.common.requests.SaslHandshakeRequest
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.ssl.SslPrincipalMapper
import org.apache.kafka.common.utils.AppInfoParser.version
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import org.mockito.Answers
import org.mockito.ArgumentCaptor
import org.mockito.MockedStatic
import org.mockito.Mockito.mockStatic
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.stubbing.Answer
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SaslServerAuthenticatorTest {
    
    private val clientId = "clientId"
    
    @Test
    @Throws(IOException::class)
    fun testOversizeRequest() {
        val transportLayer = mock<TransportLayer>()
        val configs = mapOf(
            BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG to listOf(ScramMechanism.SCRAM_SHA_256.mechanismName),
        )
        val authenticator = setupAuthenticator(
            configs = configs,
            transportLayer = transportLayer,
            mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName,
            metadataRegistry = DefaultChannelMetadataRegistry()
        )
        whenever(transportLayer.read(any<ByteBuffer>())).then { invocation ->
            invocation.getArgument<ByteBuffer>(0)
                .putInt(BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE + 1)
            4
        }
        assertFailsWith<SaslAuthenticationException> { authenticator.authenticate() }
        verify(transportLayer).read(any<ByteBuffer>())
    }

    @Test
    @Throws(IOException::class)
    fun testUnexpectedRequestType() {
        val transportLayer = mock<TransportLayer>()
        val configs = mapOf(
            BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG to listOf(ScramMechanism.SCRAM_SHA_256.mechanismName),
        )
        val authenticator = setupAuthenticator(
            configs = configs,
            transportLayer = transportLayer,
            mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName,
            metadataRegistry = DefaultChannelMetadataRegistry(),
        )
        val header = RequestHeader(
            requestApiKey = ApiKeys.METADATA,
            requestVersion = 0.toShort(),
            clientId = clientId,
            correlationId = 13243,
        )
        val headerBuffer = serializeRequestHeader(header)
        whenever(transportLayer.read(any<ByteBuffer>())).then { invocation ->
            invocation.getArgument<ByteBuffer>(0).putInt(headerBuffer.remaining())
            4
        }.then { invocation ->
            // serialize only the request header. the authenticator should not parse beyond this
            invocation.getArgument<ByteBuffer>(0).put(headerBuffer.duplicate())
            headerBuffer.remaining()
        }
        assertFailsWith<IllegalSaslStateException>(
            message = "Expected authenticate() to raise an exception",
        ) { authenticator.authenticate() }
        verify(transportLayer, times(numInvocations = 2)).read(any<ByteBuffer>())
    }

    @Test
    @Throws(IOException::class)
    fun testOldestApiVersionsRequest() = testApiVersionsRequest(
        version = ApiKeys.API_VERSIONS.oldestVersion(),
        expectedSoftwareName = ClientInformation.UNKNOWN_NAME_OR_VERSION,
        expectedSoftwareVersion = ClientInformation.UNKNOWN_NAME_OR_VERSION,
    )

    @Test
    @Throws(IOException::class)
    fun testLatestApiVersionsRequest() = testApiVersionsRequest(
        version = ApiKeys.API_VERSIONS.latestVersion(),
        expectedSoftwareName = "apache-kafka-java",
        expectedSoftwareVersion = version,
    )

    @Test
    @Throws(IOException::class)
    fun testSessionExpiresAtTokenExpiryDespiteNoReauthIsSet() {
        val mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM
        val tokenExpirationDuration = Duration.ofSeconds(1)
        val saslServer = mock<SaslServer>()
        val time = MockTime()
        mockSaslServer(
            saslServer = saslServer,
            mechanism = mechanism,
            time = time,
            tokenExpirationDuration = tokenExpirationDuration,
        ).use {
            mockKafkaPrincipal("[principal-type]", "[principal-name").use {
                mockTransportLayer().use { transportLayer ->
                    val authenticator = getSaslServerAuthenticatorForOAuth(
                        mechanism = mechanism,
                        transportLayer = transportLayer,
                        time = time,
                        maxReauth = 0L,
                    )
                    mockRequest(saslHandshakeRequest(mechanism), transportLayer)
                    authenticator.authenticate()

                    whenever(saslServer.isComplete).thenReturn(false).thenReturn(true)
                    mockRequest(saslAuthenticateRequest(), transportLayer)
                    authenticator.authenticate()

                    val atTokenExpiryNanos = time.nanoseconds() + tokenExpirationDuration.toNanos()
                    assertEquals(atTokenExpiryNanos, authenticator.serverSessionExpirationTimeNanos())

                    val secondResponseSent = getResponses(transportLayer)[1]
                    consumeSizeAndHeader(secondResponseSent)

                    val response = SaslAuthenticateResponse.parse(buffer = secondResponseSent, version = 2)
                    assertEquals(tokenExpirationDuration.toMillis(), response.sessionLifetimeMs())
                }
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testSessionExpiresAtMaxReauthTime() {
        val mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM
        val saslServer = mock<SaslServer>()
        val time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 1,
            currentHighResTimeNs = 1000,
        )
        val maxReauthMs = 100L
        val tokenExpiryGreaterThanMaxReauth = Duration.ofMillis(maxReauthMs).multipliedBy(10)
        mockSaslServer(
            saslServer = saslServer,
            mechanism = mechanism,
            time = time,
            tokenExpirationDuration = tokenExpiryGreaterThanMaxReauth,
        ).use {
            mockKafkaPrincipal(
                principalType = "[principal-type]",
                name = "[principal-name",
            ).use {
                mockTransportLayer().use { transportLayer ->
                    val authenticator = getSaslServerAuthenticatorForOAuth(
                        mechanism = mechanism,
                        transportLayer = transportLayer,
                        time = time,
                        maxReauth = maxReauthMs,
                    )
                    mockRequest(saslHandshakeRequest(mechanism), transportLayer)
                    authenticator.authenticate()

                    whenever(saslServer.isComplete).thenReturn(false).thenReturn(true)
                    mockRequest(saslAuthenticateRequest(), transportLayer)
                    authenticator.authenticate()

                    val atMaxReauthNanos = time.nanoseconds() + Duration.ofMillis(maxReauthMs).toNanos()
                    assertEquals(atMaxReauthNanos, authenticator.serverSessionExpirationTimeNanos())

                    val secondResponseSent = getResponses(transportLayer)[1]
                    consumeSizeAndHeader(secondResponseSent)

                    val response = SaslAuthenticateResponse.parse(buffer = secondResponseSent, version = 2)
                    assertEquals(maxReauthMs, response.sessionLifetimeMs())
                }
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testSessionExpiresAtTokenExpiry() {
        val mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM
        val saslServer = mock<SaslServer>()
        val time = MockTime(
            autoTickMs = 0,
            currentTimeMs = 1,
            currentHighResTimeNs = 1000,
        )
        val tokenExpiryShorterThanMaxReauth = Duration.ofSeconds(2)
        val maxReauthMs = tokenExpiryShorterThanMaxReauth.multipliedBy(2).toMillis()
        mockSaslServer(
            saslServer = saslServer,
            mechanism = mechanism,
            time = time,
            tokenExpirationDuration = tokenExpiryShorterThanMaxReauth,
        ).use {
            mockKafkaPrincipal(
                principalType = "[principal-type]",
                name = "[principal-name",
            ).use {
                mockTransportLayer().use { transportLayer ->
                    val authenticator = getSaslServerAuthenticatorForOAuth(
                        mechanism = mechanism,
                        transportLayer = transportLayer,
                        time = time,
                        maxReauth = maxReauthMs,
                    )
                    mockRequest(saslHandshakeRequest(mechanism), transportLayer)
                    authenticator.authenticate()

                    whenever(saslServer.isComplete).thenReturn(false).thenReturn(true)
                    mockRequest(saslAuthenticateRequest(), transportLayer)
                    authenticator.authenticate()

                    val atTokenExpiryNanos = time.nanoseconds() + tokenExpiryShorterThanMaxReauth.toNanos()
                    assertEquals(atTokenExpiryNanos, authenticator.serverSessionExpirationTimeNanos())

                    val secondResponseSent = getResponses(transportLayer)[1]
                    consumeSizeAndHeader(secondResponseSent)

                    val response = SaslAuthenticateResponse.parse(buffer = secondResponseSent, version = 2)
                    assertEquals(tokenExpiryShorterThanMaxReauth.toMillis(), response.sessionLifetimeMs())
                }
            }
        }
    }

    private fun getSaslServerAuthenticatorForOAuth(
        mechanism: String,
        transportLayer: TransportLayer,
        time: Time,
        maxReauth: Long,
    ): SaslServerAuthenticator {
        val configs = mapOf(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG to listOf(mechanism))
        val metadataRegistry = DefaultChannelMetadataRegistry()
        return setupAuthenticator(
            configs = configs,
            transportLayer = transportLayer,
            mechanism = mechanism,
            metadataRegistry = metadataRegistry,
            time = time,
            maxReauth = maxReauth,
        )
    }

    @Throws(SaslException::class)
    private fun mockSaslServer(
        saslServer: SaslServer,
        mechanism: String,
        time: Time,
        tokenExpirationDuration: Duration,
    ): MockedStatic<*> {
        whenever(saslServer.mechanismName).thenReturn(mechanism)
        whenever(saslServer.evaluateResponse(any())).thenReturn(byteArrayOf())
        val millisToExpiration = tokenExpirationDuration.toMillis()
        whenever(saslServer.getNegotiatedProperty(eq(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY)))
            .thenReturn(time.milliseconds() + millisToExpiration)
        return mockStatic(
            Sasl::class.java,
            Answer { saslServer } as Answer<SaslServer>,
        )
    }

    private fun mockKafkaPrincipal(principalType: String, name: String): MockedStatic<*> {
        val kafkaPrincipalBuilder = mock<KafkaPrincipalBuilder>()
        whenever(kafkaPrincipalBuilder.build(any())).thenReturn(KafkaPrincipal(principalType, name))

        val channelBuilders = mockStatic(ChannelBuilders::class.java, Answers.RETURNS_MOCKS)
        channelBuilders.`when`<KafkaPrincipalBuilder> {
            createPrincipalBuilder(
                configs = any<Map<String, String?>>(),
                kerberosShortNamer = any<KerberosShortNamer>(),
                sslPrincipalMapper = any<SslPrincipalMapper>(),
            )
        }.thenReturn(kafkaPrincipalBuilder)
        return channelBuilders
    }

    private fun consumeSizeAndHeader(responseBuffer: ByteBuffer) {
        responseBuffer.getInt()
        ResponseHeader.parse(responseBuffer, 1.toShort())
    }

    @Throws(IOException::class)
    private fun getResponses(transportLayer: TransportLayer): List<ByteBuffer> {
        val buffersCaptor = ArgumentCaptor.forClass(Array<ByteBuffer>::class.java)
        verify(transportLayer, times(numInvocations = 2)).write(buffersCaptor.capture())

        return buffersCaptor.allValues.map { buffers -> concatBuffers(buffers) }
    }

    private fun concatBuffers(buffers: Array<ByteBuffer>): ByteBuffer {
        val combinedCapacity = buffers.sumOf(ByteBuffer::capacity)

        return if (combinedCapacity > 0) {
            val concat = ByteBuffer.allocate(combinedCapacity)
            for (buffer in buffers) concat.put(buffer)
            safeFlip(concat)
        } else ByteBuffer.allocate(0)
    }

    private fun safeFlip(buffer: ByteBuffer): ByteBuffer {
        return (buffer as Buffer).flip() as ByteBuffer
    }

    private fun saslAuthenticateRequest(): SaslAuthenticateRequest {
        val authenticateRequestData = SaslAuthenticateRequestData()
        return SaslAuthenticateRequest.Builder(authenticateRequestData)
            .build(ApiKeys.SASL_AUTHENTICATE.latestVersion())
    }

    private fun saslHandshakeRequest(mechanism: String): SaslHandshakeRequest {
        val handshakeRequestData = SaslHandshakeRequestData()
        handshakeRequestData.setMechanism(mechanism)
        return SaslHandshakeRequest.Builder(handshakeRequestData)
            .build(ApiKeys.SASL_HANDSHAKE.latestVersion())
    }

    @Throws(IOException::class)
    private fun mockTransportLayer(): TransportLayer {
        val transportLayer = mock<TransportLayer>(defaultAnswer = Answers.RETURNS_DEEP_STUBS)
        whenever(transportLayer.socketChannel()!!.socket().getInetAddress())
            .thenReturn(InetAddress.getLoopbackAddress())
        whenever(transportLayer.write(any<Array<ByteBuffer>>())).thenReturn(Long.MAX_VALUE)

        return transportLayer
    }

    @Throws(IOException::class)
    private fun mockRequest(request: AbstractRequest, transportLayer: TransportLayer) {
        mockRequest(
            header = RequestHeader(
                requestApiKey = request.apiKey,
                requestVersion = request.apiKey.latestVersion(),
                clientId = clientId,
                correlationId = 0,
            ),
            request = request,
            transportLayer = transportLayer,
        )
    }

    @Throws(IOException::class)
    private fun mockRequest(header: RequestHeader, request: AbstractRequest, transportLayer: TransportLayer) {
        val headerBuffer = serializeRequestHeader(header)
        val requestBuffer = request.serialize()
        requestBuffer.rewind()
        whenever(transportLayer.read(any<ByteBuffer>())).then { invocation ->
            invocation.getArgument<ByteBuffer>(0)
                .putInt(headerBuffer.remaining() + requestBuffer.remaining())
            4
        }.then { invocation ->
            invocation.getArgument<ByteBuffer>(0)
                .put(headerBuffer.duplicate())
                .put(requestBuffer.duplicate())
            headerBuffer.remaining() + requestBuffer.remaining()
        }
    }

    @Throws(IOException::class)
    private fun testApiVersionsRequest(
        version: Short, expectedSoftwareName: String,
        expectedSoftwareVersion: String,
    ) {
        val transportLayer = mockTransportLayer()
        val configs = mapOf(
            BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG to listOf(ScramMechanism.SCRAM_SHA_256.mechanismName),
        )
        val metadataRegistry: ChannelMetadataRegistry = DefaultChannelMetadataRegistry()
        val authenticator = setupAuthenticator(
            configs = configs,
            transportLayer = transportLayer,
            mechanism = ScramMechanism.SCRAM_SHA_256.mechanismName,
            metadataRegistry = metadataRegistry,
        )
        val header = RequestHeader(
            requestApiKey = ApiKeys.API_VERSIONS,
            requestVersion = version,
            clientId = clientId,
            correlationId = 0,
        )
        val request = ApiVersionsRequest.Builder().build(version)
        mockRequest(header, request, transportLayer)
        authenticator.authenticate()
        assertEquals(expectedSoftwareName, metadataRegistry.clientInformation()!!.softwareName)
        assertEquals(expectedSoftwareVersion, metadataRegistry.clientInformation()!!.softwareVersion)
        verify(transportLayer, times(numInvocations = 2)).read(any<ByteBuffer>())
    }

    private fun setupAuthenticator(
        configs: Map<String, *>,
        transportLayer: TransportLayer,
        mechanism: String,
        metadataRegistry: ChannelMetadataRegistry,
        time: Time = MockTime(),
        maxReauth: Long? = null,
    ): SaslServerAuthenticator {
        val jaasConfig = TestJaasConfig()
        jaasConfig.addEntry(
            name = "jaasContext",
            loginModule = PlainLoginModule::class.java.getName(),
            options = emptyMap(),
        )
        val subjects = mapOf(mechanism to Subject())
        val callbackHandlers = mapOf(mechanism to SaslServerCallbackHandler())
        val apiVersionsResponse = ApiVersionsResponse.defaultApiVersionsResponse(
            listenerType = ApiMessageType.ListenerType.ZK_BROKER,
        )
        val connectionsMaxReauthMsByMechanism =
            if (maxReauth != null) mapOf(mechanism to maxReauth) else emptyMap()
        return SaslServerAuthenticator(
            configs = configs,
            callbackHandlers = callbackHandlers,
            connectionId = "node",
            subjects = subjects,
            kerberosNameParser = null,
            listenerName = ListenerName("ssl"),
            securityProtocol = SecurityProtocol.SASL_SSL,
            transportLayer = transportLayer,
            connectionsMaxReauthMsByMechanism = connectionsMaxReauthMsByMechanism,
            metadataRegistry = metadataRegistry,
            time = time,
        ) { apiVersionsResponse }
    }
}
