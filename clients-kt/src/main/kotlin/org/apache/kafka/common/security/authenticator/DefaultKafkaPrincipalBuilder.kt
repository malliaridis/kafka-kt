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
import java.nio.ByteBuffer
import java.security.Principal
import javax.net.ssl.SSLPeerUnverifiedException
import javax.security.auth.x500.X500Principal
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.message.DefaultPrincipalData
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext
import org.apache.kafka.common.security.auth.SaslAuthenticationContext
import org.apache.kafka.common.security.auth.SslAuthenticationContext
import org.apache.kafka.common.security.kerberos.KerberosName
import org.apache.kafka.common.security.kerberos.KerberosShortNamer
import org.apache.kafka.common.security.ssl.SslPrincipalMapper

/**
 * Default implementation of [KafkaPrincipalBuilder] which provides basic support for
 * SSL authentication and SASL authentication. In the latter case, when GSSAPI is used, this
 * class applies [org.apache.kafka.common.security.kerberos.KerberosShortNamer] to transform
 * the name.
 *
 * NOTE: This is an internal class and can change without notice.
 *
 * @property kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
 * @property sslPrincipalMapper SSL Principal mapper or null if none have been configured
 */
class DefaultKafkaPrincipalBuilder(
    private val kerberosShortNamer: KerberosShortNamer,
    private val sslPrincipalMapper: SslPrincipalMapper,
) : KafkaPrincipalBuilder, KafkaPrincipalSerde {

    override fun build(context: AuthenticationContext): KafkaPrincipal {
        return when (context) {
            is PlaintextAuthenticationContext -> KafkaPrincipal.ANONYMOUS

            is SslAuthenticationContext -> {
                val sslSession = context.session
                try {
                    applySslPrincipalMapper(sslSession.peerPrincipal)
                } catch (se: SSLPeerUnverifiedException) {
                    KafkaPrincipal.ANONYMOUS
                }
            }

            is SaslAuthenticationContext -> {
                val saslServer = context.server
                return if ((SaslConfigs.GSSAPI_MECHANISM == saslServer.mechanismName))
                    applyKerberosShortNamer(saslServer.authorizationID)
                else KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.authorizationID)
            }

            else -> throw IllegalArgumentException("Unhandled authentication context type: ${context.javaClass.name}")
        }
    }

    private fun applyKerberosShortNamer(authorizationId: String): KafkaPrincipal {
        val kerberosName = KerberosName.parse(authorizationId)
        try {
            val shortName = kerberosShortNamer.shortName(kerberosName)
            return KafkaPrincipal(KafkaPrincipal.USER_TYPE, shortName)
        } catch (e: IOException) {
            throw KafkaException(
                message = "Failed to set name for '$kerberosName' based on Kerberos authentication rules.",
                cause = e
            )
        }
    }

    private fun applySslPrincipalMapper(principal: Principal): KafkaPrincipal {
        try {
            return if (principal !is X500Principal)
                KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.name)
            else KafkaPrincipal(
                KafkaPrincipal.USER_TYPE,
                sslPrincipalMapper.getName(principal.getName())
            )
        } catch (e: IOException) {
            throw KafkaException(
                message = "Failed to map name for '${principal.name}' based on SSL principal mapping rules.",
                cause = e
            )
        }
    }

    override fun serialize(principal: KafkaPrincipal): ByteArray {
        val data = DefaultPrincipalData()
            .setType(principal.principalType)
            .setName(principal.name)
            .setTokenAuthenticated(principal.tokenAuthenticated)
        return MessageUtil.toVersionPrefixedBytes(
            DefaultPrincipalData.HIGHEST_SUPPORTED_VERSION,
            data
        )
    }

    override fun deserialize(bytes: ByteArray): KafkaPrincipal {
        val buffer = ByteBuffer.wrap(bytes)
        val version = buffer.getShort()
        if (version < DefaultPrincipalData.LOWEST_SUPPORTED_VERSION
            || version > DefaultPrincipalData.HIGHEST_SUPPORTED_VERSION)
            throw SerializationException("Invalid principal data version $version")

        val data = DefaultPrincipalData(ByteBufferAccessor(buffer), version)
        return KafkaPrincipal(data.type(), data.name(), data.tokenAuthenticated())
    }
}
