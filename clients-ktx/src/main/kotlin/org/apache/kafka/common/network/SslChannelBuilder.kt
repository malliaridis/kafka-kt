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

package org.apache.kafka.common.network

import java.io.Closeable
import java.io.IOException
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.util.function.Supplier
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.ChannelBuilders.createPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SslAuthenticationContext
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.security.ssl.SslPrincipalMapper
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.slf4j.Logger

/**
 * @constructor Constructs an SSL channel builder. ListenerName is provided only for server channel
 * builder and will be null for client channel builder.
 */
open class SslChannelBuilder(
    private val mode: Mode,
    private val listenerName: ListenerName?,
    private val isInterBrokerListener: Boolean,
    logContext: LogContext
) : ChannelBuilder, ListenerReconfigurable {

    private lateinit var sslFactory: SslFactory

    private lateinit var configs: Map<String, *>

    private var sslPrincipalMapper: SslPrincipalMapper? = null

    private val log: Logger = logContext.logger(javaClass)

    @Throws(KafkaException::class)
    override fun configure(configs: Map<String, *>) {
        try {
            this.configs = configs
            val sslPrincipalMappingRules =
                configs[BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG] as? String

            if (sslPrincipalMappingRules != null) sslPrincipalMapper =
                SslPrincipalMapper.fromRules(sslPrincipalMappingRules)

            sslFactory = SslFactory(
                mode = mode,
                keystoreVerifiableUsingTruststore = isInterBrokerListener,
            )
            sslFactory.configure(this.configs)
        } catch (e: KafkaException) {
            throw e
        } catch (e: Exception) {
            throw KafkaException(cause = e)
        }
    }

    override fun reconfigurableConfigs(): Set<String> = SslConfigs.RECONFIGURABLE_CONFIGS

    override fun validateReconfiguration(configs: Map<String, *>) =
        sslFactory.validateReconfiguration(configs)

    override fun reconfigure(configs: Map<String, *>) = sslFactory.reconfigure(configs)

    override fun listenerName(): ListenerName? = listenerName

    @Throws(KafkaException::class)
    override fun buildChannel(
        id: String, key: SelectionKey,
        maxReceiveSize: Int,
        memoryPool: MemoryPool?,
        metadataRegistry: ChannelMetadataRegistry?
    ): KafkaChannel {
        return try {
            val transportLayer = buildTransportLayer(sslFactory, id, key, metadataRegistry)
            val authenticatorCreator =
                Supplier<Authenticator> {
                    SslAuthenticator(
                        configs = configs,
                        transportLayer = transportLayer,
                        listenerName = listenerName,
                        sslPrincipalMapper = sslPrincipalMapper
                    )
                }
            KafkaChannel(
                id = id,
                transportLayer = transportLayer,
                authenticatorCreator = authenticatorCreator,
                maxReceiveSize = maxReceiveSize,
                memoryPool = memoryPool ?: MemoryPool.NONE,
                metadataRegistry = metadataRegistry
            )
        } catch (e: Exception) {
            throw KafkaException(cause = e)
        }
    }

    override fun close() {
        if (::sslFactory.isInitialized) sslFactory.close()
    }

    @Throws(IOException::class)
    protected open fun buildTransportLayer(
        sslFactory: SslFactory,
        id: String,
        key: SelectionKey,
        metadataRegistry: ChannelMetadataRegistry?
    ): SslTransportLayer {
        val socketChannel = key.channel() as SocketChannel
        return SslTransportLayer.create(
            id,
            key,
            sslFactory.createSslEngine(socketChannel.socket()),
            metadataRegistry!!
        )
    }

    /**
     * Note that client SSL authentication is handled in [SslTransportLayer]. This class is only
     * used to transform the derived principal using a [KafkaPrincipalBuilder] configured by the
     * user.
     */
    private class SslAuthenticator(
        configs: Map<String, *>,
        private val transportLayer: SslTransportLayer,
        private val listenerName: ListenerName? = null,
        sslPrincipalMapper: SslPrincipalMapper?
    ) : Authenticator {

        private val principalBuilder: KafkaPrincipalBuilder

        init {
            principalBuilder = createPrincipalBuilder(
                configs = configs,
                sslPrincipalMapper = sslPrincipalMapper
            )
        }

        /**
         * No-Op for plaintext authenticator
         */
        override fun authenticate() {}

        /**
         * Constructs Principal using configured principalBuilder.
         *
         * @return the built principal
         */
        override fun principal(): KafkaPrincipal {
            val clientAddress = transportLayer.socketChannel()!!.socket().inetAddress
            // listenerName should only be null in Client mode where principal() should not be called
            checkNotNull(listenerName) { "Unexpected call to principal() when listenerName is null" }
            val context = SslAuthenticationContext(
                transportLayer.sslSession(),
                clientAddress,
                listenerName.value
            )

            return principalBuilder.build(context)
        }

        override fun principalSerde(): KafkaPrincipalSerde? =
            principalBuilder as? KafkaPrincipalSerde

        @Throws(IOException::class)
        override fun close() {
            if (principalBuilder is Closeable) closeQuietly(
                closeable = principalBuilder as Closeable,
                name = "principal builder"
            )
        }

        /**
         * SslAuthenticator doesn't implement any additional authentication mechanism.
         * @return true
         */
        override fun complete(): Boolean = true
    }
}
