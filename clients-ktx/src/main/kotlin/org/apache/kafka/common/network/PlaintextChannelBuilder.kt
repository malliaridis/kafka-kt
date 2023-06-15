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
import java.util.*
import java.util.function.Supplier
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.ChannelBuilders.createPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.slf4j.LoggerFactory

/**
 * Constructs a plaintext channel builder. ListenerName is non-`null` whenever it's instantiated in
 * the broker and `null` otherwise.
 */
open class PlaintextChannelBuilder(private val listenerName: ListenerName) : ChannelBuilder {

    private lateinit var configs: Map<String, *>

    @Throws(KafkaException::class)
    override fun configure(configs: Map<String, *>) {
        this.configs = configs
    }

    @Throws(KafkaException::class)
    override fun buildChannel(
        id: String, key: SelectionKey,
        maxReceiveSize: Int,
        memoryPool: MemoryPool?,
        metadataRegistry: ChannelMetadataRegistry?
    ): KafkaChannel {
        return try {
            val transportLayer = buildTransportLayer(key)
            val authenticatorCreator =
                Supplier<Authenticator> {
                    PlaintextAuthenticator(configs, transportLayer, listenerName)
                }

            buildChannel(
                id,
                transportLayer,
                authenticatorCreator,
                maxReceiveSize,
                memoryPool ?: MemoryPool.NONE,
                metadataRegistry
            )
        } catch (e: Exception) {
            throw KafkaException(cause = e)
        }
    }

    // visible for testing
    open fun buildChannel(
        id: String,
        transportLayer: TransportLayer,
        authenticatorCreator: Supplier<Authenticator>,
        maxReceiveSize: Int,
        memoryPool: MemoryPool,
        metadataRegistry: ChannelMetadataRegistry?
    ): KafkaChannel {
        return KafkaChannel(
            id,
            transportLayer,
            authenticatorCreator,
            maxReceiveSize,
            memoryPool,
            metadataRegistry,
        )
    }

    @Throws(IOException::class)
    protected fun buildTransportLayer(key: SelectionKey) = PlaintextTransportLayer(key)

    override fun close() = Unit

    private class PlaintextAuthenticator(
        configs: Map<String, *>,
        private val transportLayer: PlaintextTransportLayer,
        listenerName: ListenerName
    ) : Authenticator {

        private val principalBuilder: KafkaPrincipalBuilder

        private val listenerName: ListenerName?

        init {
            principalBuilder = createPrincipalBuilder(configs)
            this.listenerName = listenerName
        }

        override fun authenticate() = Unit

        override fun principal(): KafkaPrincipal {
            val clientAddress = transportLayer.socketChannel()!!.socket().inetAddress
            // listenerName should only be null in Client mode where principal() should not be called
            checkNotNull(listenerName) { "Unexpected call to principal() when listenerName is null" }

            return principalBuilder.build(
                PlaintextAuthenticationContext(clientAddress, listenerName.value)
            )
        }

        override fun principalSerde(): KafkaPrincipalSerde? =
            principalBuilder as? KafkaPrincipalSerde

        override fun complete(): Boolean = true

        override fun close() {

            if (principalBuilder is Closeable) closeQuietly(
                closeable = principalBuilder as Closeable,
                name = "principal builder"
            )
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(PlaintextChannelBuilder::class.java)
    }
}
