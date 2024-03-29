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

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import java.util.Collections
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ChannelBuilder
import org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.getHost
import org.apache.kafka.common.utils.Utils.getPort
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ClientUtils {

    private val log: Logger = LoggerFactory.getLogger(ClientUtils::class.java)

    fun parseAndValidateAddresses(config: AbstractConfig): List<InetSocketAddress> {
        val urls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)!!
        val clientDnsLookupConfig = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)!!
        return parseAndValidateAddresses(urls, clientDnsLookupConfig)
    }

    fun parseAndValidateAddresses(
        urls: List<String>,
        clientDnsLookupConfig: String,
    ): List<InetSocketAddress> = parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig))

    fun parseAndValidateAddresses(
        urls: List<String>,
        clientDnsLookup: ClientDnsLookup,
    ): List<InetSocketAddress> {
        val addresses: MutableList<InetSocketAddress> = ArrayList()
        for (url in urls) {
            if (url.isNotEmpty()) {
                try {
                    val host: String? = getHost(url)
                    val port: Int? = getPort(url)
                    if (host == null || port == null) throw ConfigException(
                        "Invalid url in ${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}: $url"
                    )

                    if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
                        val inetAddresses = InetAddress.getAllByName(host)
                        for (inetAddress in inetAddresses) {
                            val resolvedCanonicalName = inetAddress.canonicalHostName
                            val address = InetSocketAddress(resolvedCanonicalName, port)
                            if (address.isUnresolved) {
                                log.warn(
                                    "Couldn't resolve server {} from {} as DNS resolution of the " +
                                            "canonical hostname {} failed for {}",
                                    url,
                                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                    resolvedCanonicalName,
                                    host
                                )
                            } else addresses.add(address)
                        }
                    } else {
                        val address = InetSocketAddress(host, port)
                        if (address.isUnresolved) log.warn(
                            "Couldn't resolve server {} from {} as DNS resolution failed for {}",
                            url,
                            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                            host
                        ) else addresses.add(address)
                    }
                } catch (e: IllegalArgumentException) {
                    throw ConfigException(
                        "Invalid port in ${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}: $url"
                    )
                } catch (e: UnknownHostException) {
                    throw ConfigException(
                        "Unknown host in ${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}: $url"
                    )
                }
            }
        }
        if (addresses.isEmpty()) throw ConfigException(
            "No resolvable bootstrap urls given in ${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}"
        )
        return addresses
    }

    /**
     * Create a new channel builder from the provided configuration.
     *
     * @param config client configs
     * @param time the time implementation
     * @param logContext the logging context
     *
     * @return configured ChannelBuilder based on the configs.
     */
    fun createChannelBuilder(
        config: AbstractConfig,
        time: Time,
        logContext: LogContext,
    ): ChannelBuilder {
        val securityProtocol =
            SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)!!)
        val clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM)

        return clientChannelBuilder(
            securityProtocol = securityProtocol,
            contextType = JaasContext.Type.CLIENT,
            config = config,
            clientSaslMechanism = clientSaslMechanism,
            time = time,
            logContext = logContext,
        )
    }

    @Throws(UnknownHostException::class)
    fun resolve(host: String?, hostResolver: HostResolver): List<InetAddress> {
        val addresses = hostResolver.resolve(host)
        val result = filterPreferredAddresses(addresses)
        if (log.isDebugEnabled) log.debug("Resolved host {} as {}",
            host,
            result.joinToString(",") { address -> address.hostAddress })
        return result
    }

    /**
     * Return a list containing the first address in `allAddresses` and subsequent addresses that
     * are a subtype of the first address.
     *
     * The outcome is that all returned addresses are either IPv4 or IPv6 (InetAddress has two
     * subclasses: Inet4Address and Inet6Address).
     */
    fun filterPreferredAddresses(allAddresses: Array<InetAddress>): List<InetAddress> {
        var clazz: Class<out InetAddress>? = null

        return allAddresses.filter { address ->
            if (clazz == null) clazz = address.javaClass
            clazz!!.isInstance(address)
        }
    }

    fun createNetworkClient(
        config: AbstractConfig,
        clientId: String? = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG),
        metrics: Metrics,
        metricsGroupPrefix: String,
        logContext: LogContext,
        apiVersions: ApiVersions,
        time: Time,
        maxInFlightRequestsPerConnection: Int,
        requestTimeoutMs: Int = config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG)!!,
        metadata: Metadata? = null,
        metadataUpdater: MetadataUpdater? = null,
        hostResolver: HostResolver = DefaultHostResolver(),
        throttleTimeSensor: Sensor? = null,
    ): NetworkClient {
        var channelBuilder: ChannelBuilder? = null
        var selector: Selector? = null
        return try {
            channelBuilder = createChannelBuilder(config, time, logContext)
            selector = Selector(
                connectionMaxIdleMs = config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG)!!,
                metrics = metrics,
                time = time,
                metricGrpPrefix = metricsGroupPrefix,
                channelBuilder = channelBuilder,
                logContext = logContext,
            )
            NetworkClient(
                metadataUpdater = metadataUpdater,
                metadata = metadata,
                selector = selector,
                clientId = clientId,
                maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection,
                reconnectBackoffMs = config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG)!!,
                reconnectBackoffMax = config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG)!!,
                socketSendBuffer = config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG)!!,
                socketReceiveBuffer = config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG)!!,
                defaultRequestTimeoutMs = requestTimeoutMs,
                connectionSetupTimeoutMs =
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)!!,
                connectionSetupTimeoutMaxMs =
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)!!,
                time = time,
                discoverBrokerVersions = true,
                apiVersions = apiVersions,
                throttleTimeSensor = throttleTimeSensor,
                logContext = logContext,
                hostResolver = hostResolver,
            )
        } catch (t: Throwable) {
            closeQuietly(selector, "Selector")
            closeQuietly(channelBuilder, "ChannelBuilder")
            throw KafkaException("Failed to create new NetworkClient", t)
        }
    }

    @Deprecated(
        message = "Use function with generic type and no clazz param.",
        replaceWith = ReplaceWith("createConfiguredInterceptors<T>(config, interceptorClassesConfigName)")
    )
    fun <T> createConfiguredInterceptors(
        config: AbstractConfig,
        interceptorClassesConfigName: String,
        clazz: Class<T>,
    ): List<T> {
        val clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)
        return config.getConfiguredInstances(
            key = interceptorClassesConfigName,
            t = clazz,
            configOverrides = mapOf(CommonClientConfigs.CLIENT_ID_CONFIG to clientId),
        )
    }

    inline fun <reified T> createConfiguredInterceptors(
        config: AbstractConfig,
        interceptorClassesConfigName: String,
    ): List<T> {
        val clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)
        return config.getConfiguredInstances(
            key = interceptorClassesConfigName,
            t = T::class.java,
            configOverrides = mapOf(CommonClientConfigs.CLIENT_ID_CONFIG to clientId),
        )
    }
}
