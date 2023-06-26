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

package org.apache.kafka.clients.admin

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.postProcessReconnectBackoffConfigs
import org.apache.kafka.clients.CommonClientConfigs.postValidateSaslMechanismConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString.Companion.`in`
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Range.Companion.atLeast
import org.apache.kafka.common.config.ConfigDef.Range.Companion.between
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils.enumOptions

/**
 * The AdminClient configuration class, which also contains constants for configuration entry names.
 */
class AdminClientConfig internal constructor(
    props: Map<String, Any>,
    doLog: Boolean = false,
) : AbstractConfig(
    definition = CONFIG,
    originals = props,
    doLog = doLog,
) {

    internal fun postProcessParsedConfig(parsedValues: Map<String, Any>): Map<String, Any?> {
        postValidateSaslMechanismConfig(this)
        return postProcessReconnectBackoffConfigs(this, parsedValues)
    }

    companion object {

        private val CONFIG: ConfigDef

        /**
         * `bootstrap.servers`
         */
        val BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
        private val BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC

        /**
         * `client.dns.lookup`
         */
        val CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG
        private val CLIENT_DNS_LOOKUP_DOC = CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC

        /**
         * `reconnect.backoff.ms`
         */
        val RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG
        private val RECONNECT_BACKOFF_MS_DOC = CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC

        /**
         * `reconnect.backoff.max.ms`
         */
        val RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG
        private val RECONNECT_BACKOFF_MAX_MS_DOC = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC

        /**
         * `retry.backoff.ms`
         */
        val RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG
        private const val RETRY_BACKOFF_MS_DOC =
            "The amount of time to wait before attempting to retry a failed request. This avoids " +
                    "repeatedly sending requests in a tight loop under some failure scenarios."

        /**
         * `socket.connection.setup.timeout.ms`
         */
        val SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG =
            CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG

        /**
         * `socket.connection.setup.timeout.max.ms`
         */
        val SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG =
            CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG

        /**
         * `connections.max.idle.ms`
         */
        val CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG
        private val CONNECTIONS_MAX_IDLE_MS_DOC = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC

        /**
         * `request.timeout.ms`
         */
        val REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG
        private val REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC

        val CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG
        private val CLIENT_ID_DOC = CommonClientConfigs.CLIENT_ID_DOC

        val METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG
        private val METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC

        val SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG
        private val SEND_BUFFER_DOC = CommonClientConfigs.SEND_BUFFER_DOC

        val RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG
        private val RECEIVE_BUFFER_DOC = CommonClientConfigs.RECEIVE_BUFFER_DOC

        val METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG
        private val METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC

        @Deprecated("")
        val AUTO_INCLUDE_JMX_REPORTER_CONFIG = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG
        val AUTO_INCLUDE_JMX_REPORTER_DOC = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC

        val METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG
        private val METRICS_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC

        val METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG
        private val METRICS_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC

        val SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
        val DEFAULT_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL
        private val SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC

        val METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG
        private val METRICS_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC

        val RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG
        val DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG

        /**
         * `security.providers`
         */
        const val SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG
        private const val SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC

        init {
            CONFIG = ConfigDef().define(
                name = BOOTSTRAP_SERVERS_CONFIG,
                type = ConfigDef.Type.LIST,
                importance = Importance.HIGH,
                documentation = BOOTSTRAP_SERVERS_DOC,
            ).define(
                name = CLIENT_ID_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = "",
                importance = Importance.MEDIUM,
                documentation = CLIENT_ID_DOC,
            ).define(
                name = METADATA_MAX_AGE_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 5 * 60 * 1000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = METADATA_MAX_AGE_DOC,
            ).define(
                name = SEND_BUFFER_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 128 * 1024,
                validator = atLeast(-1),
                importance = Importance.MEDIUM,
                documentation = SEND_BUFFER_DOC,
            ).define(
                name = RECEIVE_BUFFER_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 64 * 1024,
                validator = atLeast(-1),
                importance = Importance.MEDIUM,
                documentation = RECEIVE_BUFFER_DOC,
            ).define(
                name = RECONNECT_BACKOFF_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 50L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = RECONNECT_BACKOFF_MS_DOC,
            ).define(
                name = RECONNECT_BACKOFF_MAX_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 1000L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = RECONNECT_BACKOFF_MAX_MS_DOC,
            ).define(
                name = RETRY_BACKOFF_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 100L,
                validator = atLeast(0L),
                importance = Importance.LOW,
                documentation = RETRY_BACKOFF_MS_DOC,
            ).define(
                name = REQUEST_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 30000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = REQUEST_TIMEOUT_MS_DOC,
            ).define(
                name = SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC,
            ).define(
                name = SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC,
            ).define(
                name = CONNECTIONS_MAX_IDLE_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 5 * 60 * 1000,
                importance = Importance.MEDIUM,
                documentation = CONNECTIONS_MAX_IDLE_MS_DOC,
            ).define(
                name = RETRIES_CONFIG,
                type = ConfigDef.Type.INT, Int.MAX_VALUE,
                validator = between(0, Int.MAX_VALUE),
                importance = Importance.LOW,
                documentation = CommonClientConfigs.RETRIES_DOC,
            ).define(
                name = DEFAULT_API_TIMEOUT_MS_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 60000,
                validator = atLeast(0),
                importance = Importance.MEDIUM,
                documentation = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC,
            ).define(
                name = METRICS_SAMPLE_WINDOW_MS_CONFIG,
                type = ConfigDef.Type.LONG,
                defaultValue = 30000,
                validator = atLeast(0),
                importance = Importance.LOW,
                documentation = METRICS_SAMPLE_WINDOW_MS_DOC,
            ).define(
                name = METRICS_NUM_SAMPLES_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = 2,
                validator = atLeast(1),
                importance = Importance.LOW,
                documentation = METRICS_NUM_SAMPLES_DOC,
            ).define(
                name = METRIC_REPORTER_CLASSES_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = "",
                importance = Importance.LOW,
                documentation = METRIC_REPORTER_CLASSES_DOC,
            ).define(
                name = METRICS_RECORDING_LEVEL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = Sensor.RecordingLevel.INFO.toString(),
                validator = `in`(
                    Sensor.RecordingLevel.INFO.toString(),
                    Sensor.RecordingLevel.DEBUG.toString(),
                    Sensor.RecordingLevel.TRACE.toString(),
                ),
                importance = Importance.LOW,
                documentation = METRICS_RECORDING_LEVEL_DOC,
            ).define(
                name = AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                type = ConfigDef.Type.BOOLEAN,
                defaultValue = true,
                importance = Importance.LOW,
                documentation = AUTO_INCLUDE_JMX_REPORTER_DOC,
            ).define(
                name = CLIENT_DNS_LOOKUP_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                validator = `in`(
                    ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                    ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString(),
                ),
                importance = Importance.MEDIUM,
                documentation = CLIENT_DNS_LOOKUP_DOC,
            ).define(
                // security support
                name = SECURITY_PROVIDERS_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                importance = Importance.LOW,
                documentation = SECURITY_PROVIDERS_DOC,
            ).define(
                name = SECURITY_PROTOCOL_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = DEFAULT_SECURITY_PROTOCOL,
                validator = `in`(*enumOptions(SecurityProtocol::class.java)),
                importance = Importance.MEDIUM,
                documentation = SECURITY_PROTOCOL_DOC,
            )
                .withClientSslSupport()
                .withClientSaslSupport()
        }

        fun configNames(): Set<String> = CONFIG.names()

        fun configDef(): ConfigDef = ConfigDef(CONFIG)

        @JvmStatic
        fun main(args: Array<String>) {
            println(CONFIG.toHtml(4, { config -> "adminclientconfigs_$config" }))
        }
    }
}
