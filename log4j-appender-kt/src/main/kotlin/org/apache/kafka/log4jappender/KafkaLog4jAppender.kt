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

package org.apache.kafka.log4jappender

import java.util.Date
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_KERBEROS_SERVICE_NAME
import org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM
import org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG
import org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG
import org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
import org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
import org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
import org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.helpers.LogLog
import org.apache.log4j.spi.LoggingEvent

/**
 * A log4j appender that produces log messages to Kafka
 */
open class KafkaLog4jAppender : AppenderSkeleton() {

    var brokerList: String? = null

    var topic: String? = null

    var compressionType: String? = null

    var securityProtocol: String? = null

    var sslTruststoreLocation: String? = null

    var sslTruststorePassword: String? = null

    var sslKeystoreType: String? = null

    var sslKeystoreLocation: String? = null

    var sslKeystorePassword: String? = null

    var saslKerberosServiceName: String? = null

    var saslMechanism: String? = null

    var clientJaasConfPath: String? = null

    var clientJaasConf: String? = null

    var kerb5ConfPath: String? = null

    private var maxBlockMs: Int? = null

    var sslEngineFactoryClass: String? = null

    var retries = Int.MAX_VALUE

    var requiredNumAcks = 1

    var deliveryTimeoutMs = 120000

    var lingerMs = 0

    var batchSize = 16384

    var ignoreExceptions = true

    var syncSend = false

    var producer: Producer<ByteArray?, ByteArray?>? = null
        private set

    @Suppress("Unused")
    fun getMaxBlockMs(): Int = maxBlockMs!!

    // Kotlin Migration - Setter required by Log4j's PropertySetter
    @Suppress("Unused")
    fun setMaxBlockMs(maxBlockMs: Int) {
        this.maxBlockMs = maxBlockMs
    }

    override fun activateOptions() {
        // check for config parameter validity
        val props = Properties()
        if (brokerList != null) props[BOOTSTRAP_SERVERS_CONFIG] = brokerList
        if (props.isEmpty) throw ConfigException("The bootstrap servers property should be specified")
        if (topic == null) throw ConfigException("Topic must be specified by the Kafka log4j appender")
        if (compressionType != null) props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = compressionType

        props[ProducerConfig.ACKS_CONFIG] = requiredNumAcks.toString()
        props[ProducerConfig.RETRIES_CONFIG] = retries
        props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = deliveryTimeoutMs
        props[ProducerConfig.LINGER_MS_CONFIG] = lingerMs
        props[ProducerConfig.BATCH_SIZE_CONFIG] = batchSize
        // Disable idempotence to avoid deadlock when the producer network thread writes a log line while interacting
        // with the TransactionManager, see KAFKA-13761 for more information.
        props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = false

        if (securityProtocol != null) props[SECURITY_PROTOCOL_CONFIG] = securityProtocol
        if (
            securityProtocol != null
            && (securityProtocol!!.contains("SSL") || securityProtocol!!.contains("SASL"))
        ) sslEngineFactoryClass?.let { props[SSL_ENGINE_FACTORY_CLASS_CONFIG] = it }

        if (
            securityProtocol != null
            && securityProtocol!!.contains("SSL")
            && sslTruststoreLocation != null
            && sslTruststorePassword != null
        ) {
            props[SSL_TRUSTSTORE_LOCATION_CONFIG] = sslTruststoreLocation
            props[SSL_TRUSTSTORE_PASSWORD_CONFIG] = sslTruststorePassword
            if (sslKeystoreType != null && sslKeystoreLocation != null && sslKeystorePassword != null) {
                props[SSL_KEYSTORE_TYPE_CONFIG] = sslKeystoreType
                props[SSL_KEYSTORE_LOCATION_CONFIG] = sslKeystoreLocation
                props[SSL_KEYSTORE_PASSWORD_CONFIG] = sslKeystorePassword
            }
        }
        if (
            securityProtocol != null
            && securityProtocol!!.contains("SASL")
            && saslKerberosServiceName != null
            && clientJaasConfPath != null
        ) {
            props[SASL_KERBEROS_SERVICE_NAME] = saslKerberosServiceName
            System.setProperty("java.security.auth.login.config", clientJaasConfPath)
        }
        if (kerb5ConfPath != null) System.setProperty("java.security.krb5.conf", kerb5ConfPath)
        if (saslMechanism != null) props[SASL_MECHANISM] = saslMechanism
        if (clientJaasConf != null) props[SASL_JAAS_CONFIG] = clientJaasConf
        if (maxBlockMs != null) props[ProducerConfig.MAX_BLOCK_MS_CONFIG] = maxBlockMs

        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.getName()
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.getName()
        producer = getKafkaProducer(props)
        LogLog.debug("Kafka producer connected to $brokerList")
        LogLog.debug("Logging for topic: $topic")
    }

    internal open fun getKafkaProducer(props: Properties): Producer<ByteArray?, ByteArray?> = KafkaProducer(props)

    override fun append(event: LoggingEvent) {
        val message = subAppend(event)
        LogLog.debug("[" + Date(event.getTimeStamp()) + "]" + message)
        val response: Future<RecordMetadata> = producer!!.send(
            ProducerRecord(topic = topic!!, value = message.toByteArray())
        )
        if (syncSend) {
            try {
                response.get()
            } catch (ex: InterruptedException) {
                if (!ignoreExceptions) throw RuntimeException(ex)
                LogLog.debug("Exception while getting response", ex)
            } catch (ex: ExecutionException) {
                if (!ignoreExceptions) throw RuntimeException(ex)
                LogLog.debug("Exception while getting response", ex)
            }
        }
    }

    private fun subAppend(event: LoggingEvent): String {
        return if (layout == null) event.getRenderedMessage() else layout.format(event)
    }

    override fun close() {
        if (!closed) {
            closed = true
            producer!!.close()
        }
    }

    override fun requiresLayout(): Boolean = true
    open fun getKafkaProducer(props: Properties?): Producer<ByteArray?, ByteArray?> {
        TODO("Not yet implemented")
    }
}
