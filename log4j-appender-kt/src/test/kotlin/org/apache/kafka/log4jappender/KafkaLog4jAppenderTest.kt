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

import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.helpers.LogLog
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class KafkaLog4jAppenderTest {
    
    private val logger = Logger.getLogger(KafkaLog4jAppenderTest::class.java)

    private val mockKafkaLog4jAppender: MockKafkaLog4jAppender
        get() = Logger.getRootLogger().getAppender("KAFKA") as MockKafkaLog4jAppender

    @BeforeEach
    fun setup() {
        LogLog.setInternalDebugging(true)
    }

    @Test
    fun testKafkaLog4jConfigs() {
        val hostMissingProps = Properties()
        hostMissingProps["log4j.rootLogger"] = "INFO"
        hostMissingProps["log4j.appender.KAFKA"] = "org.apache.kafka.log4jappender.KafkaLog4jAppender"
        hostMissingProps["log4j.appender.KAFKA.layout"] = "org.apache.log4j.PatternLayout"
        hostMissingProps["log4j.appender.KAFKA.layout.ConversionPattern"] = "%-5p: %c - %m%n"
        hostMissingProps["log4j.appender.KAFKA.Topic"] = "test-topic"
        hostMissingProps["log4j.logger.kafka.log4j"] = "INFO, KAFKA"

        assertFailsWith<ConfigException>("Missing properties exception was expected !") {
            PropertyConfigurator.configure(hostMissingProps)
        }

        val topicMissingProps = Properties()
        topicMissingProps["log4j.rootLogger"] = "INFO"
        topicMissingProps["log4j.appender.KAFKA"] = "org.apache.kafka.log4jappender.KafkaLog4jAppender"
        topicMissingProps["log4j.appender.KAFKA.layout"] = "org.apache.log4j.PatternLayout"
        topicMissingProps["log4j.appender.KAFKA.layout.ConversionPattern"] = "%-5p: %c - %m%n"
        topicMissingProps["log4j.appender.KAFKA.brokerList"] = "127.0.0.1:9093"
        topicMissingProps["log4j.logger.kafka.log4j"] = "INFO, KAFKA"

        assertFailsWith<ConfigException>("Missing properties exception was expected !") {
            PropertyConfigurator.configure(topicMissingProps)
        }
    }

    @Test
    fun testSetSaslMechanism() {
        val props = getLog4jConfig(false)
        props["log4j.appender.KAFKA.SaslMechanism"] = "PLAIN"
        PropertyConfigurator.configure(props)

        assertEquals(
            expected = "PLAIN",
            actual = mockKafkaLog4jAppender.producerProperties!!.getProperty(SaslConfigs.SASL_MECHANISM),
        )
    }

    @Test
    fun testSaslMechanismNotSet() {
        testProducerPropertyNotSet(SaslConfigs.SASL_MECHANISM)
    }

    @Test
    fun testSetJaasConfig() {
        val props = getLog4jConfig(false)
        props["log4j.appender.KAFKA.ClientJaasConf"] = "jaas-config"
        PropertyConfigurator.configure(props)

        assertEquals(
            expected = "jaas-config",
            actual = mockKafkaLog4jAppender.producerProperties!!.getProperty(SaslConfigs.SASL_JAAS_CONFIG),
        )
    }

    @Test
    fun testJaasConfigNotSet() {
        testProducerPropertyNotSet(SaslConfigs.SASL_JAAS_CONFIG)
    }

    private fun testProducerPropertyNotSet(name: String) {
        PropertyConfigurator.configure(getLog4jConfig(false))
        assertFalse(mockKafkaLog4jAppender.producerProperties!!.stringPropertyNames().contains(name))
    }

    @Test
    fun testLog4jAppends() {
        PropertyConfigurator.configure(getLog4jConfig(false))
        for (i in 1..5) logger.error(getMessage(i))
        assertEquals(5, mockKafkaLog4jAppender.history.size)
    }

    @Test
    fun testSyncSendAndSimulateProducerFailShouldThrowException() {
        val props = getLog4jConfig(true)
        props["log4j.appender.KAFKA.IgnoreExceptions"] = "false"
        PropertyConfigurator.configure(props)

        val mockKafkaLog4jAppender = mockKafkaLog4jAppender
        replaceProducerWithMocked(mockKafkaLog4jAppender, false)

        assertFailsWith<RuntimeException> { logger.error(getMessage(0)) }
    }

    @Test
    fun testSyncSendWithoutIgnoringExceptionsShouldNotThrowException() {
        val props = getLog4jConfig(true)
        props["log4j.appender.KAFKA.IgnoreExceptions"] = "false"
        PropertyConfigurator.configure(props)

        replaceProducerWithMocked(mockKafkaLog4jAppender, true)

        logger.error(getMessage(0))
    }

    @Test
    fun testRealProducerConfigWithSyncSendShouldNotThrowException() {
        val props = getLog4jConfigWithRealProducer(true)
        PropertyConfigurator.configure(props)
        logger.error(getMessage(0))
    }

    @Test
    fun testRealProducerConfigWithSyncSendAndNotIgnoringExceptionsShouldThrowException() {
        val props = getLog4jConfigWithRealProducer(false)
        PropertyConfigurator.configure(props)
        
        assertFailsWith<RuntimeException> { logger.error(getMessage(0)) }
    }

    private fun replaceProducerWithMocked(mockKafkaLog4jAppender: MockKafkaLog4jAppender, success: Boolean) {
        val producer = mock<MockProducer<ByteArray?, ByteArray?>>()
        val future = CompletableFuture<RecordMetadata>()
        if (success) future.complete(
            RecordMetadata(
                topicPartition = TopicPartition("tp", 0),
                baseOffset = 0,
                batchIndex = 0,
                timestamp = 0,
                serializedKeySize = 0,
                serializedValueSize = 0,
            )
        ) else future.completeExceptionally(TimeoutException("simulated timeout"))

        whenever(producer.send(any())).thenReturn(future)
        // reconfiguring mock appender
        mockKafkaLog4jAppender.setKafkaProducer(producer)
        mockKafkaLog4jAppender.activateOptions()
    }

    private fun getMessage(i: Int): ByteArray = "test_$i".toByteArray()

    private fun getLog4jConfigWithRealProducer(ignoreExceptions: Boolean): Properties {
        val props = Properties()
        props["log4j.rootLogger"] = "INFO, KAFKA"
        props["log4j.appender.KAFKA"] = "org.apache.kafka.log4jappender.KafkaLog4jAppender"
        props["log4j.appender.KAFKA.layout"] = "org.apache.log4j.PatternLayout"
        props["log4j.appender.KAFKA.layout.ConversionPattern"] = "%-5p: %c - %m%n"
        props["log4j.appender.KAFKA.BrokerList"] = "127.0.0.2:9093"
        props["log4j.appender.KAFKA.Topic"] = "test-topic"
        props["log4j.appender.KAFKA.RequiredNumAcks"] = "-1"
        props["log4j.appender.KAFKA.SyncSend"] = "true"
        // setting producer timeout (max.block.ms) to be low
        props["log4j.appender.KAFKA.maxBlockMs"] = "10"
        // ignoring exceptions
        props["log4j.appender.KAFKA.IgnoreExceptions"] = ignoreExceptions.toString()
        props["log4j.logger.kafka.log4j"] = "INFO, KAFKA"
        return props
    }

    private fun getLog4jConfig(syncSend: Boolean): Properties {
        val props = Properties()
        props["log4j.rootLogger"] = "INFO, KAFKA"
        props["log4j.appender.KAFKA"] = "org.apache.kafka.log4jappender.MockKafkaLog4jAppender"
        props["log4j.appender.KAFKA.layout"] = "org.apache.log4j.PatternLayout"
        props["log4j.appender.KAFKA.layout.ConversionPattern"] = "%-5p: %c - %m%n"
        props["log4j.appender.KAFKA.BrokerList"] = "127.0.0.1:9093"
        props["log4j.appender.KAFKA.Topic"] = "test-topic"
        props["log4j.appender.KAFKA.RequiredNumAcks"] = "-1"
        props["log4j.appender.KAFKA.SyncSend"] = syncSend.toString()
        props["log4j.logger.kafka.log4j"] = "INFO, KAFKA"
        return props
    }
}
