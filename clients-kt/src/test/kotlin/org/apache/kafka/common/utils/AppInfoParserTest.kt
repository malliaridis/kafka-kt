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

package org.apache.kafka.common.utils

import java.lang.management.ManagementFactory
import javax.management.JMException
import javax.management.MBeanServer
import javax.management.MalformedObjectNameException
import javax.management.ObjectName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.AppInfoParser.commitId
import org.apache.kafka.common.utils.AppInfoParser.registerAppInfo
import org.apache.kafka.common.utils.AppInfoParser.unregisterAppInfo
import org.apache.kafka.common.utils.AppInfoParser.version
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AppInfoParserTest {
    
    private lateinit var metrics: Metrics
    
    private lateinit var mBeanServer: MBeanServer
    
    @BeforeEach
    fun setUp() {
        metrics = Metrics(time = MockTime(1))
        mBeanServer = ManagementFactory.getPlatformMBeanServer()
    }

    @AfterEach
    fun tearDown() {
        metrics.close()
    }

    @Test
    @Throws(JMException::class)
    fun testRegisterAppInfoRegistersMetrics() {
        registerAppInfo()
    }

    @Test
    @Throws(JMException::class)
    fun testUnregisterAppInfoUnregistersMetrics() {
        registerAppInfo()
        unregisterAppInfo(METRICS_PREFIX, METRICS_ID, metrics)
        assertFalse(mBeanServer.isRegistered(expectedAppObjectName()))
        assertNull(metrics.metric(metrics.metricName("commit-id", "app-info")))
        assertNull(metrics.metric(metrics.metricName("version", "app-info")))
        assertNull(metrics.metric(metrics.metricName("start-time-ms", "app-info")))
    }

    @Throws(JMException::class)
    private fun registerAppInfo() {
        assertEquals(EXPECTED_COMMIT_VERSION, commitId)
        assertEquals(EXPECTED_VERSION, version)
        registerAppInfo(METRICS_PREFIX, METRICS_ID, metrics, EXPECTED_START_MS)
        assertTrue(mBeanServer.isRegistered(expectedAppObjectName()))
        assertEquals(
            EXPECTED_COMMIT_VERSION,
            metrics.metric(metrics.metricName("commit-id", "app-info"))!!.metricValue(),
        )
        assertEquals(
            EXPECTED_VERSION,
            metrics.metric(metrics.metricName("version", "app-info"))!!.metricValue(),
        )
        assertEquals(
            EXPECTED_START_MS,
            metrics.metric(metrics.metricName("start-time-ms", "app-info"))!!.metricValue(),
        )
    }

    @Throws(MalformedObjectNameException::class)
    private fun expectedAppObjectName(): ObjectName =
        ObjectName("$METRICS_PREFIX:type=app-info,id=$METRICS_ID")

    companion object {

        private const val EXPECTED_COMMIT_VERSION = AppInfoParser.DEFAULT_VALUE

        private const val EXPECTED_VERSION = AppInfoParser.DEFAULT_VALUE

        private const val EXPECTED_START_MS = 1552313875722L

        private const val METRICS_PREFIX = "app-info-test"

        private const val METRICS_ID = "test"
    }
}
