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

package org.apache.kafka.common.metrics

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.metrics.stats.WindowedSum
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import javax.management.Attribute
import javax.management.AttributeList
import javax.management.AttributeNotFoundException
import javax.management.ObjectName
import javax.management.RuntimeMBeanException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.fail

class KafkaMbeanTest {
    
    private val mBeanServer = ManagementFactory.getPlatformMBeanServer()
    
    private var sensor: Sensor? = null
    
    private var countMetricName: MetricName? = null
    
    private var sumMetricName: MetricName? = null
    
    private var metrics: Metrics? = null
    
    @BeforeEach
    @Throws(Exception::class)
    fun setup() {
        metrics = Metrics()
        metrics!!.addReporter(JmxReporter())
        sensor = metrics!!.sensor("kafka.requests")
        countMetricName = metrics!!.metricName("pack.bean1.count", "grp1")
        sensor!!.add(countMetricName!!, WindowedCount())
        sumMetricName = metrics!!.metricName("pack.bean1.sum", "grp1")
        sensor!!.add(sumMetricName!!, WindowedSum())
    }

    @AfterEach
    fun tearDown() {
        metrics!!.close()
    }

    @Test
    @Throws(Exception::class)
    fun testGetAttribute() {
        sensor!!.record(2.5)
        val counterAttribute = getAttribute(countMetricName)
        assertEquals(1.0, counterAttribute)
        val sumAttribute = getAttribute(sumMetricName)
        assertEquals(2.5, sumAttribute)
    }

    @Test
    @Throws(Exception::class)
    fun testGetAttributeUnknown() {
        sensor!!.record(2.5)
        try {
            getAttribute(sumMetricName, "name")
            fail("Should have gotten attribute not found")
        } catch (_: AttributeNotFoundException) {
            // Expected
        }
    }

    @Test
    @Throws(Exception::class)
    fun testGetAttributes() {
        sensor!!.record(3.5)
        sensor!!.record(4.0)
        val attributeList = getAttributes(countMetricName, countMetricName!!.name, sumMetricName!!.name)
        val attributes = attributeList.asList()
        assertEquals(2, attributes.size)
        for (attribute in attributes) {
            when (attribute.name) {
                countMetricName!!.name -> assertEquals(2.0, attribute.value)
                sumMetricName!!.name -> assertEquals(7.5, attribute.value)
                else -> fail("Unexpected attribute returned: " + attribute.name)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testGetAttributesWithUnknown() {
        sensor!!.record(3.5)
        sensor!!.record(4.0)
        val attributeList = getAttributes(countMetricName, countMetricName!!.name, sumMetricName!!.name, "name")
        val attributes = attributeList.asList()
        assertEquals(2, attributes.size)

        for (attribute in attributes) {
            if (countMetricName!!.name == attribute.name) assertEquals(2.0, attribute.value)
            else if (sumMetricName!!.name == attribute.name) assertEquals(7.5, attribute.value)
            else fail("Unexpected attribute returned: " + attribute.name)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInvoke() {
        val e = assertFailsWith<RuntimeMBeanException> {
            mBeanServer.invoke(
                objectName(countMetricName),
                "something",
                null,
                null,
            )
        }
        assertIs<UnsupportedOperationException>(e.cause!!)
    }

    @Test
    @Throws(Exception::class)
    fun testSetAttribute() {
        val e = assertFailsWith<RuntimeMBeanException> {
            mBeanServer.setAttribute(
                objectName(countMetricName),
                Attribute("anything", 1),
            )
        }
        assertIs<UnsupportedOperationException>(e.cause!!)
    }

    @Test
    @Throws(Exception::class)
    fun testSetAttributes() {
        val e = assertFailsWith<RuntimeMBeanException> {
            mBeanServer.setAttributes(objectName(countMetricName), AttributeList(1))
        }
        assertIs<UnsupportedOperationException>(e.cause!!)
    }

    @Throws(Exception::class)
    private fun objectName(metricName: MetricName?): ObjectName {
        return ObjectName(JmxReporter.getMBeanName("", metricName!!))
    }

    @Throws(Exception::class)
    private fun getAttribute(metricName: MetricName?, attribute: String): Any {
        return mBeanServer.getAttribute(objectName(metricName), attribute)
    }

    @Throws(Exception::class)
    private fun getAttribute(metricName: MetricName?): Any {
        return getAttribute(metricName, metricName!!.name)
    }

    @Throws(Exception::class)
    private fun getAttributes(metricName: MetricName?, vararg attributes: String): AttributeList {
        return mBeanServer.getAttributes(objectName(metricName), attributes)
    }
}
