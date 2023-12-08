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

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertNull

class KafkaMetricsContextTest {
    
    private lateinit var namespace: String
    
    private lateinit var labels: MutableMap<String, String?>
    
    private lateinit var context: KafkaMetricsContext
    
    @BeforeEach
    fun beforeEach() {
        namespace = SAMPLE_NAMESPACE
        labels = HashMap()
        labels[LABEL_A_KEY] = LABEL_A_VALUE
    }

    @Test
    fun testCreationWithValidNamespaceAndNoLabels() {
        labels.clear()
        context = KafkaMetricsContext(namespace, labels)
        assertEquals(1, context.contextLabels().size)
        assertEquals(namespace, context.contextLabels()[MetricsContext.NAMESPACE])
    }

    @Test
    fun testCreationWithValidNamespaceAndLabels() {
        context = KafkaMetricsContext(namespace, labels)
        assertEquals(2, context.contextLabels().size)
        assertEquals(namespace, context.contextLabels()[MetricsContext.NAMESPACE])
        assertEquals(
            LABEL_A_VALUE,
            context.contextLabels()[LABEL_A_KEY]
        )
    }

    @Test
    fun testCreationWithValidNamespaceAndNullLabelValues() {
        labels[LABEL_A_KEY] = null
        context = KafkaMetricsContext(namespace, labels)
        assertEquals(2, context.contextLabels().size)
        assertEquals(namespace, context.contextLabels()[MetricsContext.NAMESPACE])
        assertNull(context.contextLabels()[LABEL_A_KEY])
    }

    @Test
    fun testCreationWithNullNamespaceAndLabels() {
        context = KafkaMetricsContext(null, labels)
        assertEquals(2, context.contextLabels().size)
        assertNull(context.contextLabels()[MetricsContext.NAMESPACE])
        assertEquals(
            LABEL_A_VALUE,
            context.contextLabels()[LABEL_A_KEY]
        )
    }

    @Test
    @Disabled("Kotlin Migration: Returned context labels is immutable map in Kotlin by default")
    fun testKafkaMetricsContextLabelsAreImmutable() {
        context = KafkaMetricsContext(namespace, labels)
        // assertFailsWith<UnsupportedOperationException> { context.contextLabels.clear() }
    }

    companion object {
        
        private const val SAMPLE_NAMESPACE = "sample-ns"
        
        private const val LABEL_A_KEY = "label-a"
        
        private const val LABEL_A_VALUE = "label-a-value"
    }
}
