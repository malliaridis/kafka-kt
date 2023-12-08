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

package org.apache.kafka.clients.consumer

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Companion.getAssignorInstances
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import java.util.Properties
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class ConsumerPartitionAssignorTest {
    
    @Test
    fun shouldInstantiateAssignor() {
        val assignors = getAssignorInstances(
            assignorClasses = listOf(StickyAssignor::class.java.getName()),
            configs = emptyMap()
        )
        assertIs<StickyAssignor>(assignors[0])
    }

    @Test
    fun shouldInstantiateListOfAssignors() {
        val assignors: List<ConsumerPartitionAssignor> = getAssignorInstances(
            assignorClasses = listOf(
                StickyAssignor::class.java.getName(),
                CooperativeStickyAssignor::class.java.getName(),
            ),
            configs = emptyMap(),
        )
        assertIs<StickyAssignor>(assignors[0])
        assertIs<CooperativeStickyAssignor>(assignors[1])
    }

    @Test
    fun shouldThrowKafkaExceptionOnNonAssignor() {
        assertFailsWith<KafkaException> {
            getAssignorInstances(
                assignorClasses = listOf(String::class.java.getName()),
                configs = emptyMap(),
            )
        }
    }

    @Test
    fun shouldThrowKafkaExceptionOnAssignorNotFound() {
        assertFailsWith<KafkaException> {
            getAssignorInstances(
                assignorClasses = listOf("Non-existent assignor"),
                configs = emptyMap(),
            )
        }
    }

    @Test
    fun shouldInstantiateFromClassType() {
        val classTypes = initConsumerConfigWithClassTypes(
            listOf(StickyAssignor::class.java.getName())
        )
            .getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)!!

        val assignors = getAssignorInstances(classTypes, emptyMap())
        assertIs<StickyAssignor>(assignors[0])
    }

    @Test
    fun shouldInstantiateFromListOfClassTypes() {
        val classTypes = initConsumerConfigWithClassTypes(
            classTypes = listOf(
                StickyAssignor::class.java.getName(),
                CooperativeStickyAssignor::class.java.getName()
            ),
        ).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)!!

        val assignors = getAssignorInstances(classTypes, emptyMap())
        assertIs<StickyAssignor>(assignors[0])
        assertIs<CooperativeStickyAssignor>(assignors[1])
    }

    @Test
    fun shouldThrowKafkaExceptionOnListWithNonAssignorClassType() {
        val classTypes = initConsumerConfigWithClassTypes(
            listOf(StickyAssignor::class.java.getName(), String::class.java.getName())
        ).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)!!
        assertFailsWith<KafkaException> {
            getAssignorInstances(
                assignorClasses = classTypes,
                configs = emptyMap(),
            )
        }
    }

    @Test
    fun shouldThrowKafkaExceptionOnAssignorsWithSameName() {
        assertFailsWith<KafkaException> {
            getAssignorInstances(
                assignorClasses = listOf(
                    RangeAssignor::class.java.getName(),
                    TestConsumerPartitionAssignor::class.java.getName(),
                ),
                configs = emptyMap(),
            )
        }
    }

    class TestConsumerPartitionAssignor : ConsumerPartitionAssignor {

        override fun assign(
            metadata: Cluster,
            groupSubscription: GroupSubscription
        ): GroupAssignment = GroupAssignment(emptyMap())

        override fun name(): String {
            // use the RangeAssignor's name to cause naming conflict
            return RangeAssignor().name()
        }
    }

    private fun initConsumerConfigWithClassTypes(classTypes: List<Any>): ConsumerConfig {
        val props = Properties()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.getName()
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.getName()
        props[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = classTypes
        return ConsumerConfig(props)
    }
}
