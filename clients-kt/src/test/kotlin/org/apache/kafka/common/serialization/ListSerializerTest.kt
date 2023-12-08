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

package org.apache.kafka.common.serialization

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.Serdes.Integer
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull

class ListSerializerTest {

    private val listSerializer: ListSerializer<*> = ListSerializer<Any>()

    private val props: MutableMap<String, Any?> = HashMap()

    private val nonExistingClass = "non.existing.class"

    private class FakeObject

    @Test
    fun testListKeySerializerNoArgConstructorsWithClassName() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java.getName()
        listSerializer.configure(props, true)
        val inner = listSerializer.innerSerializer
        assertNotNull(inner, "Inner serializer should be not null")
        assertIs<StringSerializer>(inner, "Inner serializer type should be StringSerializer")
    }

    @Test
    fun testListValueSerializerNoArgConstructorsWithClassName() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java.getName()
        listSerializer.configure(props, false)
        val inner = listSerializer.innerSerializer
        assertNotNull(inner, "Inner serializer should be not null")
        assertIs<StringSerializer>(inner, "Inner serializer type should be StringSerializer")
    }

    @Test
    fun testListKeySerializerNoArgConstructorsWithClassObject() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        listSerializer.configure(props, true)
        val inner = listSerializer.innerSerializer
        assertNotNull(inner, "Inner serializer should be not null")
        assertIs<StringSerializer>(inner, "Inner serializer type should be StringSerializer")
    }

    @Test
    fun testListValueSerializerNoArgConstructorsWithClassObject() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        listSerializer.configure(props, false)
        val inner = listSerializer.innerSerializer
        assertNotNull(inner, "Inner serializer should be not null")
        assertIs<StringSerializer>(inner, "Inner serializer type should be StringSerializer")
    }

    @Test
    fun testListSerializerNoArgConstructorsShouldThrowConfigExceptionDueMissingProp() {
        var exception = assertFailsWith<ConfigException> { listSerializer.configure(props, true) }
        assertEquals(
            "Not able to determine the serializer class because it was neither passed via the constructor nor set in the config.",
            exception.message
        )
        exception = assertFailsWith<ConfigException> { listSerializer.configure(props, false) }
        assertEquals(
            "Not able to determine the serializer class because it was neither passed via the constructor nor set in the config.",
            exception.message
        )
    }

    @Test
    fun testListKeySerializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidClass() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = FakeObject()
        val exception = assertFailsWith<KafkaException> { listSerializer.configure(props, true) }
        assertEquals(
            "Could not create a serializer class instance using \"${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS}\" property.",
            exception.message
        )
    }

    @Test
    fun testListValueSerializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidClass() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = FakeObject()
        val exception = assertFailsWith<KafkaException> { listSerializer.configure(props, false) }
        assertEquals(
            "Could not create a serializer class instance using \"${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS}\" property.",
            exception.message
        )
    }

    @Test
    fun testListKeySerializerNoArgConstructorsShouldThrowKafkaExceptionDueClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = nonExistingClass
        val exception = assertFailsWith<KafkaException> { listSerializer.configure(props, true) }
        assertEquals(
            "Invalid value non.existing.class for configuration ${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS}: Serializer class $nonExistingClass could not be found.",
            exception.message
        )
    }

    @Test
    fun testListValueSerializerNoArgConstructorsShouldThrowKafkaExceptionDueClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = nonExistingClass
        val exception = assertFailsWith<KafkaException> { listSerializer.configure(props, false) }
        assertEquals(
            "Invalid value non.existing.class for configuration ${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS}: Serializer class $nonExistingClass could not be found.",
            exception.message
        )
    }

    @Test
    fun testListKeySerializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        val initializedListSerializer = ListSerializer(Integer().serializer())
        val exception = assertFailsWith<ConfigException> { initializedListSerializer.configure(props, true) }
        assertEquals(
            "List serializer was already initialized using a non-default constructor",
            exception.message
        )
    }

    @Test
    fun testListValueSerializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        val initializedListSerializer = ListSerializer(Integer().serializer())
        val exception = assertFailsWith<ConfigException> { initializedListSerializer.configure(props, false) }
        assertEquals(
            "List serializer was already initialized using a non-default constructor",
            exception.message
        )
    }
}
