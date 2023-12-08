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
import org.apache.kafka.common.serialization.Serdes.IntegerSerde
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull

class ListDeserializerTest {

    private val listDeserializer: ListDeserializer<*> = ListDeserializer<Any>()

    private val props: MutableMap<String, Any?> = HashMap()

    private val nonExistingClass = "non.existing.class"

    private class FakeObject

    @Test
    fun testListKeyDeserializerNoArgConstructorsWithClassNames() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java.getName()
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java.getName()
        listDeserializer.configure(props, true)
        val inner = listDeserializer.innerDeserializer()
        assertNotNull(inner, "Inner deserializer should be not null")
        assertIs<StringDeserializer>(inner, "Inner deserializer type should be StringDeserializer")
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsWithClassNames() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java.getName()
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = IntegerSerde::class.java.getName()
        listDeserializer.configure(props, false)
        val inner = listDeserializer.innerDeserializer()
        assertNotNull(inner, "Inner deserializer should be not null")
        assertIs<IntegerDeserializer>(inner, "Inner deserializer type should be IntegerDeserializer")
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsWithClassObjects() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        listDeserializer.configure(props, true)
        val inner = listDeserializer.innerDeserializer()
        assertNotNull(inner, "Inner deserializer should be not null")
        assertIs<StringDeserializer>(inner, "Inner deserializer type should be StringDeserializer")
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsWithClassObjects() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        listDeserializer.configure(props, false)
        val inner = listDeserializer.innerDeserializer()
        assertNotNull(inner, "Inner deserializer should be not null")
        assertIs<StringDeserializer>(inner, "Inner deserializer type should be StringDeserializer")
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingInnerClassProp() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Not able to determine the inner serde class because it was neither passed via the constructor nor set in the config.",
            exception.message,
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingInnerClassProp() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Not able to determine the inner serde class because it was neither passed via the constructor nor set in the config.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingTypeClassProp() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Not able to determine the list class because it was neither passed via the constructor nor set in the config.",
            exception.message,
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingTypeClassProp() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Not able to determine the list class because it was neither passed via the constructor nor set in the config.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = FakeObject()
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<KafkaException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Could not determine the list class instance using \"${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS}\" property.",
            exception.message,
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = FakeObject()
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<KafkaException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Could not determine the list class instance using \"${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS}\" property.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = FakeObject()
        val exception = assertFailsWith<KafkaException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Could not determine the inner serde class instance using \"${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS}\" property.",
            exception.message,
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = FakeObject()
        val exception = assertFailsWith<KafkaException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Could not determine the inner serde class instance using \"${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS}\" property.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueListClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = nonExistingClass
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Invalid value $nonExistingClass for configuration ${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS}: " +
                    "Deserializer's list class \"$nonExistingClass\" could not be found.",
            exception.message,
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueListClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = nonExistingClass
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Invalid value $nonExistingClass for configuration ${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS}: " +
                    "Deserializer's list class \"$nonExistingClass\" could not be found.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueInnerSerdeClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = nonExistingClass
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, true) }
        assertEquals(
            "Invalid value $nonExistingClass for configuration ${CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS}: " +
                    "Deserializer's inner serde class \"$nonExistingClass\" could not be found.",
            exception.message
        )
    }

    @Test
    fun testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueInnerSerdeClassNotFound() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = nonExistingClass
        val exception = assertFailsWith<ConfigException> { listDeserializer.configure(props, false) }
        assertEquals(
            "Invalid value $nonExistingClass for configuration ${CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS}: " +
                    "Deserializer's inner serde class \"$nonExistingClass\" could not be found.",
            exception.message,
        )
    }

    @Test
    fun testListKeyDeserializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS] = StringSerde::class.java
        val initializedListDeserializer = ListDeserializer(ArrayList::class.java, Integer().deserializer())
        val exception = assertFailsWith<ConfigException> { initializedListDeserializer.configure(props, true) }
        assertEquals(
            "List deserializer was already initialized using a non-default constructor",
            exception.message
        )
    }

    @Test
    fun testListValueDeserializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS] = ArrayList::class.java
        props[CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS] = StringSerde::class.java
        val initializedListDeserializer = ListDeserializer(ArrayList::class.java, Integer().deserializer())
        val exception = assertFailsWith<ConfigException> { initializedListDeserializer.configure(props, true) }
        assertEquals(
            "List deserializer was already initialized using a non-default constructor",
            exception.message
        )
    }
}
