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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.LinkedList
import java.util.Stack
import java.util.UUID
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serdes.Boolean
import org.apache.kafka.common.serialization.Serdes.Double
import org.apache.kafka.common.serialization.Serdes.Float
import org.apache.kafka.common.serialization.Serdes.Integer
import org.apache.kafka.common.serialization.Serdes.ListSerde
import org.apache.kafka.common.serialization.Serdes.Long
import org.apache.kafka.common.serialization.Serdes.Short
import org.apache.kafka.common.serialization.Serdes.String
import org.apache.kafka.common.serialization.Serdes.UUID
import org.apache.kafka.common.serialization.Serdes.Void
import org.apache.kafka.common.serialization.Serdes.serdeFrom
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.common.utils.Utils.wrapNullable
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class SerializationTest {
    
    private val topic = "testTopic"
    
    private val testData = mapOf(
        String::class.java to listOf(null, "my string"),
        Short::class.java to listOf<Short?>(null, 32767, -32768),
        Int::class.java to listOf(null, 423412424, -41243432),
        Long::class.java to listOf(null, 922337203685477580L, -922337203685477581L),
        Float::class.java to listOf(null, 5678567.12312f, -5678567.12341f),
        Double::class.java to listOf(null, 5678567.12312, -5678567.12341),
        ByteArray::class.java to listOf(null, "my string".toByteArray()),
        ByteBuffer::class.java to listOf(
            null,
            ByteBuffer.wrap("my string".toByteArray()),
            ByteBuffer.allocate(10).put("my string".toByteArray()),
            ByteBuffer.allocateDirect(10).put("my string".toByteArray()),
        ),
        Bytes::class.java to listOf(null, Bytes("my string".toByteArray())),
        UUID::class.java to listOf(null, UUID.randomUUID()),
    )

    private inner class DummyClass

    @Test
    fun allSerdesShouldRoundtripInput() {
        for ((key, value) in testData) {
            assertIs<Class<Any>>(key)
            serdeFrom(key).use { serde ->
                for (element in value) {
                    val serialized = serde.serializer().serialize(topic, element)
                    assertEquals(
                        expected = element,
                        actual = serde.deserializer().deserialize(topic, serialized),
                        message = "Should get the original ${key.name} after serialization and deserialization"
                    )

                    if (element is ByteArray) assertContentEquals(
                        expected = element,
                        actual = serde.deserializer().deserialize(
                            topic = topic,
                            data = element,
                        ) as ByteArray?,
                        message = "Should get the original ${key.name} after serialization and deserialization"
                    )
                    else assertEquals(
                        expected = element,
                        actual = serde.deserializer().deserialize(
                            topic = topic,
                            data = wrapNullable(serialized),
                        ),
                        message = "Should get the original ${key.name} after serialization and deserialization",
                    )
                }
            }
        }
    }

    @Test
    fun allSerdesShouldSupportNull() {
        for (cls in testData.keys) {
            serdeFrom(cls).use { serde ->
                assertNull(
                    actual = serde.serializer().serialize(topic = topic, data = null),
                    message = "Should support null in ${cls.name} serialization",
                )
                assertNull(
                    actual = serde.deserializer().deserialize(topic = topic, data = null as ByteArray?),
                    message = "Should support null in ${cls.name} deserialization",
                )
                assertNull(
                    actual = serde.deserializer().deserialize(topic = topic, data = null as ByteBuffer?),
                    message = "Should support null in ${cls.name} deserialization",
                )
            }
        }
    }

    @Test
    fun testSerdeFromUnknown() {
        assertFailsWith<IllegalArgumentException> { serdeFrom(DummyClass::class.java) }
    }

    @Test
    @Disabled("Kotlin Migration: serdeFrom parameters are non-nullable in Kotlin.")
    fun testSerdeFromNotNull() {
//        Long().use { serde ->
//            assertFailsWith<IllegalArgumentException> { serdeFrom(null, serde.deserializer()) }
//        }
    }

    @Test
    fun stringSerdeShouldSupportDifferentEncodings() {
        val str = "my string"
        val encodings = listOf(StandardCharsets.UTF_8.name(), StandardCharsets.UTF_16.name())
        for (encoding in encodings) {
            getStringSerde(encoding).use { serDeser ->
                val serializer = serDeser.serializer()
                val deserializer = serDeser.deserializer()
                assertEquals(
                    expected = str,
                    actual = deserializer.deserialize(topic, serializer.serialize(topic, str)),
                    message = "Should get the original string after serialization and deserialization with encoding $encoding",
                )
            }
        }
    }

    @Test
    fun listSerdeShouldReturnEmptyCollection() {
        val testData = emptyList<Int>()
        val listSerde = ListSerde(ArrayList::class.java, Integer())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get empty collection after serialization and deserialization on an empty list"
        )
    }

    @Test
    fun listSerdeShouldReturnNull() {
        val testData: List<Int>? = null
        val listSerde = ListSerde(ArrayList::class.java, Integer())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get null after serialization and deserialization on an empty list",
        )
    }

    @Test
    fun listSerdeShouldRoundtripIntPrimitiveInput() {
        val testData = listOf(1, 2, 3)
        val listSerde = ListSerde(ArrayList::class.java, Integer())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of integer primitives after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForIntPrimitiveInput() {
        val testData = listOf(1, 2, 3)
        val listSerde = ListSerde(ArrayList::class.java, Integer())
        assertEquals(
            expected = 21,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 21 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripShortPrimitiveInput() {
        val testData = listOf<Short>(1, 2, 3)
        val listSerde = ListSerde(ArrayList::class.java, Short())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of short primitives after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForShortPrimitiveInput() {
        val testData = listOf<Short>(1, 2, 3)
        val listSerde = ListSerde(ArrayList::class.java, Short())
        assertEquals(
            expected = 15,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 15 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripFloatPrimitiveInput() {
        val testData = listOf(1f, 2f, 3f)
        val listSerde = ListSerde(ArrayList::class.java, Float())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of float primitives after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForFloatPrimitiveInput() {
        val testData = listOf(1f, 2f, 3f)
        val listSerde = ListSerde(ArrayList::class.java, Float())
        assertEquals(
            expected = 21,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 21 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripLongPrimitiveInput() {
        val testData = listOf(1L, 2L, 3L)
        val listSerde = ListSerde(ArrayList::class.java, Long())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of long primitives after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForLongPrimitiveInput() {
        val testData = listOf(1L, 2L, 3L)
        val listSerde = ListSerde(ArrayList::class.java, Long())
        assertEquals(
            expected = 33,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 33 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripDoublePrimitiveInput() {
        val testData = listOf(1.0, 2.0, 3.0)
        val listSerde = ListSerde(ArrayList::class.java, Double())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of double primitives after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForDoublePrimitiveInput() {
        val testData = listOf(1.0, 2.0, 3.0)
        val listSerde = ListSerde(ArrayList::class.java, Double())
        assertEquals(
            expected = 33,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 33 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripUUIDInput() {
        val testData = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        val listSerde = ListSerde(ArrayList::class.java, UUID())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of UUID after serialization and deserialization",
        )
    }

    @Test
    fun listSerdeSerializerShouldReturnByteArrayOfFixedSizeForUUIDInput() {
        val testData = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        val listSerde = ListSerde(ArrayList::class.java, UUID())
        assertEquals(
            expected = 117,
            actual = listSerde.serializer().serialize(topic, testData)!!.size,
            message = "Should get length of 117 bytes after serialization",
        )
    }

    @Test
    fun listSerdeShouldRoundtripNonPrimitiveInput() {
        val testData = mutableListOf("A", "B", "C")
        val listSerde = ListSerde(ArrayList::class.java, String())
        assertEquals(
            expected = testData,
            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should get the original collection of strings list after serialization and deserialization",
        )
    }

    @Test
    @Disabled("TODO Add nullable serdes too")
    fun listSerdeShouldRoundtripPrimitiveInputWithNullEntries() {
        val testData = listOf(1, null, 3)
        val listSerde = ListSerde(ArrayList::class.java, Integer())
//        assertEquals(
//            expected = testData,
//            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
//            message = "Should get the original collection of integer primitives with null entries after serialization and deserialization",
//        )
    }

    @Test
    @Disabled("TODO Add nullable serdes too")
    fun listSerdeShouldRoundtripNonPrimitiveInputWithNullEntries() {
        val testData = listOf("A", null, "C")
        val listSerde = ListSerde(ArrayList::class.java, String())
//        assertEquals(
//            expected = testData,
//            actual = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
//            message = "Should get the original collection of strings list with null entries after serialization and deserialization",
//        )
    }

    @Test
    fun listSerdeShouldReturnLinkedList() {
        val testData = LinkedList<Int>()
        val listSerde = ListSerde(LinkedList::class.java, Integer())
        assertIs<LinkedList<*>>(
            value = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should return List instance of type LinkedList", 
        )
    }

    @Test
    fun listSerdeShouldReturnStack() {
        val testData: List<Int> = Stack()
        val listSerde = ListSerde(Stack::class.java, Integer())
        assertIs<Stack<*>>(
            value = listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            message = "Should return List instance of type Stack",
        )
    }

    @Test
    fun floatDeserializerShouldThrowSerializationExceptionOnZeroBytes() {
        Float().use { serde ->
            assertFailsWith<SerializationException> { serde.deserializer().deserialize(topic, ByteArray(0)) }
        }
    }

    @Test
    fun floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes() {
        Float().use { serde ->
            assertFailsWith<SerializationException> { serde.deserializer().deserialize(topic, ByteArray(3)) }
        }
    }

    @Test
    fun floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes() {
        Float().use { serde ->
            assertFailsWith<SerializationException> { serde.deserializer().deserialize(topic, ByteArray(5)) }
        }
    }

    @Test
    fun floatSerdeShouldPreserveNaNValues() {
        val someNaNAsIntBits = 0x7f800001
        val someNaN = java.lang.Float.intBitsToFloat(someNaNAsIntBits)
        val anotherNaNAsIntBits = 0x7f800002
        val anotherNaN = java.lang.Float.intBitsToFloat(anotherNaNAsIntBits)
        Float().use { serde ->
            // Because of NaN semantics we must assert based on the raw int bits.
            val roundtrip = serde.deserializer().deserialize(
                topic = topic,
                data = serde.serializer().serialize(topic, someNaN),
            )!!
            assertEquals(
                expected = someNaNAsIntBits,
                actual = java.lang.Float.floatToRawIntBits(roundtrip),
            )
            val otherRoundtrip = serde.deserializer().deserialize(
                topic = topic,
                data = serde.serializer().serialize(topic, anotherNaN),
            )!!
            assertEquals(
                expected = anotherNaNAsIntBits,
                actual = java.lang.Float.floatToRawIntBits(otherRoundtrip),
            )
        }
    }

    @Test
    fun testSerializeVoid() {
        Void().use { serde -> serde.serializer().serialize(topic = topic, data = null) }
    }

    @Test
    fun testDeserializeVoid() {
        Void().use { serde -> serde.deserializer().deserialize(topic = topic, data = null as ByteArray?) }
    }

    @Test
    fun voidDeserializerShouldThrowOnNotNullValues() {
        Void().use { serde ->
            assertFailsWith<IllegalArgumentException> { serde.deserializer().deserialize(topic, ByteArray(5)) }
        }
    }

    @Test
    fun stringDeserializerSupportByteBuffer() {
        val data = "Hello, ByteBuffer!"
        String().use { serde ->
            val serializer = serde.serializer()
            val deserializer = serde.deserializer()
            val serializedBytes = serializer.serialize(topic, data)!!

            val heapBuff = ByteBuffer.allocate(serializedBytes.size shl 1)
                .put(serializedBytes)
            heapBuff.flip()
            assertEquals(data, deserializer.deserialize(topic = topic, data = heapBuff))

            val directBuff = ByteBuffer.allocateDirect(serializedBytes.size shl 2)
                .put(serializedBytes)
            directBuff.flip()
            assertEquals(data, deserializer.deserialize(topic = topic, data = directBuff))
        }
    }

    private fun getStringSerde(encoder: String): Serde<String> {
        val serializerConfigs: MutableMap<String, Any?> = HashMap()
        serializerConfigs["key.serializer.encoding"] = encoder
        val serializer = String().serializer()
        serializer.configure(serializerConfigs, true)
        val deserializerConfigs: MutableMap<String, Any?> = HashMap()
        deserializerConfigs["key.deserializer.encoding"] = encoder
        val deserializer = String().deserializer()
        deserializer.configure(deserializerConfigs, true)
        return serdeFrom(serializer, deserializer)
    }

    @Test
    fun testByteBufferSerializer() {
        val bytes = "Hello".toByteArray(StandardCharsets.UTF_8)
        val heapBuffer0 = ByteBuffer.allocate(bytes.size + 1).put(bytes)
        val heapBuffer1 = ByteBuffer.allocate(bytes.size).put(bytes)
        val heapBuffer2 = ByteBuffer.wrap(bytes)
        val directBuffer0 = ByteBuffer.allocateDirect(bytes.size + 1).put(bytes)
        val directBuffer1 = ByteBuffer.allocateDirect(bytes.size).put(bytes)
        ByteBufferSerializer().use { serializer ->
            assertContentEquals(bytes, serializer.serialize(topic, heapBuffer0))
            assertContentEquals(bytes, serializer.serialize(topic, heapBuffer1))
            assertContentEquals(bytes, serializer.serialize(topic, heapBuffer2))
            assertContentEquals(bytes, serializer.serialize(topic, directBuffer0))
            assertContentEquals(bytes, serializer.serialize(topic, directBuffer1))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testBooleanSerializer(dataToSerialize: Boolean) {
        val testData = ByteArray(1)
        testData[0] = (if (dataToSerialize) 1 else 0).toByte()
        val booleanSerde = Boolean()
        assertContentEquals(testData, booleanSerde.serializer().serialize(topic, dataToSerialize))
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun testBooleanDeserializer(dataToDeserialize: Boolean) {
        val testData = ByteArray(1)
        testData[0] = (if (dataToDeserialize) 1 else 0).toByte()
        val booleanSerde = Boolean()
        assertEquals(dataToDeserialize, booleanSerde.deserializer().deserialize(topic, testData))
    }

    @Test
    fun booleanDeserializerShouldThrowOnEmptyInput() {
        Boolean().use { serde ->
            assertFailsWith<SerializationException> {
                serde.deserializer().deserialize(topic, ByteArray(0))
            }
        }
    }
}
