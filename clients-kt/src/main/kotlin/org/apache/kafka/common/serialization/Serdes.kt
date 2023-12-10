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
import java.util.UUID
import org.apache.kafka.common.utils.Bytes

/**
 * Factory for creating serializers / deserializers.
 */
@Suppress("FunctionName")
object Serdes {

    fun <T> serdeFrom(type: Class<T>): Serde<T> {
        if (String::class.java.isAssignableFrom(type)) {
            return String() as Serde<T>
        }
        if (Short::class.java.isAssignableFrom(type)) {
            return Short() as Serde<T>
        }
        if (Int::class.java.isAssignableFrom(type)) {
            return Integer() as Serde<T>
        }
        if (Long::class.java.isAssignableFrom(type)) {
            return Long() as Serde<T>
        }
        if (Float::class.java.isAssignableFrom(type)) {
            return Float() as Serde<T>
        }
        if (Double::class.java.isAssignableFrom(type)) {
            return Double() as Serde<T>
        }
        if (ByteArray::class.java.isAssignableFrom(type)) {
            return ByteArray() as Serde<T>
        }
        if (ByteBuffer::class.java.isAssignableFrom(type)) {
            return ByteBuffer() as Serde<T>
        }
        if (Bytes::class.java.isAssignableFrom(type)) {
            return Bytes() as Serde<T>
        }
        if (UUID::class.java.isAssignableFrom(type)) {
            return UUID() as Serde<T>
        }

        if (Boolean::class.java.isAssignableFrom(type)) {
            return Boolean() as Serde<T>
        }


        // TODO: we can also serializes objects of type T using generic Java serialization by default
        throw IllegalArgumentException(
            "Unknown class for built-in serializer. Supported types are: " +
                    "String, Short, Integer, Long, Float, Double, ByteArray, ByteBuffer, Bytes, UUID, Boolean"
        )
    }

    /**
     * Construct a serde object from separate serializer and deserializer
     *
     * @param serializer must not be null.
     * @param deserializer must not be null.
     */
    fun <T> serdeFrom(serializer: Serializer<T>, deserializer: Deserializer<T>): Serde<T> {
        return WrapperSerde(serializer, deserializer)
    }

    /**
     * A serde for nullable `Long` type.
     */
    fun Long(): Serde<Long> = LongSerde()

    /**
     * A serde for nullable `Integer` type.
     */
    fun Integer(): Serde<Int> = IntegerSerde()

    /**
     * A serde for nullable `Short` type.
     */
    fun Short(): Serde<Short> = ShortSerde()

    /**
     * A serde for nullable `Float` type.
     */
    fun Float(): Serde<Float> = FloatSerde()

    /**
     * A serde for nullable `Double` type.
     */
    fun Double(): Serde<Double> = DoubleSerde()

    /**
     * A serde for nullable `String` type.
     */
    fun String(): Serde<String> = StringSerde()

    /**
     * A serde for nullable `ByteBuffer` type.
     */
    fun ByteBuffer(): Serde<ByteBuffer> = ByteBufferSerde()

    /**
     * A serde for nullable `Bytes` type.
     */
    fun Bytes(): Serde<Bytes> = BytesSerde()

    /**
     * A serde for nullable `UUID` type
     */
    fun UUID(): Serde<UUID> = UUIDSerde()

    /**
     * A serde for nullable `byte[]` type.
     */
    fun ByteArray(): Serde<ByteArray> = ByteArraySerde()

    /**
     * A serde for `Void` type.
     */
    fun Void(): Serde<Void> = VoidSerde()

    /**
     * A serde for nullable `Boolean` type.
     */
    fun Boolean(): Serde<Boolean?> = BooleanSerde()

    /*
     * A serde for {@code List} type
     */
    fun <L : List<Inner>?, Inner> ListSerde(
        listClass: Class<L?>?,
        innerSerde: Serde<Inner>,
    ): Serde<List<Inner>> = Serdes.ListSerde(listClass, innerSerde)

    open class WrapperSerde<T>(
        private val serializer: Serializer<T>,
        private val deserializer: Deserializer<T>,
    ) : Serde<T> {

        override fun configure(configs: Map<String, *>, isKey: Boolean) {
            serializer.configure(configs, isKey)
            deserializer.configure(configs, isKey)
        }

        override fun close() {
            serializer.close()
            deserializer.close()
        }

        override fun serializer(): Serializer<T> {
            return serializer
        }

        override fun deserializer(): Deserializer<T> {
            return deserializer
        }
    }

    class VoidSerde : WrapperSerde<Void>(VoidSerializer(), VoidDeserializer())

    class LongSerde : WrapperSerde<Long>(LongSerializer(), LongDeserializer())

    class IntegerSerde : WrapperSerde<Int>(IntegerSerializer(), IntegerDeserializer())

    class ShortSerde : WrapperSerde<Short>(ShortSerializer(), ShortDeserializer())

    class FloatSerde : WrapperSerde<Float>(FloatSerializer(), FloatDeserializer())

    class DoubleSerde : WrapperSerde<Double>(DoubleSerializer(), DoubleDeserializer())

    class StringSerde : WrapperSerde<String>(StringSerializer(), StringDeserializer())

    class ByteBufferSerde : WrapperSerde<ByteBuffer>(
        serializer = ByteBufferSerializer(),
        deserializer = ByteBufferDeserializer(),
    )

    class BytesSerde : WrapperSerde<Bytes>(BytesSerializer(), BytesDeserializer())

    class ByteArraySerde : WrapperSerde<ByteArray>(ByteArraySerializer(), ByteArrayDeserializer())

    class UUIDSerde : WrapperSerde<UUID>(UUIDSerializer(), UUIDDeserializer())

    class BooleanSerde : WrapperSerde<Boolean?>(BooleanSerializer(), BooleanDeserializer())

    class ListSerde<Inner> : WrapperSerde<List<Inner>> {

        internal enum class SerializationStrategy {
            CONSTANT_SIZE,
            VARIABLE_SIZE;

            companion object {
                val VALUES = values()
            }
        }

        constructor() : super(
            serializer = ListSerializer<Inner>() as Serializer<List<Inner>>,
            deserializer = ListDeserializer<Inner>() as Deserializer<List<Inner>>,
        )

        constructor(
            listClass: Class<*>,
            serde: Serde<Inner>,
        ) : super(
            serializer = ListSerializer<Inner>(serde.serializer()) as Serializer<List<Inner>>,
            deserializer = ListDeserializer<Inner>(
                listClass = listClass,
                inner = serde.deserializer()
            ) as Deserializer<List<Inner>>,
        )

        companion object {
            const val NULL_ENTRY_VALUE = -1
        }
    }
}
