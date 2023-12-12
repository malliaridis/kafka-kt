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

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.utils.Utils.newInstance
import org.slf4j.LoggerFactory

class ListSerializer<Inner> : Serializer<List<Inner>?> {

    private val log = LoggerFactory.getLogger(ListSerializer::class.java)

    lateinit var innerSerializer: Serializer<Inner>
        private set

    private var serStrategy: Serdes.ListSerde.SerializationStrategy? = null

    constructor()

    constructor(inner: Serializer<Inner>) {
        innerSerializer = inner
        serStrategy =
            if (FIXED_LENGTH_SERIALIZERS.contains(inner.javaClass))
                Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE
            else Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        if (this::innerSerializer.isInitialized) {
            log.error(
                "Could not configure ListSerializer as the parameter has already been set -- inner: {}",
                innerSerializer
            )
            throw ConfigException("List serializer was already initialized using a non-default constructor")
        }

        val innerSerdePropertyName =
            if (isKey) CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS
            else CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS

        val innerSerdeClassOrName = configs[innerSerdePropertyName] ?: throw ConfigException(
                "Not able to determine the serializer class because it was neither passed via " +
                        "the constructor nor set in the config."
            )

        try {
            innerSerializer = when (innerSerdeClassOrName) {
                is String -> newInstance(innerSerdeClassOrName, Serde::class.java).serializer()

                is Class<*> -> (newInstance(innerSerdeClassOrName) as Serde<*>).serializer()

                else -> throw KafkaException(
                    "Could not create a serializer class instance using " +
                            "\"$innerSerdePropertyName\" property."
                )
            } as Serializer<Inner>

            innerSerializer.configure(configs, isKey)
            serStrategy =
                if (FIXED_LENGTH_SERIALIZERS.contains(innerSerializer.javaClass))
                    Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE
                else Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE

        } catch (e: ClassNotFoundException) {
            throw ConfigException(
                name = innerSerdePropertyName,
                value = innerSerdeClassOrName,
                message = "Serializer class $innerSerdeClassOrName could not be found."
            )
        }
    }

    @Throws(IOException::class)
    private fun serializeNullIndexList(out: DataOutputStream, data: List<Inner?>) {
        var i = 0
        val nullIndexList: MutableList<Int> = ArrayList()
        val it: Iterator<Inner?> = data.listIterator()

        while (it.hasNext()) {
            if (it.next() == null) nullIndexList.add(i)
            i++
        }

        out.writeInt(nullIndexList.size)

        nullIndexList.forEach { nullIndex -> out.writeInt(nullIndex) }
    }

    override fun serialize(topic: String, data: List<Inner>?): ByteArray? {
        if (data == null) return null

        try {
            ByteArrayOutputStream().use { baos ->
                DataOutputStream(baos).use { out ->

                    out.writeByte(serStrategy!!.ordinal) // write serialization strategy flag

                    if (serStrategy == Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE) {
                        // In CONSTANT_SIZE strategy, indexes of null entries are encoded in a null index list
                        serializeNullIndexList(out, data)
                    }

                    val size = data.size
                    out.writeInt(size)

                    data.forEach { entry ->
                        if (entry == null) {
                            if (serStrategy == Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE) {
                                out.writeInt(Serdes.ListSerde.NULL_ENTRY_VALUE)
                            }
                        } else {
                            val bytes = innerSerializer.serialize(topic, entry)
                            if (serStrategy == Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE) {
                                out.writeInt(bytes!!.size)
                            }
                            out.write(bytes)
                        }
                    }

                    return baos.toByteArray()
                }
            }
        } catch (e: IOException) {
            log.error("Failed to serialize list due to", e)
            // avoid logging actual data above TRACE level since it may contain sensitive information
            log.trace("List that could not be serialized: {}", data)
            throw KafkaException("Failed to serialize List", e)
        }
    }

    override fun close() {
        if (this::innerSerializer.isInitialized) innerSerializer.close()
    }

    companion object {

        private val FIXED_LENGTH_SERIALIZERS = listOf<Class<out Serializer<*>?>>(
            ShortSerializer::class.java,
            IntegerSerializer::class.java,
            FloatSerializer::class.java,
            LongSerializer::class.java,
            DoubleSerializer::class.java,
            UUIDSerializer::class.java
        )
    }
}
