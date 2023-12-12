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

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.utils.Utils.loadClass
import org.apache.kafka.common.utils.Utils.mkEntry
import org.apache.kafka.common.utils.Utils.mkMap
import org.apache.kafka.common.utils.Utils.newInstance
import org.slf4j.LoggerFactory

class ListDeserializer<Inner> : Deserializer<List<Inner>?> {

    private val log = LoggerFactory.getLogger(ListDeserializer::class.java)

    private lateinit var inner: Deserializer<Inner>

    private lateinit var listClass: Class<*>

    private var primitiveSize: Int? = null

    constructor()

    constructor(listClass: Class<*>, inner: Deserializer<Inner>) {
        this.listClass = listClass
        this.inner = inner
        primitiveSize = FIXED_LENGTH_DESERIALIZERS[inner.javaClass]
    }

    fun innerDeserializer(): Deserializer<Inner> = inner

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        if (this::listClass.isInitialized || this::inner.isInitialized) {
            log.error(
                "Could not configure ListDeserializer as some parameters were already set -- listClass: {}, inner: {}",
                listClass,
                inner
            )
            throw ConfigException("List deserializer was already initialized using a non-default constructor")
        }

        configureListClass(configs, isKey)
        configureInnerSerde(configs, isKey)
    }

    private fun configureListClass(configs: Map<String, *>, isKey: Boolean) {
        val listTypePropertyName =
            if (isKey) CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS
            else CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS

        val listClassOrName = configs[listTypePropertyName] ?: throw ConfigException(
            "Not able to determine the list class because it was neither passed via the " +
                    "constructor nor set in the config."
        )

        listClass = try {
            when (listClassOrName) {
                is String -> loadClass(listClassOrName, Any::class.java)
                is Class<*> -> listClassOrName
                else -> throw KafkaException(
                    "Could not determine the list class instance using \"$listTypePropertyName\" " +
                            "property."
                )
            }

        } catch (e: ClassNotFoundException) {
            throw ConfigException(
                name = listTypePropertyName,
                value = listClassOrName,
                message = "Deserializer's list class \"$listClassOrName\" could not be found."
            )
        }
    }

    private fun configureInnerSerde(configs: Map<String, *>, isKey: Boolean) {
        val innerSerdePropertyName =
            if (isKey) CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS
            else CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS

        val innerSerdeClassOrName = configs[innerSerdePropertyName] ?: throw ConfigException(
            "Not able to determine the inner serde class because it was neither passed via the " +
                    "constructor nor set in the config."
        )

        try {
            inner = (when (innerSerdeClassOrName) {
                is String -> newInstance(klass = innerSerdeClassOrName, base = Serde::class.java)
                is Class<*> -> (newInstance(innerSerdeClassOrName) as Serde<*>)
                else -> throw KafkaException(
                    "Could not determine the inner serde class instance using " +
                            "\"$innerSerdePropertyName\" property."
                )
            }.deserializer() as Deserializer<Inner>).also {
                it.configure(configs, isKey)
            }

            primitiveSize = FIXED_LENGTH_DESERIALIZERS[inner.javaClass]
        } catch (e: ClassNotFoundException) {
            throw ConfigException(
                name = innerSerdePropertyName,
                value = innerSerdeClassOrName,
                message = "Deserializer's inner serde class \"$innerSerdeClassOrName\" could not be " +
                        "found."
            )
        }
    }

    private fun createListInstance(listSize: Int): MutableList<Inner> {
        return try {
            var listConstructor: Constructor<MutableList<Inner>>
            try {
                listConstructor =
                    listClass.getConstructor(Integer.TYPE) as Constructor<MutableList<Inner>>
                listConstructor.newInstance(listSize)
            } catch (e: NoSuchMethodException) {
                listConstructor = listClass.getConstructor() as Constructor<MutableList<Inner>>
                listConstructor.newInstance()
            }
        } catch (e: InstantiationException) {
            log.error("Failed to construct list due to ", e)
            throw KafkaException(
                message = "Could not construct a list instance of \"${listClass.canonicalName}\"",
                cause = e
            )
        } catch (e: IllegalAccessException) {
            log.error("Failed to construct list due to ", e)
            throw KafkaException(
                message = "Could not construct a list instance of \"${listClass.canonicalName}\"",
                cause = e
            )
        } catch (e: NoSuchMethodException) {
            log.error("Failed to construct list due to ", e)
            throw KafkaException(
                message = "Could not construct a list instance of \"${listClass.canonicalName}\"",
                cause = e
            )
        } catch (e: IllegalArgumentException) {
            log.error("Failed to construct list due to ", e)
            throw KafkaException(
                message = "Could not construct a list instance of \"${listClass.canonicalName}\"",
                cause = e
            )
        } catch (e: InvocationTargetException) {
            log.error("Failed to construct list due to ", e)
            throw KafkaException(
                message = "Could not construct a list instance of \"${listClass.canonicalName}\"",
                cause = e
            )
        }
    }

    @Throws(IOException::class)
    private fun parseSerializationStrategyFlag(
        serializationStrategyFlag: Int,
    ): Serdes.ListSerde.SerializationStrategy {
        if (serializationStrategyFlag < 0
            || serializationStrategyFlag >= Serdes.ListSerde.SerializationStrategy.VALUES.size)
            throw SerializationException("Invalid serialization strategy flag value")

        return Serdes.ListSerde.SerializationStrategy.VALUES[serializationStrategyFlag]
    }

    @Throws(IOException::class)
    private fun deserializeNullIndexList(dis: DataInputStream): List<Int> {
        var nullIndexListSize = dis.readInt()
        val nullIndexList: MutableList<Int> = ArrayList(nullIndexListSize)

        while (nullIndexListSize != 0) {
            nullIndexList.add(dis.readInt())
            nullIndexListSize--
        }

        return nullIndexList
    }

    override fun deserialize(topic: String, data: ByteArray?): List<Inner>? {
        if (data == null) return null

        try {
            DataInputStream(ByteArrayInputStream(data)).use { dis ->
                val serStrategy =
                    parseSerializationStrategyFlag(dis.readByte().toInt())

                var nullIndexList: List<Int>? = null
                if (serStrategy == Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE) {
                    // In CONSTANT_SIZE strategy, indexes of null entries are decoded from a null index list
                    nullIndexList = deserializeNullIndexList(dis)
                }

                val size = dis.readInt()
                val deserializedList = createListInstance(size)

                for (i in 0..<size) {
                    val entrySize =
                        if (serStrategy == Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE) primitiveSize!!
                        else dis.readInt()

                    if (
                        entrySize == Serdes.ListSerde.NULL_ENTRY_VALUE
                        || nullIndexList != null
                        && nullIndexList.contains(i)
                    ) {
                        deserializedList.add(null as Inner)
                        continue
                    }

                    val payload = ByteArray(entrySize)
                    if (dis.read(payload) == -1) {
                        log.error("Ran out of bytes in serialized list")
                        log.trace(
                            "Deserialized list so far: {}",
                            deserializedList
                        ) // avoid logging actual data above TRACE level since it may contain sensitive information
                        throw SerializationException("End of the stream was reached prematurely")
                    }
                    deserializedList.add(inner.deserialize(topic, payload))
                }
                return deserializedList
            }
        } catch (e: IOException) {
            throw KafkaException("Unable to deserialize into a List", e)
        }
    }

    override fun close() {
        if (this::inner.isInitialized) inner.close()
    }

    companion object {

        private val FIXED_LENGTH_DESERIALIZERS: Map<Class<out Deserializer<*>?>, Int> = mkMap(
            mkEntry(ShortDeserializer::class.java, java.lang.Short.BYTES),
            mkEntry(IntegerDeserializer::class.java, Integer.BYTES),
            mkEntry(FloatDeserializer::class.java, java.lang.Float.BYTES),
            mkEntry(LongDeserializer::class.java, java.lang.Long.BYTES),
            mkEntry(DoubleDeserializer::class.java, java.lang.Double.BYTES),
            mkEntry(UUIDDeserializer::class.java, 36)
        )
    }
}
