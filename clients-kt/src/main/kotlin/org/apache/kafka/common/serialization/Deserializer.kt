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

import java.io.Closeable
import java.nio.ByteBuffer
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.utils.Utils.toNullableArray

/**
 * An interface for converting bytes to objects.
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 *
 * Implement [org.apache.kafka.common.ClusterResourceListener] to receive cluster metadata once it's
 * available. Please see the class documentation for ClusterResourceListener for more information.
 *
 * @param T Type to be deserialized into.
 */
interface Deserializer<T> : Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    fun configure(configs: Map<String, *>, isKey: Boolean) {
        // intentionally left blank
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data serialized bytes; may be null; implementations are recommended to handle null by
     * returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    fun deserialize(topic: String, data: ByteArray?): T?

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param headers headers associated with the record; may be empty.
     * @param data serialized bytes; may be null; implementations are recommended to handle null by
     * returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    fun deserialize(topic: String, headers: Headers, data: ByteArray?): T? = deserialize(topic, data)

    /**
     * Deserialize a record value from a ByteBuffer into a value or object.
     *
     * @param topic topic associated with the data
     * @param data serialized ByteBuffer; may be null; implementations are recommended to handle null by returning
     * a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    fun deserialize(topic: String, data: ByteBuffer?): T? = deserialize(topic, toNullableArray(data))

    /**
     * Deserialize a record value from a ByteBuffer into a value or object.
     *
     * @param topic topic associated with the data
     * @param headers headers associated with the record; may be empty.
     * @param data serialized ByteBuffer; may be null; implementations are recommended to handle null by returning
     * a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    fun deserialize(topic: String, headers: Headers, data: ByteBuffer?): T? =
        deserialize(topic, headers, toNullableArray(data))

    /**
     * Close this deserializer.
     *
     * This method must be idempotent as it may be called multiple times.
     */
    override fun close() {
        // intentionally left blank
    }
}
