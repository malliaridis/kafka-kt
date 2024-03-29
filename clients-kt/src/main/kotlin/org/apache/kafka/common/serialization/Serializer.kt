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
import org.apache.kafka.common.header.Headers

/**
 * An interface for converting objects to bytes.
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 *
 * Implement [org.apache.kafka.common.ClusterResourceListener] to receive cluster metadata once it's
 * available. Please see the class documentation for ClusterResourceListener for more information.
 *
 * @param T Type to be serialized from.
 */
interface Serializer<T> : Closeable {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    fun configure(configs: Map<String, *>, isKey: Boolean) = Unit // intentionally Unit

    /**
     * Convert `data` into a byte array.
     *
     * @param topic topic associated with data
     * @param data typed data
     * @return serialized bytes
     */
    fun serialize(topic: String, data: T?): ByteArray?

    /**
     * Convert `data` into a byte array.
     *
     * @param topic topic associated with data
     * @param headers headers associated with the record
     * @param data typed data
     * @return serialized bytes
     */
    fun serialize(topic: String, headers: Headers, data: T?): ByteArray? {
        return serialize(topic, data)
    }

    /**
     * Close this serializer.
     *
     * This method must be idempotent as it may be called multiple times.
     */
    override fun close() {
        // intentionally left blank
    }
}
