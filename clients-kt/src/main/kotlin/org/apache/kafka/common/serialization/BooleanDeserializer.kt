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
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers

class BooleanDeserializer : Deserializer<Boolean?> {

    override fun deserialize(topic: String, data: ByteArray?): Boolean? {
        if (data == null) return null
        if (data.size != 1) throw SerializationException("Size of data received by BooleanDeserializer is not 1")

        return when(data[0]) {
            TRUE -> true
            FALSE -> false
            else -> throw SerializationException("Unexpected byte received by BooleanDeserializer: " + data[0])
        }
    }

    override fun deserialize(topic: String, headers: Headers, data: ByteBuffer?): Boolean? {
        if (data == null) return null
        if (data.remaining() != 1) throw SerializationException("Size of data received by BooleanDeserializer is not 1")

        val b = data[data.position()]
        return when (b) {
            TRUE -> true
            FALSE -> false
            else -> throw SerializationException("Unexpected byte received by BooleanDeserializer: $b")
        }
    }

    companion object {

        private const val TRUE: Byte = 0x01

        private const val FALSE: Byte = 0x00
    }
}
