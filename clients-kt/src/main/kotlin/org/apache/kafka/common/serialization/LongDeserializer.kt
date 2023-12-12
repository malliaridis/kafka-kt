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

class LongDeserializer : Deserializer<Long?> {

    override fun deserialize(topic: String, data: ByteArray?): Long? = when {
        data == null -> null
        data.size != 8 -> throw SerializationException("Size of data received by LongDeserializer is not 8")
        else -> {
            var value: Long = 0
            data.forEach { b ->
                value = value shl 8
                value = value or (b.toInt() and 0xFF).toLong()
            }
            value
        }
    }

    override fun deserialize(topic: String, headers: Headers, data: ByteBuffer?): Long? = when {
        data == null -> null
        data.remaining() != 8 -> throw SerializationException("Size of data received by LongDeserializer is not 8")
        else -> data.getLong(data.position())
    }
}
