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

import java.nio.charset.StandardCharsets
import java.nio.charset.UnsupportedCharsetException
import org.apache.kafka.common.errors.SerializationException

/**
 * String encoding defaults to UTF8 and can be customized by setting the property
 * key.serializer.encoding, value.serializer.encoding or serializer.encoding. The first two take
 * precedence over the last.
 */
class StringSerializer : Serializer<String?> {

    private var encoding = StandardCharsets.UTF_8.name()

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        val propertyName = if (isKey) "key.serializer.encoding" else "value.serializer.encoding"
        var encodingValue = configs[propertyName]
        if (encodingValue == null) encodingValue = configs["serializer.encoding"]
        if (encodingValue is String) encoding = encodingValue
    }

    override fun serialize(topic: String, data: String?): ByteArray? {
        return try {
            data?.toByteArray(charset(encoding))
        } catch (e: UnsupportedCharsetException) {
            throw SerializationException(
                "Error when serializing string to byte[] due to unsupported encoding $encoding"
            )
        }
    }
}
