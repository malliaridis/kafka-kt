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

import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets
import java.nio.charset.UnsupportedCharsetException
import java.util.*
import org.apache.kafka.common.errors.SerializationException

/**
 * We are converting the byte array to String before deserializing to UUID. String encoding defaults
 * to UTF8 and can be customized by setting the property key.deserializer.encoding,
 * value.deserializer.encoding or deserializer.encoding. The first two take precedence over the
 * last.
 */
class UUIDDeserializer : Deserializer<UUID> {

    private var encoding = StandardCharsets.UTF_8.name()

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        val propertyName = if (isKey) "key.deserializer.encoding" else "value.deserializer.encoding"
        var encodingValue = configs[propertyName]

        if (encodingValue == null) encodingValue = configs["deserializer.encoding"]
        if (encodingValue is String) encoding = encodingValue
    }

    override fun deserialize(topic: String, data: ByteArray?): UUID? {
        return try {
            if (data == null) null else UUID.fromString(String(data, charset(encoding)))
        } catch (cause: UnsupportedCharsetException) {
            throw SerializationException(
                "Error when deserializing byte[] to UUID due to unsupported encoding $encoding",
                cause
            )
        } catch (cause: IllegalArgumentException) {
            throw SerializationException("Error parsing data into UUID", cause)
        }
    }
}