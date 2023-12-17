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

package org.apache.kafka.server.common.serialization

import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.protocol.Readable
import org.apache.kafka.common.protocol.Writable
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.server.common.ApiMessageAndVersion

/**
 * This is an implementation of [RecordSerde] with [ApiMessageAndVersion] but implementors need to implement
 * [apiMessageFor] to return a [ApiMessage] instance for the given `apiKey`.
 *
 * This can be used as the underlying serialization mechanism for records defined with [ApiMessage]s.
 *
 * Serialization format for the given `ApiMessageAndVersion` is below:
 *
 * ```markdown
 * [data_frame_version header message]
 * header => [api_key version]
 *
 * data_frame_version   : This is the header version, current value is 0. Header includes both api_key and version.
 * api_key              : apiKey of ApiMessageAndVersion object.
 * version              : version of ApiMessageAndVersion object.
 * message              : serialized message of ApiMessageAndVersion object.
 * ```
 */
abstract class AbstractApiMessageSerde() : RecordSerde<ApiMessageAndVersion> {

    override fun recordSize(
        data: ApiMessageAndVersion,
        serializationCache: ObjectSerializationCache,
    ): Int {
        var size = DEFAULT_FRAME_VERSION_SIZE
        size += sizeOfUnsignedVarint(data.message.apiKey().toInt())
        size += sizeOfUnsignedVarint(data.version.toInt())
        size += data.message.size(serializationCache, data.version)
        return size
    }

    override fun write(
        data: ApiMessageAndVersion,
        serializationCache: ObjectSerializationCache,
        out: Writable,
    ) {
        out.writeUnsignedVarint(DEFAULT_FRAME_VERSION.toInt())
        out.writeUnsignedVarint(data.message.apiKey().toInt())
        out.writeUnsignedVarint(data.version.toInt())
        data.message.write(out, serializationCache, data.version)
    }

    override fun read(
        input: Readable,
        size: Int,
    ): ApiMessageAndVersion {
        val frameVersion = unsignedIntToShort(input, "frame version")
        if (frameVersion.toInt() == 0) throw MetadataParseException(
            "Could not deserialize metadata record with frame version 0. Note that upgrades " +
                    "from the preview release of KRaft in 2.8 to newer versions are not supported."
        )
        else if (frameVersion != DEFAULT_FRAME_VERSION) throw MetadataParseException(
            "Could not deserialize metadata record due to unknown frame version " +
                    "$frameVersion(only frame version $DEFAULT_FRAME_VERSION is supported)"
        )

        val apiKey = unsignedIntToShort(input, "type")
        val version = unsignedIntToShort(input, "version")
        val record: ApiMessage
        try {
            record = apiMessageFor(apiKey)
        } catch (e: Exception) {
            throw MetadataParseException(e)
        }
        try {
            record.read(input, version)
        } catch (e: Exception) {
            throw MetadataParseException("Failed to deserialize record with type $apiKey", e)
        }
        if (input.remaining() > 0) {
            throw MetadataParseException("Found ${input.remaining()} byte(s) of garbage after $apiKey")
        }
        return ApiMessageAndVersion(record, version)
    }

    /**
     * Return `ApiMessage` instance for the given `apiKey`. This is used while deserializing the bytes
     * payload into the respective `ApiMessage` in [read] method.
     *
     * @param apiKey apiKey for which a `ApiMessage` to be created.
     */
    abstract fun apiMessageFor(apiKey: Short): ApiMessage

    companion object {

        private val DEFAULT_FRAME_VERSION: Short = 1

        private val DEFAULT_FRAME_VERSION_SIZE = sizeOfUnsignedVarint(DEFAULT_FRAME_VERSION.toInt())

        private fun unsignedIntToShort(input: Readable, entity: String): Short {
            val value: Int
            try {
                value = input.readUnsignedVarint()
            } catch (e: Exception) {
                throw MetadataParseException("Error while reading $entity", e)
            }
            if (value > Short.MAX_VALUE) throw MetadataParseException("Value for $entity was too large.")
            return value.toShort()
        }
    }
}
