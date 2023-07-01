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

package org.apache.kafka.common.record

import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * This class represents the control record which is written to the log to indicate the completion
 * of a transaction. The record key specifies the [control type][ControlRecordType] and the value
 * embeds information useful for write validation (for now, just the coordinator epoch).
 */
class EndTransactionMarker(
    val controlType: ControlRecordType,
    val coordinatorEpoch: Int,
) {

    init {
        ensureTransactionMarkerControlType(controlType)
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("coordinatorEpoch"),
    )
    fun coordinatorEpoch(): Int = coordinatorEpoch

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("controlType"),
    )
    fun controlType(): ControlRecordType = controlType

    private fun buildRecordValue(): Struct {
        val struct = Struct(END_TXN_MARKER_SCHEMA_VERSION_V0)
        struct["version"] = CURRENT_END_TXN_MARKER_VERSION
        struct["coordinator_epoch"] = coordinatorEpoch
        return struct
    }

    fun serializeValue(): ByteBuffer {
        val valueStruct = buildRecordValue()
        val value = ByteBuffer.allocate(valueStruct.sizeOf())
        valueStruct.writeTo(value)
        value.flip()
        return value
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as EndTransactionMarker
        return coordinatorEpoch == that.coordinatorEpoch && controlType == that.controlType
    }

    override fun hashCode(): Int {
        var result = controlType.hashCode()
        result = 31 * result + coordinatorEpoch
        return result
    }

    companion object {

        private val log = LoggerFactory.getLogger(EndTransactionMarker::class.java)

        private const val CURRENT_END_TXN_MARKER_VERSION: Short = 0

        private val END_TXN_MARKER_SCHEMA_VERSION_V0 = Schema(
            Field("version", Type.INT16),
            Field("coordinator_epoch", Type.INT32)
        )

        const val CURRENT_END_TXN_MARKER_VALUE_SIZE = 6

        val CURRENT_END_TXN_SCHEMA_RECORD_SIZE = DefaultRecord.sizeInBytes(
            0, 0L,
            ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE,
            CURRENT_END_TXN_MARKER_VALUE_SIZE,
            Record.EMPTY_HEADERS
        )

        private fun ensureTransactionMarkerControlType(type: ControlRecordType) {
            require(type == ControlRecordType.COMMIT || type == ControlRecordType.ABORT) {
                "Invalid control record type for end transaction marker$type"
            }
        }

        fun deserialize(record: Record): EndTransactionMarker {
            val type = ControlRecordType.parse(record.key()!!)
            return deserializeValue(type, record.value()!!)
        }

        fun deserializeValue(type: ControlRecordType, value: ByteBuffer): EndTransactionMarker {
            ensureTransactionMarkerControlType(type)
            if (value.remaining() < CURRENT_END_TXN_MARKER_VALUE_SIZE) throw InvalidRecordException(
                "Invalid value size found for end transaction marker. Must have at least " +
                        "$CURRENT_END_TXN_MARKER_VALUE_SIZE bytes, but found only ${value.remaining()}"
            )
            val version = value.getShort(0)
            if (version < 0) throw InvalidRecordException(
                "Invalid version found for end transaction marker: $version. May indicate data " +
                        "corruption"
            )
            if (version > CURRENT_END_TXN_MARKER_VERSION) log.debug(
                "Received end transaction marker value version {}. Parsing as version {}",
                version,
                CURRENT_END_TXN_MARKER_VERSION,
            )
            val coordinatorEpoch = value.getInt(2)
            return EndTransactionMarker(type, coordinatorEpoch)
        }
    }
}
