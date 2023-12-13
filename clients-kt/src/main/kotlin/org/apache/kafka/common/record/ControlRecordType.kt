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

import java.nio.ByteBuffer
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.protocol.types.Field
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.types.Type
import org.slf4j.LoggerFactory

/**
 * Control records specify a schema for the record key which includes a version and type:
 *
 * - Key => Version Type
 * - Version => Int16
 * - Type => Int16
 *
 * In the future, the version can be bumped to indicate a new schema, but it must be backwards
 * compatible with the current schema. In general, this means we can add new fields, but we cannot
 * remove old ones.
 *
 * Note that control records are not considered for compaction by the log cleaner.
 *
 * The schema for the value field is left to the control record type to specify.
 */
enum class ControlRecordType(val type: Short) {

    // UNKNOWN is used to indicate a control type which the client is not aware of and should be
    // ignored
    UNKNOWN((-1).toShort()),

    ABORT(0.toShort()),

    COMMIT(1.toShort()),

    // Raft quorum related control messages.
    LEADER_CHANGE(2.toShort()),

    SNAPSHOT_HEADER(3.toShort()),

    SNAPSHOT_FOOTER(4.toShort());

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("type")
    )
    fun type(): Short = type

    fun recordKey(): Struct {
        require(this != UNKNOWN) { "Cannot serialize UNKNOWN control record type" }
        val struct = Struct(CONTROL_RECORD_KEY_SCHEMA_VERSION_V0)
        struct["version"] = CURRENT_CONTROL_RECORD_KEY_VERSION
        struct["type"] = type
        return struct
    }

    companion object {

        private val log = LoggerFactory.getLogger(ControlRecordType::class.java)

        const val CURRENT_CONTROL_RECORD_KEY_VERSION: Short = 0

        const val CURRENT_CONTROL_RECORD_KEY_SIZE = 4

        private val CONTROL_RECORD_KEY_SCHEMA_VERSION_V0 = Schema(
            Field("version", Type.INT16),
            Field("type", Type.INT16)
        )

        fun parseTypeId(key: ByteBuffer): Short {
            if (key.remaining() < CURRENT_CONTROL_RECORD_KEY_SIZE) throw InvalidRecordException(
                "Invalid value size found for end control record key. Must have at least " +
                        "$CURRENT_CONTROL_RECORD_KEY_SIZE bytes, but found only ${key.remaining()}"
            )
            val version = key.getShort(0)
            if (version < 0) throw InvalidRecordException(
                "Invalid version found for control record: $version. May indicate data corruption"
            )
            if (version != CURRENT_CONTROL_RECORD_KEY_VERSION) log.debug(
                "Received unknown control record key version {}. Parsing as version {}",
                version,
                CURRENT_CONTROL_RECORD_KEY_VERSION,
            )
            return key.getShort(2)
        }

        fun fromTypeId(typeId: Short): ControlRecordType = when (typeId.toInt()) {
            0 -> ABORT
            1 -> COMMIT
            2 -> LEADER_CHANGE
            3 -> SNAPSHOT_HEADER
            4 -> SNAPSHOT_FOOTER
            else -> UNKNOWN
        }

        fun parse(key: ByteBuffer): ControlRecordType = fromTypeId(parseTypeId(key))
    }
}
