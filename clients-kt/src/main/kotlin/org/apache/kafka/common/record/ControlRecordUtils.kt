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

import org.apache.kafka.common.message.LeaderChangeMessage
import org.apache.kafka.common.message.SnapshotFooterRecord
import org.apache.kafka.common.message.SnapshotHeaderRecord
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer

/**
 * Utility class for easy interaction with control records.
 */
object ControlRecordUtils {

    const val LEADER_CHANGE_CURRENT_VERSION: Short = 0

    const val SNAPSHOT_HEADER_CURRENT_VERSION: Short = 0

    const val SNAPSHOT_FOOTER_CURRENT_VERSION: Short = 0

    fun deserializeLeaderChangeMessage(record: Record): LeaderChangeMessage {
        val recordType = ControlRecordType.parse(record.key()!!)
        require(recordType == ControlRecordType.LEADER_CHANGE) {
            "Expected LEADER_CHANGE control record type(2), but found $recordType"
        }

        return deserializeLeaderChangeMessage(record.value()!!.duplicate())
    }

    fun deserializeLeaderChangeMessage(data: ByteBuffer): LeaderChangeMessage {
        val byteBufferAccessor = ByteBufferAccessor(data.duplicate())
        return LeaderChangeMessage(byteBufferAccessor, LEADER_CHANGE_CURRENT_VERSION)
    }

    fun deserializedSnapshotHeaderRecord(record: Record): SnapshotHeaderRecord {
        val recordType = ControlRecordType.parse(record.key()!!)
        require(recordType == ControlRecordType.SNAPSHOT_HEADER) {
            "Expected SNAPSHOT_HEADER control record type(3), but found $recordType"
        }

        return deserializedSnapshotHeaderRecord(record.value()!!.duplicate())
    }

    fun deserializedSnapshotHeaderRecord(data: ByteBuffer): SnapshotHeaderRecord {
        val byteBufferAccessor = ByteBufferAccessor(data.duplicate())
        return SnapshotHeaderRecord(byteBufferAccessor, SNAPSHOT_HEADER_CURRENT_VERSION)
    }

    fun deserializedSnapshotFooterRecord(record: Record): SnapshotFooterRecord {
        val recordType = ControlRecordType.parse(record.key()!!)
        require(recordType == ControlRecordType.SNAPSHOT_FOOTER) {
            "Expected SNAPSHOT_FOOTER control record type(4), but found $recordType"
        }

        return deserializedSnapshotFooterRecord(record.value()!!.duplicate())
    }

    fun deserializedSnapshotFooterRecord(data: ByteBuffer): SnapshotFooterRecord {
        val byteBufferAccessor = ByteBufferAccessor(data.duplicate())
        return SnapshotFooterRecord(byteBufferAccessor, SNAPSHOT_FOOTER_CURRENT_VERSION)
    }
}
