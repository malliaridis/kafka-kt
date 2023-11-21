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
import org.apache.kafka.common.message.LeaderChangeMessage
import org.apache.kafka.common.message.LeaderChangeMessage.Voter
import org.apache.kafka.common.message.SnapshotFooterRecord
import org.apache.kafka.common.message.SnapshotHeaderRecord
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.record.ControlRecordUtils.deserializeLeaderChangeMessage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ControlRecordUtilsTest {

    @Test
    fun testCurrentVersions() {
        // If any of these asserts fail, please make sure that Kafka supports reading and
        // writing the latest version for these records.
        assertEquals(
            expected = LeaderChangeMessage.HIGHEST_SUPPORTED_VERSION,
            actual = ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION,
        )
        assertEquals(
            expected = SnapshotHeaderRecord.HIGHEST_SUPPORTED_VERSION,
            actual = ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION,
        )
        assertEquals(
            expected = SnapshotFooterRecord.HIGHEST_SUPPORTED_VERSION,
            actual = ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION,
        )
    }

    @Test
    fun testInvalidControlRecordType() {
        val thrown = assertFailsWith<IllegalArgumentException> { testDeserializeRecord(ControlRecordType.COMMIT) }
        assertEquals(
            expected = "Expected LEADER_CHANGE control record type(2), but found COMMIT",
            actual = thrown.message,
        )
    }

    @Test
    fun testDeserializeByteData() {
        testDeserializeRecord(ControlRecordType.LEADER_CHANGE)
    }

    private fun testDeserializeRecord(controlRecordType: ControlRecordType) {
        val leaderId = 1
        val voterId = 2
        val data = LeaderChangeMessage()
            .setLeaderId(leaderId)
            .setVoters(listOf(Voter().setVoterId(voterId)))
        val valueBuffer = ByteBuffer.allocate(256)
        data.write(ByteBufferAccessor(valueBuffer), ObjectSerializationCache(), data.highestSupportedVersion())
        valueBuffer.flip()
        val keyData = byteArrayOf(0, 0, 0, controlRecordType.type.toByte())
        val record = DefaultRecord(
            sizeInBytes = 256,
            attributes = 0.toByte(),
            offset = 0,
            timestamp = 0L,
            sequence = 0,
            key = ByteBuffer.wrap(keyData),
            value = valueBuffer,
            headers = emptyArray(),
        )
        val deserializedData = deserializeLeaderChangeMessage(record)
        assertEquals(leaderId, deserializedData.leaderId)
        assertEquals(listOf(Voter().setVoterId(voterId)), deserializedData.voters)
    }
}
