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
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EndTransactionMarkerTest {

    @Test
    fun testUnknownControlTypeNotAllowed() {
        assertFailsWith<IllegalArgumentException> {
            EndTransactionMarker(controlType = ControlRecordType.UNKNOWN, coordinatorEpoch = 24)
        }
    }

    @Test
    fun testCannotDeserializeUnknownControlType() {
        assertFailsWith<IllegalArgumentException> {
            EndTransactionMarker.deserializeValue(
                type = ControlRecordType.UNKNOWN,
                value = ByteBuffer.wrap(ByteArray(0)),
            )
        }
    }

    @Test
    fun testIllegalNegativeVersion() {
        val buffer = ByteBuffer.allocate(2)
        buffer.putShort(-1)
        buffer.flip()
        assertFailsWith<InvalidRecordException> {
            EndTransactionMarker.deserializeValue(
                type = ControlRecordType.ABORT,
                value = buffer,
            )
        }
    }

    @Test
    fun testNotEnoughBytes() {
        assertFailsWith<InvalidRecordException> {
            EndTransactionMarker.deserializeValue(
                type = ControlRecordType.COMMIT,
                value = ByteBuffer.wrap(ByteArray(0)),
            )
        }
    }

    @Test
    fun testSerde() {
        val coordinatorEpoch = 79
        val marker = EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
        val buffer = marker.serializeValue()
        val deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer)
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch)
    }

    @Test
    fun testDeserializeNewerVersion() {
        val coordinatorEpoch = 79
        val buffer = ByteBuffer.allocate(8)
        buffer.putShort(5)
        buffer.putInt(coordinatorEpoch)
        buffer.putShort(0) // unexpected data
        buffer.flip()
        val deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer)
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch)
    }
}
