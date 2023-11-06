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

package org.apache.kafka.clients.consumer

import org.apache.kafka.common.utils.Serializer
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * This test case ensures OffsetAndMetadata class is serializable and is serialization compatible.
 * Note: this ensures that the current code can deserialize data serialized with older versions of the code,
 * but not the reverse. That is, older code won't necessarily be able to deserialize data serialized with newer code.
 */
class OffsetAndMetadataTest {
    @Test
    fun testInvalidNegativeOffset() {
        assertFailsWith<IllegalArgumentException> {
            OffsetAndMetadata(
                offset = -239L,
                leaderEpoch = 15,
                metadata = "",
            )
        }
    }

    @Test
    @Throws(IOException::class, ClassNotFoundException::class)
    fun testSerializationRoundtrip() {
        checkSerde(OffsetAndMetadata(offset = 239L, leaderEpoch = 15, metadata = "blah"))
        checkSerde(OffsetAndMetadata(offset = 239L, metadata = "blah"))
        checkSerde(OffsetAndMetadata(offset = 239L))
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun checkSerde(offsetAndMetadata: OffsetAndMetadata) {
        val bytes = Serializer.serialize(offsetAndMetadata)
        val deserialized = Serializer.deserialize(bytes) as OffsetAndMetadata
        assertEquals(offsetAndMetadata, deserialized)
    }

    @Test
    @Throws(IOException::class, ClassNotFoundException::class)
    fun testDeserializationCompatibilityBeforeLeaderEpoch() {
        val fileName = "serializedData/offsetAndMetadataBeforeLeaderEpoch"
        val deserializedObject = Serializer.deserialize(fileName)
        assertEquals(OffsetAndMetadata(offset = 10, metadata = "test commit metadata"), deserializedObject)
    }

    @Test
    @Throws(IOException::class, ClassNotFoundException::class)
    fun testDeserializationCompatibilityWithLeaderEpoch() {
        val fileName = "serializedData/offsetAndMetadataWithLeaderEpoch"
        val deserializedObject = Serializer.deserialize(fileName)
        assertEquals(
            expected = OffsetAndMetadata(offset = 10, leaderEpoch = 235, metadata = "test commit metadata"),
            actual = deserializedObject,
        )
    }
}
