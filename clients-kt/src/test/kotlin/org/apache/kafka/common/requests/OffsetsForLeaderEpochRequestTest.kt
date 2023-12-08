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

package org.apache.kafka.common.requests

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.Builder.Companion.forConsumer
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.Builder.Companion.forFollower
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OffsetsForLeaderEpochRequestTest {

    @Test
    fun testForConsumerRequiresVersion3() {
        val builder = forConsumer(OffsetForLeaderTopicCollection())
        for (version in 0..2) assertFailsWith<UnsupportedVersionException> {
            builder.build(version.toShort())
        }
        for (version in 3..ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion()) {
            val request = builder.build(version.toShort())
            assertEquals(OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID, request.replicaId())
        }
    }

    @Test
    fun testDefaultReplicaId() {
        for (version in ApiKeys.OFFSET_FOR_LEADER_EPOCH.allVersions()) {
            val replicaId = 1
            val builder = forFollower(
                version = version,
                epochsByPartition = OffsetForLeaderTopicCollection(),
                replicaId = replicaId
            )
            val request = builder.build()
            val parsed = OffsetsForLeaderEpochRequest.parse(
                buffer = request.serialize(),
                version = version,
            )
            if (version < 3) assertEquals(
                expected = OffsetsForLeaderEpochRequest.DEBUGGING_REPLICA_ID,
                actual = parsed.replicaId()
            ) else assertEquals(replicaId, parsed.replicaId())
        }
    }
}
