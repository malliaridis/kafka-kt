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

import org.apache.kafka.common.requests.JoinGroupRequest
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ConsumerGroupMetadataTest {

    private val groupId = "group"

    @Test
    fun testAssignmentConstructor() {
        val memberId = "member"
        val generationId = 2
        val groupInstanceId = "instance"
        val metadata = ConsumerGroupMetadata(
            groupId = groupId,
            generationId = generationId,
            memberId = memberId,
            groupInstanceId = groupInstanceId,
        )

        assertEquals(groupId, metadata.groupId)
        assertEquals(generationId, metadata.generationId)
        assertEquals(memberId, metadata.memberId)
        assertNotNull(metadata.groupInstanceId)
        assertEquals(groupInstanceId, metadata.groupInstanceId)
    }

    @Test
    fun testGroupIdConstructor() {
        val (groupId1, generationId, memberId, groupInstanceId) = ConsumerGroupMetadata(groupId)
        assertEquals(groupId, groupId1)
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, generationId)
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, memberId)
        assertNull(groupInstanceId)
    }

    @Test
    @Disabled("Kotlin Migration: groupId is not nullable")
    fun testInvalidGroupId() {
//        val memberId = "member"
//        val generationId = 2
//        assertFailsWith<NullPointerException> {
//            ConsumerGroupMetadata(
//                groupId = null,
//                generationId = generationId,
//                memberId = memberId,
//                groupInstanceId = null,
//            )
//        }
    }

    @Test
    @Disabled("Kotlin Migration: memberId is not nullable")
    fun testInvalidMemberId() {
//        val generationId = 2
//        assertFailsWith<NullPointerException> {
//            ConsumerGroupMetadata(
//                groupId = groupId,
//                generationId = generationId,
//                memberId = null,
//                groupInstanceId = null,
//            )
//        }
    }

    @Test
    @Disabled("Kotlin Migration: In Kotlin groupInstanceId is a nullable string, and therefore can be null.")
    fun testInvalidInstanceId() {
//        val memberId = "member"
//        val generationId = 2
//        assertFailsWith<NullPointerException> {
//            ConsumerGroupMetadata(
//                groupId = groupId,
//                generationId = generationId,
//                memberId = memberId,
//                groupInstanceId = null,
//            )
//        }
    }
}
