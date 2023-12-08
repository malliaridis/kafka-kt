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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class MemberDescriptionTest {

    @Test
    fun testEqualsWithoutGroupInstanceId() {
        val dynamicMemberDescription = MemberDescription(
            memberId = MEMBER_ID,
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT,
        )
        val identityDescription = MemberDescription(
            memberId = MEMBER_ID,
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT,
        )
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, dynamicMemberDescription)
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), dynamicMemberDescription.hashCode())

        // Check self equality.
        assertEquals(dynamicMemberDescription, dynamicMemberDescription)
        assertEquals(dynamicMemberDescription, identityDescription)
        assertEquals(dynamicMemberDescription.hashCode(), identityDescription.hashCode())
    }

    @Test
    fun testEqualsWithGroupInstanceId() {
        // Check self equality.
        assertEquals(STATIC_MEMBER_DESCRIPTION, STATIC_MEMBER_DESCRIPTION)
        val identityDescription = MemberDescription(
            memberId = MEMBER_ID,
            groupInstanceId = INSTANCE_ID,
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT,
        )
        assertEquals(STATIC_MEMBER_DESCRIPTION, identityDescription)
        assertEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), identityDescription.hashCode())
    }

    @Test
    fun testNonEqual() {
        val newMemberDescription = MemberDescription(
            memberId = "new_member",
            groupInstanceId = INSTANCE_ID,
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT,
        )
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newMemberDescription)
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newMemberDescription.hashCode())
        val newInstanceDescription = MemberDescription(
            memberId = MEMBER_ID,
            groupInstanceId = "new_instance",
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT
        )
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newInstanceDescription)
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newInstanceDescription.hashCode())
    }

    companion object {

        private const val MEMBER_ID = "member_id"

        private const val INSTANCE_ID = "instanceId"

        private const val CLIENT_ID = "client_id"

        private const val HOST = "host"

        private val ASSIGNMENT = MemberAssignment(setOf(TopicPartition(topic = "topic", partition = 1)))

        private val STATIC_MEMBER_DESCRIPTION = MemberDescription(
            memberId = MEMBER_ID,
            groupInstanceId = INSTANCE_ID,
            clientId = CLIENT_ID,
            host = HOST,
            assignment = ASSIGNMENT,
        )
    }
}
