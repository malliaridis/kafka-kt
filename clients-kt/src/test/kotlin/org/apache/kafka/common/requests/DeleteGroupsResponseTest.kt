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

import org.apache.kafka.common.message.DeleteGroupsResponseData
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DeleteGroupsResponseTest {

    @Test
    fun testGetErrorWithExistingGroupIds() {
        assertEquals(Errors.NONE, deleteGroupsResponse[GROUP_ID_1])
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, deleteGroupsResponse[GROUP_ID_2])

        val expectedErrors = mapOf(
            GROUP_ID_1 to Errors.NONE,
            GROUP_ID_2 to Errors.GROUP_AUTHORIZATION_FAILED,
        )
        assertEquals(expectedErrors, deleteGroupsResponse.errors())

        val expectedErrorCounts = mapOf(
            Errors.NONE to 1,
            Errors.GROUP_AUTHORIZATION_FAILED to 1,
        )
        assertEquals(expectedErrorCounts, deleteGroupsResponse.errorCounts())
    }

    @Test
    fun testGetErrorWithInvalidGroupId() {
        assertFailsWith<IllegalArgumentException> { deleteGroupsResponse["invalid-group-id"] }
    }

    @Test
    fun testGetThrottleTimeMs() {
        assertEquals(THROTTLE_TIME_MS, deleteGroupsResponse.throttleTimeMs())
    }

    companion object {

        private const val GROUP_ID_1 = "groupId1"

        private const val GROUP_ID_2 = "groupId2"

        private const val THROTTLE_TIME_MS = 10

        private val deleteGroupsResponse = DeleteGroupsResponse(
            DeleteGroupsResponseData()
                .setResults(
                    DeletableGroupResultCollection(
                        listOf(
                            DeletableGroupResult()
                                .setGroupId(GROUP_ID_1)
                                .setErrorCode(Errors.NONE.code),
                            DeletableGroupResult()
                                .setGroupId(GROUP_ID_2)
                                .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
                        ).iterator(),
                    )
                )
                .setThrottleTimeMs(THROTTLE_TIME_MS)
        )
    }
}
