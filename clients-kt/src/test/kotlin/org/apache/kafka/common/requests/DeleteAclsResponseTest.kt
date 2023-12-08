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

import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.DeleteAclsResponseData
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DeleteAclsResponseTest {

    @Test
    fun shouldThrowOnV0IfNotLiteral() {
        assertFailsWith<UnsupportedVersionException> {
            DeleteAclsResponse(
                data = DeleteAclsResponseData()
                    .setThrottleTimeMs(10)
                    .setFilterResults(listOf(PREFIXED_RESPONSE)),
                version = V0,
            )
        }
    }

    @Test
    fun shouldThrowOnIfUnknown() {
        assertFailsWith<IllegalArgumentException> {
            DeleteAclsResponse(
                data = DeleteAclsResponseData()
                    .setThrottleTimeMs(10)
                    .setFilterResults(listOf(UNKNOWN_RESPONSE)),
                version = V1,
            )
        }
    }

    @Test
    fun shouldRoundTripV0() {
        val original = DeleteAclsResponse(
            data = DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(listOf(LITERAL_RESPONSE)),
            version = V0,
        )
        val buffer = original.serialize(V0)
        val result = DeleteAclsResponse.parse(buffer, V0)
        assertEquals(original.filterResults(), result.filterResults())
    }

    @Test
    fun shouldRoundTripV1() {
        val original = DeleteAclsResponse(
            data = DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(listOf(LITERAL_RESPONSE, PREFIXED_RESPONSE)),
            version = V1,
        )
        val buffer = original.serialize(V1)
        val result = DeleteAclsResponse.parse(buffer, V1)
        assertEquals(original.filterResults(), result.filterResults())
    }

    companion object {
        
        private const val V0: Short = 0
        
        private const val V1: Short = 1
        
        private val LITERAL_ACL1 = DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.TOPIC.code)
            .setResourceName("foo")
            .setPatternType(PatternType.LITERAL.code)
            .setPrincipal("User:ANONYMOUS")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.READ.code)
            .setPermissionType(AclPermissionType.DENY.code)
        
        private val LITERAL_ACL2 = DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.GROUP.code)
            .setResourceName("group")
            .setPatternType(PatternType.LITERAL.code)
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.WRITE.code)
            .setPermissionType(AclPermissionType.ALLOW.code)

        private val PREFIXED_ACL1 = DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.GROUP.code)
            .setResourceName("prefix")
            .setPatternType(PatternType.PREFIXED.code)
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.CREATE.code)
            .setPermissionType(AclPermissionType.ALLOW.code)

        private val UNKNOWN_ACL = DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.UNKNOWN.code)
            .setResourceName("group")
            .setPatternType(PatternType.LITERAL.code)
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.WRITE.code)
            .setPermissionType(AclPermissionType.ALLOW.code)

        private val LITERAL_RESPONSE = DeleteAclsFilterResult()
            .setMatchingAcls(listOf(LITERAL_ACL1, LITERAL_ACL2))

        private val PREFIXED_RESPONSE = DeleteAclsFilterResult()
            .setMatchingAcls(listOf(LITERAL_ACL1, PREFIXED_ACL1))

        private val UNKNOWN_RESPONSE = DeleteAclsFilterResult()
            .setMatchingAcls(listOf(UNKNOWN_ACL))
    }
}
