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

import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.CreateAclsRequestData
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CreateAclsRequestTest {

    @Test
    fun shouldThrowOnV0IfNotLiteral() {
        assertFailsWith<UnsupportedVersionException> { CreateAclsRequest(data(PREFIXED_ACL1), V0) }
    }

    @Test
    fun shouldThrowOnIfUnknown() {
        assertFailsWith<IllegalArgumentException> { CreateAclsRequest(data(UNKNOWN_ACL1), V0) }
    }

    @Test
    fun shouldRoundTripV0() {
        val original = CreateAclsRequest(data(LITERAL_ACL1, LITERAL_ACL2), V0)
        val buffer = original.serialize()
        val result = CreateAclsRequest.parse(buffer, V0)
        assertRequestEquals(original, result)
    }

    @Test
    fun shouldRoundTripV1() {
        val original = CreateAclsRequest(data(LITERAL_ACL1, PREFIXED_ACL1), V1)
        val buffer = original.serialize()
        val result = CreateAclsRequest.parse(buffer, V1)
        assertRequestEquals(original, result)
    }

    companion object {
        private const val V0: Short = 0
        private const val V1: Short = 1
        private val LITERAL_ACL1 = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "foo",
                patternType = PatternType.LITERAL,
            ),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.READ,
                permissionType = AclPermissionType.DENY,
            ),
        )
        private val LITERAL_ACL2 = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.GROUP,
                name = "group",
                patternType = PatternType.LITERAL,
            ),
            entry = AccessControlEntry(
                principal = "User:*",
                host = "127.0.0.1",
                operation = AclOperation.WRITE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )
        private val PREFIXED_ACL1 = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.GROUP,
                name = "prefix",
                patternType = PatternType.PREFIXED,
            ),
            entry = AccessControlEntry(
                principal = "User:*",
                host = "127.0.0.1",
                operation = AclOperation.CREATE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )
        private val UNKNOWN_ACL1 = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.UNKNOWN,
                name = "unknown",
                patternType = PatternType.LITERAL,
            ),
            entry = AccessControlEntry(
                principal = "User:*",
                host = "127.0.0.1",
                operation = AclOperation.CREATE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )

        private fun assertRequestEquals(original: CreateAclsRequest, actual: CreateAclsRequest) {
            assertEquals(
                expected = original.aclCreations().size,
                actual = actual.aclCreations().size,
                message = "Number of Acls wrong",
            )
            for (idx in original.aclCreations().indices) {
                val originalBinding = CreateAclsRequest.aclBinding(original.aclCreations()[idx])
                val actualBinding = CreateAclsRequest.aclBinding(actual.aclCreations()[idx])
                assertEquals(originalBinding, actualBinding)
            }
        }

        private fun data(vararg acls: AclBinding): CreateAclsRequestData {
            val aclCreations = acls.map(CreateAclsRequest::aclCreation)
            return CreateAclsRequestData().setCreations(aclCreations)
        }
    }
}
