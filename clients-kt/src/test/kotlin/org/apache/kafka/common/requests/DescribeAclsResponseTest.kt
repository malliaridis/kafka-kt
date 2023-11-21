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
import org.apache.kafka.common.message.DescribeAclsResponseData
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DescribeAclsResponseTest {
    
    @Test
    fun shouldThrowOnV0IfNotLiteral() {
        assertFailsWith<UnsupportedVersionException> {
            buildResponse(
                throttleTimeMs = 10,
                error = Errors.NONE,
                resources = listOf(PREFIXED_ACL1),
            ).serialize(V0)
        }
    }

    @Test
    fun shouldThrowIfUnknown() {
        assertFailsWith<IllegalArgumentException> {
            buildResponse(
                throttleTimeMs = 10,
                error = Errors.NONE,
                resources = listOf(UNKNOWN_ACL),
            ).serialize(V0)
        }
    }

    @Test
    fun shouldRoundTripV0() {
        val resources = listOf(LITERAL_ACL1, LITERAL_ACL2)
        val original = buildResponse(10, Errors.NONE, resources)
        val buffer = original.serialize(V0)
        val result = DescribeAclsResponse.parse(buffer, V0)
        assertResponseEquals(original, result)
        val result2 = buildResponse(
            throttleTimeMs = 10,
            error = Errors.NONE,
            resources = DescribeAclsResponse.aclsResources(DescribeAclsResponse.aclBindings(resources)),
        )
        assertResponseEquals(original, result2)
    }

    @Test
    fun shouldRoundTripV1() {
        val resources = listOf(LITERAL_ACL1, PREFIXED_ACL1)
        val original = buildResponse(100, Errors.NONE, resources)
        val buffer = original.serialize(V1)
        val result = DescribeAclsResponse.parse(buffer, V1)
        assertResponseEquals(original, result)
        val result2 = buildResponse(
            throttleTimeMs = 100,
            error = Errors.NONE,
            resources = DescribeAclsResponse.aclsResources(DescribeAclsResponse.aclBindings(resources)),
        )
        assertResponseEquals(original, result2)
    }

    @Test
    fun testAclBindings() {
        val original = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "foo",
                patternType = PatternType.LITERAL,
            ),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.CREATE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )
        val result = DescribeAclsResponse.aclBindings(listOf(LITERAL_ACL1))
        assertEquals(1, result.size)
        assertEquals(original, result[0])
    }

    companion object {

        private const val V0: Short = 0

        private const val V1: Short = 1

        private val ALLOW_CREATE_ACL = buildAclDescription(
            host = "127.0.0.1",
            principal = "User:ANONYMOUS",
            operation = AclOperation.CREATE,
            permission = AclPermissionType.ALLOW,
        )

        private val DENY_READ_ACL = buildAclDescription(
            host = "127.0.0.1",
            principal = "User:ANONYMOUS",
            operation = AclOperation.READ,
            permission = AclPermissionType.DENY,
        )

        private val UNKNOWN_ACL = buildResource(
            name = "foo",
            type = ResourceType.UNKNOWN,
            patternType = PatternType.LITERAL,
            acls = listOf(DENY_READ_ACL),
        )

        private val PREFIXED_ACL1 = buildResource(
            name = "prefix",
            type = ResourceType.GROUP,
            patternType = PatternType.PREFIXED,
            acls = listOf(ALLOW_CREATE_ACL),
        )

        private val LITERAL_ACL1 = buildResource(
            name = "foo",
            type = ResourceType.TOPIC,
            patternType = PatternType.LITERAL,
            acls = listOf(ALLOW_CREATE_ACL),
        )

        private val LITERAL_ACL2 = buildResource(
            name = "group",
            type = ResourceType.GROUP,
            patternType = PatternType.LITERAL,
            acls = listOf(DENY_READ_ACL),
        )

        private fun assertResponseEquals(original: DescribeAclsResponse, actual: DescribeAclsResponse) {
            val originalBindings = original.acls.toSet()
            val actualBindings = actual.acls.toSet()
            assertEquals(originalBindings, actualBindings)
        }

        private fun buildResponse(
            throttleTimeMs: Int,
            error: Errors,
            resources: List<DescribeAclsResource>,
        ): DescribeAclsResponse = DescribeAclsResponse(
            DescribeAclsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code)
                .setErrorMessage(error.message)
                .setResources(resources)
        )

        private fun buildResource(
            name: String,
            type: ResourceType,
            patternType: PatternType,
            acls: List<AclDescription>,
        ): DescribeAclsResource = DescribeAclsResource()
            .setResourceName(name)
            .setResourceType(type.code)
            .setPatternType(patternType.code)
            .setAcls(acls)

        private fun buildAclDescription(
            host: String,
            principal: String,
            operation: AclOperation,
            permission: AclPermissionType,
        ): AclDescription = AclDescription()
            .setHost(host)
            .setPrincipal(principal)
            .setOperation(operation.code)
            .setPermissionType(permission.code)
    }
}
