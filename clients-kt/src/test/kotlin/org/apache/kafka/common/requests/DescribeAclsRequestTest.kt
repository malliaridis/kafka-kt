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

import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DescribeAclsRequestTest {

    @Test
    fun shouldThrowOnV0IfPrefixed() {
        assertFailsWith<UnsupportedVersionException> { DescribeAclsRequest.Builder(PREFIXED_FILTER).build(V0) }
    }

    @Test
    fun shouldThrowIfUnknown() {
        assertFailsWith<IllegalArgumentException> { DescribeAclsRequest.Builder(UNKNOWN_FILTER).build(V0) }
    }

    @Test
    fun shouldRoundTripLiteralV0() {
        val original = DescribeAclsRequest.Builder(LITERAL_FILTER).build(V0)
        val result = DescribeAclsRequest.parse(original.serialize(), V0)
        assertRequestEquals(original, result)
    }

    @Test
    fun shouldRoundTripAnyV0AsLiteral() {
        val original = DescribeAclsRequest.Builder(ANY_FILTER).build(V0)
        val expected = DescribeAclsRequest.Builder(
            AclBindingFilter(
                patternFilter = ResourcePatternFilter(
                    resourceType = ANY_FILTER.patternFilter.resourceType,
                    name = ANY_FILTER.patternFilter.name,
                    patternType = PatternType.LITERAL,
                ),
                entryFilter = ANY_FILTER.entryFilter,
            )
        ).build(V0)
        val result = DescribeAclsRequest.parse(original.serialize(), V0)
        assertRequestEquals(expected, result)
    }

    @Test
    fun shouldRoundTripLiteralV1() {
        val original = DescribeAclsRequest.Builder(LITERAL_FILTER).build(V1)
        val result = DescribeAclsRequest.parse(original.serialize(), V1)
        assertRequestEquals(original, result)
    }

    @Test
    fun shouldRoundTripPrefixedV1() {
        val original = DescribeAclsRequest.Builder(PREFIXED_FILTER).build(V1)
        val result = DescribeAclsRequest.parse(original.serialize(), V1)
        assertRequestEquals(original, result)
    }

    @Test
    fun shouldRoundTripAnyV1() {
        val original = DescribeAclsRequest.Builder(ANY_FILTER).build(V1)
        val result = DescribeAclsRequest.parse(original.serialize(), V1)
        assertRequestEquals(original, result)
    }

    companion object {

        private const val V0: Short = 0

        private const val V1: Short = 1

        private val LITERAL_FILTER = AclBindingFilter(
            patternFilter = ResourcePatternFilter(
                resourceType = ResourceType.TOPIC,
                name = "foo",
                patternType = PatternType.LITERAL,
            ),
            entryFilter = AccessControlEntryFilter(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.READ,
                permissionType = AclPermissionType.DENY,
            ),
        )

        private val PREFIXED_FILTER = AclBindingFilter(
            patternFilter = ResourcePatternFilter(
                resourceType = ResourceType.GROUP,
                name = "prefix",
                patternType = PatternType.PREFIXED,
            ),
            entryFilter = AccessControlEntryFilter(
                principal = "User:*",
                host = "127.0.0.1",
                operation = AclOperation.CREATE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )

        private val ANY_FILTER = AclBindingFilter(
            patternFilter = ResourcePatternFilter(
                resourceType = ResourceType.GROUP,
                name = "bar",
                patternType = PatternType.ANY,
            ),
            entryFilter = AccessControlEntryFilter(
                principal = "User:*",
                host = "127.0.0.1",
                operation = AclOperation.CREATE,
                permissionType = AclPermissionType.ALLOW,
            ),
        )

        private val UNKNOWN_FILTER = AclBindingFilter(
            patternFilter = ResourcePatternFilter(
                resourceType = ResourceType.UNKNOWN,
                name = "foo",
                patternType = PatternType.LITERAL,
            ),
            entryFilter = AccessControlEntryFilter(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.READ,
                permissionType = AclPermissionType.DENY,
            )
        )

        private fun assertRequestEquals(original: DescribeAclsRequest, actual: DescribeAclsRequest) {
            val originalFilter = original.filter()
            val acttualFilter = actual.filter()
            assertEquals(originalFilter, acttualFilter)
        }
    }
}
