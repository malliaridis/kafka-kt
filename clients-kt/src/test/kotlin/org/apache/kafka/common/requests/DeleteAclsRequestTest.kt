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
import org.apache.kafka.common.message.DeleteAclsRequestData
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DeleteAclsRequestTest {

    @Test
    fun shouldThrowOnV0IfPrefixed() {
        assertFailsWith<UnsupportedVersionException> {
            DeleteAclsRequest.Builder(requestData(PREFIXED_FILTER)).build(V0)
        }
    }

    @Test
    fun shouldThrowOnUnknownElements() {
        assertFailsWith<IllegalArgumentException> {
            DeleteAclsRequest.Builder(requestData(UNKNOWN_FILTER)).build(V1)
        }
    }

    @Test
    fun shouldRoundTripLiteralV0() {
        val original = DeleteAclsRequest.Builder(requestData(LITERAL_FILTER)).build(V0)
        val buffer = original.serialize()
        val result = DeleteAclsRequest.parse(buffer, V0)
        assertRequestEquals(original, result)
    }

    @Test
    fun shouldRoundTripAnyV0AsLiteral() {
        val original = DeleteAclsRequest.Builder(requestData(ANY_FILTER)).build(V0)
        val expected = DeleteAclsRequest.Builder(
            requestData(
                AclBindingFilter(
                    patternFilter = ResourcePatternFilter(
                        resourceType = ANY_FILTER.patternFilter.resourceType,
                        name = ANY_FILTER.patternFilter.name,
                        patternType = PatternType.LITERAL,
                    ),
                    entryFilter = ANY_FILTER.entryFilter,
                )
            )
        ).build(V0)
        val result = DeleteAclsRequest.parse(original.serialize(), V0)
        assertRequestEquals(expected, result)
    }

    @Test
    fun shouldRoundTripV1() {
        val original = DeleteAclsRequest.Builder(requestData(LITERAL_FILTER, PREFIXED_FILTER, ANY_FILTER))
            .build(V1)
        val buffer = original.serialize()
        val result = DeleteAclsRequest.parse(buffer, V1)
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

        private fun assertRequestEquals(original: DeleteAclsRequest, actual: DeleteAclsRequest) {
            assertEquals(
                expected = original.filters().size,
                actual = actual.filters().size,
                message = "Number of filters wrong",
            )
            for (idx in original.filters().indices) {
                val originalFilter = original.filters()[idx]
                val actualFilter = actual.filters()[idx]
                assertEquals(originalFilter, actualFilter)
            }
        }

        private fun requestData(vararg acls: AclBindingFilter): DeleteAclsRequestData {
            return DeleteAclsRequestData().setFilters(
                acls.map(DeleteAclsRequest::deleteAclsFilter)
            )
        }
    }
}
