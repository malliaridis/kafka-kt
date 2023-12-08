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

package org.apache.kafka.common.acl

import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AclBindingTest {

    @Test
    fun testMatching() {
        assertEquals(ACL1, ACL1)
        val acl1Copy = AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "mytopic",
                patternType = PatternType.LITERAL,
            ),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "",
                operation = AclOperation.ALL,
                permissionType = AclPermissionType.ALLOW,
            )
        )
        assertEquals(ACL1, acl1Copy)
        assertEquals(acl1Copy, ACL1)
        assertEquals(ACL2, ACL2)
        assertNotEquals(ACL1, ACL2)
        assertNotEquals(ACL2, ACL1)
        assertTrue(AclBindingFilter.ANY.matches(ACL1))
        assertFalse(AclBindingFilter.ANY.equals(ACL1))
        assertTrue(AclBindingFilter.ANY.matches(ACL2))
        assertFalse(AclBindingFilter.ANY.equals(ACL2))
        assertTrue(AclBindingFilter.ANY.matches(ACL3))
        assertFalse(AclBindingFilter.ANY.equals(ACL3))
        assertEquals(AclBindingFilter.ANY, AclBindingFilter.ANY)
        assertTrue(ANY_ANONYMOUS.matches(ACL1))
        assertFalse(ANY_ANONYMOUS.equals(ACL1))
        assertFalse(ANY_ANONYMOUS.matches(ACL2))
        assertFalse(ANY_ANONYMOUS.equals(ACL2))
        assertTrue(ANY_ANONYMOUS.matches(ACL3))
        assertFalse(ANY_ANONYMOUS.equals(ACL3))
        assertFalse(ANY_DENY.matches(ACL1))
        assertFalse(ANY_DENY.matches(ACL2))
        assertTrue(ANY_DENY.matches(ACL3))
        assertTrue(ANY_MYTOPIC.matches(ACL1))
        assertTrue(ANY_MYTOPIC.matches(ACL2))
        assertFalse(ANY_MYTOPIC.matches(ACL3))
        assertTrue(ANY_ANONYMOUS.matches(UNKNOWN_ACL))
        assertTrue(ANY_DENY.matches(UNKNOWN_ACL))
        assertEquals(UNKNOWN_ACL, UNKNOWN_ACL)
        assertFalse(ANY_MYTOPIC.matches(UNKNOWN_ACL))
    }

    @Test
    fun testUnknowns() {
        assertFalse(ACL1.isUnknown)
        assertFalse(ACL2.isUnknown)
        assertFalse(ACL3.isUnknown)
        assertFalse(ANY_ANONYMOUS.isUnknown)
        assertFalse(ANY_DENY.isUnknown)
        assertFalse(ANY_MYTOPIC.isUnknown)
        assertTrue(UNKNOWN_ACL.isUnknown)
    }

    @Test
    fun testMatchesAtMostOne() {
        assertNull(ACL1.toFilter().findIndefiniteField())
        assertNull(ACL2.toFilter().findIndefiniteField())
        assertNull(ACL3.toFilter().findIndefiniteField())
        assertFalse(ANY_ANONYMOUS.matchesAtMostOne())
        assertFalse(ANY_DENY.matchesAtMostOne())
        assertFalse(ANY_MYTOPIC.matchesAtMostOne())
    }

    @Test
    fun shouldNotThrowOnUnknownPatternType() {
        AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "foo",
                patternType = PatternType.UNKNOWN,
            ),
            entry = ACL1.entry,
        )
    }

    @Test
    fun shouldNotThrowOnUnknownResourceType() {
        AclBinding(
            pattern = ResourcePattern(
                resourceType = ResourceType.UNKNOWN,
                name = "foo",
                patternType = PatternType.LITERAL,
            ),
            entry = ACL1.entry,
        )
    }

    @Test
    fun shouldThrowOnMatchPatternType() {
        assertFailsWith<IllegalArgumentException> {
            AclBinding(
                pattern = ResourcePattern(
                    resourceType = ResourceType.TOPIC,
                    name = "foo",
                    patternType = PatternType.MATCH,
                ),
                entry = ACL1.entry,
            )
        }
    }

    @Test
    fun shouldThrowOnAnyPatternType() {
        assertFailsWith<IllegalArgumentException> {
            AclBinding(
                pattern = ResourcePattern(
                    resourceType = ResourceType.TOPIC,
                    name = "foo",
                    patternType = PatternType.ANY,
                ),
                entry = ACL1.entry,
            )
        }
    }

    @Test
    fun shouldThrowOnAnyResourceType() {
        assertFailsWith<IllegalArgumentException> {
            AclBinding(
                pattern = ResourcePattern(
                    resourceType = ResourceType.ANY,
                    name = "foo",
                    patternType = PatternType.LITERAL,
                ),
                entry = ACL1.entry,
            )
        }
    }

    companion object {

        private val ACL1 = AclBinding(
            pattern = ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "",
                operation = AclOperation.ALL,
                permissionType = AclPermissionType.ALLOW,
            ),
        )

        private val ACL2 = AclBinding(
            pattern = ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            entry = AccessControlEntry(
                principal = "User:*",
                host = "",
                operation = AclOperation.READ,
                permissionType = AclPermissionType.ALLOW,
            ),
        )

        private val ACL3 = AclBinding(
            pattern = ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.READ,
                permissionType = AclPermissionType.DENY,
            ),
        )

        private val UNKNOWN_ACL = AclBinding(
            pattern = ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
            entry = AccessControlEntry(
                principal = "User:ANONYMOUS",
                host = "127.0.0.1",
                operation = AclOperation.UNKNOWN,
                permissionType = AclPermissionType.DENY,
            )
        )

        private val ANY_ANONYMOUS = AclBindingFilter(
            ResourcePatternFilter.ANY,
            AccessControlEntryFilter(
                principal = "User:ANONYMOUS",
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.ANY,
            )
        )

        private val ANY_DENY = AclBindingFilter(
            ResourcePatternFilter.ANY,
            AccessControlEntryFilter(
                principal = null,
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.DENY,
            )
        )

        private val ANY_MYTOPIC = AclBindingFilter(
            ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            AccessControlEntryFilter(
                principal = null,
                host = null,
                operation = AclOperation.ANY,
                permissionType = AclPermissionType.ANY,
            )
        )
    }
}
