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
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ResourcePatternFilterTest {

    @Test
    fun shouldBeUnknownIfResourceTypeUnknown() {
        assertTrue(ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL).isUnknown)
    }

    @Test
    fun shouldBeUnknownIfPatternTypeUnknown() {
        assertTrue(ResourcePatternFilter(ResourceType.GROUP, null, PatternType.UNKNOWN).isUnknown)
    }

    @Test
    fun shouldNotMatchIfDifferentResourceType() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.GROUP, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchIfDifferentName() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Different", PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldNotMatchIfDifferentNameCase() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "NAME", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchIfDifferentPatternType() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchWhereResourceTypeIsAny() {
        assertTrue(
            ResourcePatternFilter(ResourceType.ANY, "Name", PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchWhereResourceNameIsAny() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchWherePatternTypeIsAny() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchWherePatternTypeIsMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchLiteralIfExactMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeAny() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.ANY)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.MATCH)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchLiteralIfNamePrefixed() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name-something", PatternType.MATCH)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldMatchLiteralWildcardIfExactMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "*", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchLiteralWildcardAgainstOtherName() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "*", PatternType.LITERAL)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldNotMatchLiteralWildcardIfFilterHasPatternTypeOfAny() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.ANY)
                .matches(ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldMatchLiteralWildcardIfFilterHasPatternTypeOfMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.MATCH)
                .matches(ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL))
        )
    }

    @Test
    fun shouldMatchPrefixedIfExactMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name", PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name-something", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name-something", PatternType.PREFIXED)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldNotMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() {
        assertFalse(
            ResourcePatternFilter(ResourceType.TOPIC, "Name-something", PatternType.ANY)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }

    @Test
    fun shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsMatch() {
        assertTrue(
            ResourcePatternFilter(ResourceType.TOPIC, "Name-something", PatternType.MATCH)
                .matches(ResourcePattern(ResourceType.TOPIC, "Name", PatternType.PREFIXED))
        )
    }
}
