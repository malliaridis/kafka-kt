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
import org.apache.kafka.common.resource.ResourceType
import org.junit.jupiter.api.Disabled
import kotlin.test.Ignore
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class ResourcePatternTest {

    @Test
    fun shouldThrowIfResourceTypeIsAny() {
        assertFailsWith<IllegalArgumentException> {
            ResourcePattern(
                resourceType = ResourceType.ANY,
                name = "name",
                patternType = PatternType.LITERAL,
            )
        }
    }

    @Test
    fun shouldThrowIfPatternTypeIsMatch() {
        assertFailsWith<IllegalArgumentException> {
            ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "name",
                patternType = PatternType.MATCH,
            )
        }
    }

    @Test
    fun shouldThrowIfPatternTypeIsAny() {
        assertFailsWith<IllegalArgumentException> {
            ResourcePattern(
                resourceType = ResourceType.TOPIC,
                name = "name",
                patternType = PatternType.ANY,
            )
        }
    }

    @Test
    @Disabled("Kotlin Migration: name cannot be null")
    fun shouldThrowIfResourceNameIsNull() {
//        assertFailsWith<NullPointerException> {
//            ResourcePattern(
//                resourceType = ResourceType.TOPIC,
//                name = null,
//                patternType = PatternType.ANY,
//            )
//        }
    }
}
