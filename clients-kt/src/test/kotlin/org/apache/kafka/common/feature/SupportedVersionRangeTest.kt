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

package org.apache.kafka.common.feature

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Unit tests for the SupportedVersionRange class.
 * Along the way, this suite also includes extensive tests for the base class BaseVersionRange.
 */
class SupportedVersionRangeTest {
    
    @Test
    fun testFailDueToInvalidParams() {
        // min and max can't be < 1.
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange(0, 0) }
        
        // min can't be < 1.
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange(0, 1) }
        
        // max can't be < 1.
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange(1, 0) }
        
        // min can't be > max.
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange(2, 1) }
    }

    @Test
    fun testFromToMap() {
        val versionRange = SupportedVersionRange(1, 2)
        assertEquals(1, versionRange.min)
        assertEquals(2, versionRange.max)
        val versionRangeMap = versionRange.toMap()
        assertEquals(
            expected = mapOf("min_version" to versionRange.min, "max_version" to versionRange.max),
            actual = versionRangeMap,
        )
        val newVersionRange = SupportedVersionRange.fromMap(versionRangeMap)
        assertEquals(1, newVersionRange.min)
        assertEquals(2, newVersionRange.max)
        assertEquals(versionRange, newVersionRange)
    }

    @Test
    fun testFromMapFailure() {
        // min_version can't be < 1.
        val invalidWithBadMinVersion: Map<String, Short> = mapOf("min_version" to 0, "max_version" to 1)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithBadMinVersion) }

        // max_version can't be < 1.
        val invalidWithBadMaxVersion: Map<String, Short> = mapOf("min_version" to 1, "max_version" to 0)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithBadMaxVersion) }

        // min_version and max_version can't be < 1.
        val invalidWithBadMinMaxVersion: Map<String, Short> = mapOf("min_version" to 0, "max_version" to 0)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithBadMinMaxVersion) }

        // min_version can't be > max_version.
        val invalidWithLowerMaxVersion: Map<String, Short> = mapOf("min_version" to 2, "max_version" to 1)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithLowerMaxVersion) }

        // min_version key missing.
        val invalidWithMinKeyMissing: Map<String, Short> = mapOf("max_version" to 1)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithMinKeyMissing) }

        // max_version key missing.
        val invalidWithMaxKeyMissing: Map<String, Short> = mapOf("min_version" to 1)
        assertFailsWith<IllegalArgumentException> { SupportedVersionRange.fromMap(invalidWithMaxKeyMissing) }
    }

    @Test
    fun testToString() {
        assertEquals(
            expected = "SupportedVersionRange[min_version:1, max_version:1]",
            actual = SupportedVersionRange(1, 1).toString()
        )
        assertEquals(
            expected = "SupportedVersionRange[min_version:1, max_version:2]",
            actual = SupportedVersionRange(1, 2).toString()
        )
    }

    @Test
    fun testEquals() {
        val tested = SupportedVersionRange(1, 1)
        assertEquals(tested, tested)
        assertNotEquals(tested, SupportedVersionRange(1, 2))
        assertNotNull(tested)
    }

    @Test
    fun testMinMax() {
        val versionRange = SupportedVersionRange(1, 2)
        assertEquals(1, versionRange.min)
        assertEquals(2, versionRange.max)
    }

    @Test
    fun testIsIncompatibleWith() {
        assertFalse(SupportedVersionRange(1, 1).isIncompatibleWith(1))
        assertFalse(SupportedVersionRange(1, 4).isIncompatibleWith(2))
        assertFalse(SupportedVersionRange(1, 4).isIncompatibleWith(1))
        assertFalse(SupportedVersionRange(1, 4).isIncompatibleWith(4))
        assertTrue(SupportedVersionRange(2, 3).isIncompatibleWith(1))
        assertTrue(SupportedVersionRange(2, 3).isIncompatibleWith(4))
    }
}
