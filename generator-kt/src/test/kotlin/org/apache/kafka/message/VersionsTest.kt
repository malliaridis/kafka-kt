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

package org.apache.kafka.message

import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class VersionsTest {

    @JvmField
    @Rule
    val timeout = Timeout(120, TimeUnit.SECONDS)

    @Test
    fun testVersionsParse() {
        assertEquals(
            expected = Versions.NONE,
            actual = Versions.parse(input = null, defaultVersions = Versions.NONE),
        )
        assertEquals(
            expected = Versions.ALL,
            actual = Versions.parse(input = " ", defaultVersions = Versions.ALL),
        )
        assertEquals(
            expected = Versions.ALL,
            actual = Versions.parse(input = "", defaultVersions = Versions.ALL),
        )
        assertEquals(
            expected = newVersions(4, 5),
            actual = Versions.parse(input = " 4-5 ", defaultVersions = null),
        )
    }

    @Test
    fun testRoundTrips() {
        testRoundTrip(versions = Versions.ALL, string = "0+")
        testRoundTrip(versions = newVersions(lower = 1, higher = 3), string = "1-3")
        testRoundTrip(versions = newVersions(lower = 2, higher = 2), string = "2")
        testRoundTrip(
            versions = newVersions(lower = 3, higher = Short.MAX_VALUE.toInt()),
            string = "3+",
        )
        testRoundTrip(Versions.NONE, "none")
    }

    private fun testRoundTrip(versions: Versions, string: String) {
        assertEquals(
            expected = string,
            actual = versions.toString(),
        )
        assertEquals(
            expected = versions,
            actual = Versions.parse(input = versions.toString(), defaultVersions = null),
        )
    }

    @Test
    fun testIntersections() {
        assertEquals(
            expected = newVersions(2, 3),
            actual = newVersions(1, 3).intersect(newVersions(2, 4)),
        )
        assertEquals(
            expected = newVersions(lower = 3, higher = 3),
            actual = newVersions(lower = 0, higher = Short.MAX_VALUE.toInt())
                .intersect(newVersions(lower = 3, higher = 3)),
        )
        assertEquals(
            expected = Versions.NONE,
            actual = newVersions(lower = 9, higher = Short.MAX_VALUE.toInt())
                .intersect(newVersions(lower = 2, higher = 8)),
        )
        assertEquals(
            expected = Versions.NONE,
            actual = Versions.NONE.intersect(Versions.NONE),
        )
    }

    @Test
    fun testContains() {
        assertTrue(newVersions(lower = 2, higher = 3).contains(3.toShort()))
        assertTrue(newVersions(lower = 2, higher = 3).contains(2.toShort()))
        assertFalse(newVersions(lower = 0, higher = 1).contains(2.toShort()))
        assertTrue(
            newVersions(lower = 0, higher = Short.MAX_VALUE.toInt()).contains(100.toShort())
        )
        assertFalse(
            newVersions(lower = 2, higher = Short.MAX_VALUE.toInt()).contains(0.toShort())
        )
        assertTrue(newVersions(lower = 2, higher = 3).contains(newVersions(lower = 2, higher = 3)))
        assertTrue(newVersions(lower = 2, higher = 3).contains(newVersions(lower = 2, higher = 2)))
        assertFalse(newVersions(lower = 2, higher = 3).contains(newVersions(lower = 2, higher = 4)))
        assertTrue(newVersions(lower = 2, higher = 3).contains(Versions.NONE))
        assertTrue(Versions.ALL.contains(newVersions(lower = 1, higher = 2)))
    }

    @Test
    fun testSubtract() {
        assertEquals(Versions.NONE, Versions.NONE.subtract(Versions.NONE))
        assertEquals(
            expected = newVersions(lower = 0, higher = 0),
            actual = newVersions(lower = 0, higher = 0).subtract(Versions.NONE),
        )
        assertEquals(
            expected = newVersions(lower = 1, higher = 1),
            actual = newVersions(lower = 1, higher = 2)
                .subtract(newVersions(lower = 2, higher = 2)),
        )
        assertEquals(
            expected = newVersions(lower = 2, higher = 2),
            actual = newVersions(lower = 1, higher = 2)
                .subtract(newVersions(lower = 1, higher = 1)),
        )
        assertNull(
            newVersions(lower = 0, higher = Short.MAX_VALUE.toInt())
                .subtract(newVersions(lower = 1, higher = 100))
        )
        assertEquals(
            expected = newVersions(lower = 10, higher = 10),
            actual = newVersions(lower = 1, higher = 10)
                .subtract(newVersions(lower = 1, higher = 9)),
        )
        assertEquals(
            expected = newVersions(lower = 1, higher = 1),
            actual = newVersions(lower = 1, higher = 10)
                .subtract(newVersions(lower = 2, higher = 10)),
        )
        assertEquals(
            expected = newVersions(lower = 2, higher = 4),
            actual = newVersions(lower = 2, higher = Short.MAX_VALUE.toInt())
                .subtract(newVersions(lower = 5, higher = Short.MAX_VALUE.toInt())),
        )
        assertEquals(
            expected = newVersions(lower = 5, higher = Short.MAX_VALUE.toInt()),
            actual = newVersions(lower = 0, higher = Short.MAX_VALUE.toInt())
                .subtract(newVersions(lower = 0, higher = 4)),
        )
    }

    companion object {

        private fun newVersions(lower: Int, higher: Int): Versions {
            if (lower < Short.MIN_VALUE || lower > Short.MAX_VALUE)
                throw RuntimeException("lower bound out of range.")

            if (higher < Short.MIN_VALUE || higher > Short.MAX_VALUE)
                throw RuntimeException("higher bound out of range.")

            return Versions(lower.toShort(), higher.toShort())
        }
    }
}
