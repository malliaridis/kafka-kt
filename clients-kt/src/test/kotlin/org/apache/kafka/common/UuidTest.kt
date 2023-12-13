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

package org.apache.kafka.common

import java.util.Base64
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals

class UuidTest {

    @Test
    fun testSignificantBits() {
        val (mostSignificantBits, leastSignificantBits) = Uuid(34L, 98L)
        assertEquals(mostSignificantBits, 34L)
        assertEquals(leastSignificantBits, 98L)
    }

    @Test
    fun testUuidEquality() {
        val id1 = Uuid(12L, 13L)
        val id2 = Uuid(12L, 13L)
        val id3 = Uuid(24L, 38L)
        assertEquals(Uuid.ZERO_UUID, Uuid.ZERO_UUID)
        assertEquals(id1, id2)
        assertNotEquals(id1, id3)
        assertEquals(Uuid.ZERO_UUID.hashCode(), Uuid.ZERO_UUID.hashCode())
        assertEquals(id1.hashCode(), id2.hashCode())
        assertNotEquals(id1.hashCode(), id3.hashCode())
    }

    @Test
    @Disabled("Kotlin Migration - hashCode implementation is generated in kotlin data classes")
    fun testHashCode() {
        val id1 = Uuid(16L, 7L)
        val id2 = Uuid(1043L, 20075L)
        val id3 = Uuid(104312423523523L, 200732425676585L)
        assertEquals(23, id1.hashCode())
        assertEquals(19064, id2.hashCode())
        assertEquals(-2011255899, id3.hashCode())
    }

    @Test
    fun testStringConversion() {
        val id = Uuid.randomUuid()
        val idString = id.toString()
        assertEquals(Uuid.fromString(idString), id)
        val zeroIdString = Uuid.ZERO_UUID.toString()
        assertEquals(Uuid.fromString(zeroIdString), Uuid.ZERO_UUID)
    }

    @RepeatedTest(100)
    fun testRandomUuid() {
        val randomID = Uuid.randomUuid()
        assertNotEquals(randomID, Uuid.ZERO_UUID)
        assertNotEquals(randomID, Uuid.METADATA_TOPIC_ID)
        assertFalse(randomID.toString().startsWith("-"))
    }

    @Test
    fun testCompareUuids() {
        val id00 = Uuid(0L, 0L)
        val id01 = Uuid(0L, 1L)
        val id10 = Uuid(1L, 0L)
        assertEquals(0, id00.compareTo(id00))
        assertEquals(0, id01.compareTo(id01))
        assertEquals(0, id10.compareTo(id10))
        assertEquals(-1, id00.compareTo(id01))
        assertEquals(-1, id00.compareTo(id10))
        assertEquals(1, id01.compareTo(id00))
        assertEquals(1, id10.compareTo(id00))
        assertEquals(-1, id01.compareTo(id10))
        assertEquals(1, id10.compareTo(id01))
    }

    @Test
    fun testFromStringWithInvalidInput() {
        val oversizeString = Base64.getUrlEncoder().withoutPadding().encodeToString(ByteArray(32))
        assertFailsWith<IllegalArgumentException> { Uuid.fromString(oversizeString) }
        val undersizeString = Base64.getUrlEncoder().withoutPadding().encodeToString(ByteArray(4))
        assertFailsWith<IllegalArgumentException> { Uuid.fromString(undersizeString) }
    }
}
