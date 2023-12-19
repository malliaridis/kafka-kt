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

package org.apache.kafka.server.common

import org.apache.kafka.common.record.RecordVersion
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class MetadataVersionTest {

    @Test
    fun testKRaftFeatureLevelsBefore3_0_IV1() {
        for (i in 0..<MetadataVersion.IBP_3_0_IV1.ordinal) {
            assertEquals(-1, MetadataVersion.VERSIONS[i].featureLevel)
        }
    }

    @Test
    fun testKRaftFeatureLevelsAtAndAfter3_0_IV1() {
        for (i in MetadataVersion.IBP_3_0_IV1.ordinal..<MetadataVersion.entries.size) {
            val expectedLevel = i - MetadataVersion.IBP_3_0_IV1.ordinal + 1
            assertEquals(expectedLevel.toShort(), MetadataVersion.VERSIONS[i].featureLevel)
        }
    }

    @Test
    fun testFromVersionString() {
        assertEquals(MetadataVersion.IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0"))
        assertEquals(MetadataVersion.IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.0"))
        assertEquals(MetadataVersion.IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.1"))
        // should throw an exception as long as IBP_8_0_IV0 is not defined
        assertFailsWith<IllegalArgumentException> { MetadataVersion.fromVersionString("8.0") }
        assertEquals(MetadataVersion.IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1"))
        assertEquals(MetadataVersion.IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.0"))
        assertEquals(MetadataVersion.IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.1"))
        assertEquals(MetadataVersion.IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2"))
        assertEquals(MetadataVersion.IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.0"))
        assertEquals(MetadataVersion.IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.1"))
        assertEquals(MetadataVersion.IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0"))
        assertEquals(MetadataVersion.IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.0"))
        assertEquals(MetadataVersion.IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.1"))
        assertEquals(MetadataVersion.IBP_0_10_0_IV0, MetadataVersion.fromVersionString("0.10.0-IV0"))
        assertEquals(MetadataVersion.IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0"))
        assertEquals(MetadataVersion.IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0"))
        assertEquals(MetadataVersion.IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0-IV0"))
        assertEquals(MetadataVersion.IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.1"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV0, MetadataVersion.fromVersionString("0.10.1-IV0"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV1, MetadataVersion.fromVersionString("0.10.1-IV1"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.0"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1-IV2"))
        assertEquals(MetadataVersion.IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.1"))
        assertEquals(MetadataVersion.IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2"))
        assertEquals(MetadataVersion.IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.0"))
        assertEquals(MetadataVersion.IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2-IV0"))
        assertEquals(MetadataVersion.IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.1"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV0, MetadataVersion.fromVersionString("0.11.0-IV0"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV1, MetadataVersion.fromVersionString("0.11.0-IV1"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.0"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0-IV2"))
        assertEquals(MetadataVersion.IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.1"))
        assertEquals(MetadataVersion.IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0"))
        assertEquals(MetadataVersion.IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0"))
        assertEquals(MetadataVersion.IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0-IV0"))
        assertEquals(MetadataVersion.IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.1"))
        assertFailsWith<IllegalArgumentException> { MetadataVersion.fromVersionString("0.1.0") }
        assertFailsWith<IllegalArgumentException> { MetadataVersion.fromVersionString("0.1.0.0") }
        assertFailsWith<IllegalArgumentException> { MetadataVersion.fromVersionString("0.1.0-IV0") }
        assertFailsWith<IllegalArgumentException> { MetadataVersion.fromVersionString("0.1.0.0-IV0") }
        assertEquals(MetadataVersion.IBP_1_1_IV0, MetadataVersion.fromVersionString("1.1-IV0"))
        assertEquals(MetadataVersion.IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0"))
        assertEquals(MetadataVersion.IBP_2_0_IV0, MetadataVersion.fromVersionString("2.0-IV0"))
        assertEquals(MetadataVersion.IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0-IV1"))
        assertEquals(MetadataVersion.IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1"))
        assertEquals(MetadataVersion.IBP_2_1_IV0, MetadataVersion.fromVersionString("2.1-IV0"))
        assertEquals(MetadataVersion.IBP_2_1_IV1, MetadataVersion.fromVersionString("2.1-IV1"))
        assertEquals(MetadataVersion.IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1-IV2"))
        assertEquals(MetadataVersion.IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2"))
        assertEquals(MetadataVersion.IBP_2_2_IV0, MetadataVersion.fromVersionString("2.2-IV0"))
        assertEquals(MetadataVersion.IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2-IV1"))
        assertEquals(MetadataVersion.IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3"))
        assertEquals(MetadataVersion.IBP_2_3_IV0, MetadataVersion.fromVersionString("2.3-IV0"))
        assertEquals(MetadataVersion.IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3-IV1"))
        assertEquals(MetadataVersion.IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4"))
        assertEquals(MetadataVersion.IBP_2_4_IV0, MetadataVersion.fromVersionString("2.4-IV0"))
        assertEquals(MetadataVersion.IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4-IV1"))
        assertEquals(MetadataVersion.IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5"))
        assertEquals(MetadataVersion.IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5-IV0"))
        assertEquals(MetadataVersion.IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6"))
        assertEquals(MetadataVersion.IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6-IV0"))
        assertEquals(MetadataVersion.IBP_2_7_IV0, MetadataVersion.fromVersionString("2.7-IV0"))
        assertEquals(MetadataVersion.IBP_2_7_IV1, MetadataVersion.fromVersionString("2.7-IV1"))
        assertEquals(MetadataVersion.IBP_2_7_IV2, MetadataVersion.fromVersionString("2.7-IV2"))
        assertEquals(MetadataVersion.IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8"))
        assertEquals(MetadataVersion.IBP_2_8_IV0, MetadataVersion.fromVersionString("2.8-IV0"))
        assertEquals(MetadataVersion.IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8-IV1"))
        assertEquals(MetadataVersion.IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0"))
        assertEquals(MetadataVersion.IBP_3_0_IV0, MetadataVersion.fromVersionString("3.0-IV0"))
        assertEquals(MetadataVersion.IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0-IV1"))
        assertEquals(MetadataVersion.IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1"))
        assertEquals(MetadataVersion.IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1-IV0"))
        assertEquals(MetadataVersion.IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2"))
        assertEquals(MetadataVersion.IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2-IV0"))
        assertEquals(MetadataVersion.IBP_3_3_IV0, MetadataVersion.fromVersionString("3.3-IV0"))
        assertEquals(MetadataVersion.IBP_3_3_IV1, MetadataVersion.fromVersionString("3.3-IV1"))
        assertEquals(MetadataVersion.IBP_3_3_IV2, MetadataVersion.fromVersionString("3.3-IV2"))
        assertEquals(MetadataVersion.IBP_3_3_IV3, MetadataVersion.fromVersionString("3.3-IV3"))
        assertEquals(MetadataVersion.IBP_3_4_IV0, MetadataVersion.fromVersionString("3.4-IV0"))
        assertEquals(MetadataVersion.IBP_3_5_IV0, MetadataVersion.fromVersionString("3.5-IV0"))
        assertEquals(MetadataVersion.IBP_3_5_IV1, MetadataVersion.fromVersionString("3.5-IV1"))
        assertEquals(MetadataVersion.IBP_3_5_IV2, MetadataVersion.fromVersionString("3.5-IV2"))
        assertEquals(MetadataVersion.IBP_3_6_IV0, MetadataVersion.fromVersionString("3.6-IV0"))
        assertEquals(MetadataVersion.IBP_3_6_IV1, MetadataVersion.fromVersionString("3.6-IV1"))
        assertEquals(MetadataVersion.IBP_3_6_IV2, MetadataVersion.fromVersionString("3.6-IV2"))
    }

    @Test
    fun testMinSupportedVersionFor() {
        assertEquals(MetadataVersion.IBP_0_8_0, MetadataVersion.minSupportedFor(RecordVersion.V0))
        assertEquals(MetadataVersion.IBP_0_10_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V1))
        assertEquals(MetadataVersion.IBP_0_11_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V2))

        // Ensure that all record versions have a defined min version so that we remember to update the method
        for (recordVersion in RecordVersion.entries) {
            assertNotNull(MetadataVersion.minSupportedFor(recordVersion))
        }
    }

    @Test
    fun testShortVersion() {
        assertEquals("0.8.0", MetadataVersion.IBP_0_8_0.shortVersion)
        assertEquals("0.10.0", MetadataVersion.IBP_0_10_0_IV0.shortVersion)
        assertEquals("0.10.0", MetadataVersion.IBP_0_10_0_IV1.shortVersion)
        assertEquals("0.11.0", MetadataVersion.IBP_0_11_0_IV0.shortVersion)
        assertEquals("0.11.0", MetadataVersion.IBP_0_11_0_IV1.shortVersion)
        assertEquals("0.11.0", MetadataVersion.IBP_0_11_0_IV2.shortVersion)
        assertEquals("1.0", MetadataVersion.IBP_1_0_IV0.shortVersion)
        assertEquals("1.1", MetadataVersion.IBP_1_1_IV0.shortVersion)
        assertEquals("2.0", MetadataVersion.IBP_2_0_IV0.shortVersion)
        assertEquals("2.0", MetadataVersion.IBP_2_0_IV1.shortVersion)
        assertEquals("2.1", MetadataVersion.IBP_2_1_IV0.shortVersion)
        assertEquals("2.1", MetadataVersion.IBP_2_1_IV1.shortVersion)
        assertEquals("2.1", MetadataVersion.IBP_2_1_IV2.shortVersion)
        assertEquals("2.2", MetadataVersion.IBP_2_2_IV0.shortVersion)
        assertEquals("2.2", MetadataVersion.IBP_2_2_IV1.shortVersion)
        assertEquals("2.3", MetadataVersion.IBP_2_3_IV0.shortVersion)
        assertEquals("2.3", MetadataVersion.IBP_2_3_IV1.shortVersion)
        assertEquals("2.4", MetadataVersion.IBP_2_4_IV0.shortVersion)
        assertEquals("2.5", MetadataVersion.IBP_2_5_IV0.shortVersion)
        assertEquals("2.6", MetadataVersion.IBP_2_6_IV0.shortVersion)
        assertEquals("2.7", MetadataVersion.IBP_2_7_IV2.shortVersion)
        assertEquals("2.8", MetadataVersion.IBP_2_8_IV0.shortVersion)
        assertEquals("2.8", MetadataVersion.IBP_2_8_IV1.shortVersion)
        assertEquals("3.0", MetadataVersion.IBP_3_0_IV0.shortVersion)
        assertEquals("3.0", MetadataVersion.IBP_3_0_IV1.shortVersion)
        assertEquals("3.1", MetadataVersion.IBP_3_1_IV0.shortVersion)
        assertEquals("3.2", MetadataVersion.IBP_3_2_IV0.shortVersion)
        assertEquals("3.3", MetadataVersion.IBP_3_3_IV0.shortVersion)
        assertEquals("3.3", MetadataVersion.IBP_3_3_IV1.shortVersion)
        assertEquals("3.3", MetadataVersion.IBP_3_3_IV2.shortVersion)
        assertEquals("3.3", MetadataVersion.IBP_3_3_IV3.shortVersion)
        assertEquals("3.4", MetadataVersion.IBP_3_4_IV0.shortVersion)
        assertEquals("3.5", MetadataVersion.IBP_3_5_IV0.shortVersion)
        assertEquals("3.5", MetadataVersion.IBP_3_5_IV1.shortVersion)
        assertEquals("3.5", MetadataVersion.IBP_3_5_IV2.shortVersion)
        assertEquals("3.6", MetadataVersion.IBP_3_6_IV0.shortVersion)
        assertEquals("3.6", MetadataVersion.IBP_3_6_IV1.shortVersion)
        assertEquals("3.6", MetadataVersion.IBP_3_6_IV2.shortVersion)
    }

    @Test
    fun testVersion() {
        assertEquals("0.8.0", MetadataVersion.IBP_0_8_0.version)
        assertEquals("0.8.2", MetadataVersion.IBP_0_8_2.version)
        assertEquals("0.10.0-IV0", MetadataVersion.IBP_0_10_0_IV0.version)
        assertEquals("0.10.0-IV1", MetadataVersion.IBP_0_10_0_IV1.version)
        assertEquals("0.11.0-IV0", MetadataVersion.IBP_0_11_0_IV0.version)
        assertEquals("0.11.0-IV1", MetadataVersion.IBP_0_11_0_IV1.version)
        assertEquals("0.11.0-IV2", MetadataVersion.IBP_0_11_0_IV2.version)
        assertEquals("1.0-IV0", MetadataVersion.IBP_1_0_IV0.version)
        assertEquals("1.1-IV0", MetadataVersion.IBP_1_1_IV0.version)
        assertEquals("2.0-IV0", MetadataVersion.IBP_2_0_IV0.version)
        assertEquals("2.0-IV1", MetadataVersion.IBP_2_0_IV1.version)
        assertEquals("2.1-IV0", MetadataVersion.IBP_2_1_IV0.version)
        assertEquals("2.1-IV1", MetadataVersion.IBP_2_1_IV1.version)
        assertEquals("2.1-IV2", MetadataVersion.IBP_2_1_IV2.version)
        assertEquals("2.2-IV0", MetadataVersion.IBP_2_2_IV0.version)
        assertEquals("2.2-IV1", MetadataVersion.IBP_2_2_IV1.version)
        assertEquals("2.3-IV0", MetadataVersion.IBP_2_3_IV0.version)
        assertEquals("2.3-IV1", MetadataVersion.IBP_2_3_IV1.version)
        assertEquals("2.4-IV0", MetadataVersion.IBP_2_4_IV0.version)
        assertEquals("2.5-IV0", MetadataVersion.IBP_2_5_IV0.version)
        assertEquals("2.6-IV0", MetadataVersion.IBP_2_6_IV0.version)
        assertEquals("2.7-IV2", MetadataVersion.IBP_2_7_IV2.version)
        assertEquals("2.8-IV0", MetadataVersion.IBP_2_8_IV0.version)
        assertEquals("2.8-IV1", MetadataVersion.IBP_2_8_IV1.version)
        assertEquals("3.0-IV0", MetadataVersion.IBP_3_0_IV0.version)
        assertEquals("3.0-IV1", MetadataVersion.IBP_3_0_IV1.version)
        assertEquals("3.1-IV0", MetadataVersion.IBP_3_1_IV0.version)
        assertEquals("3.2-IV0", MetadataVersion.IBP_3_2_IV0.version)
        assertEquals("3.3-IV0", MetadataVersion.IBP_3_3_IV0.version)
        assertEquals("3.3-IV1", MetadataVersion.IBP_3_3_IV1.version)
        assertEquals("3.3-IV2", MetadataVersion.IBP_3_3_IV2.version)
        assertEquals("3.3-IV3", MetadataVersion.IBP_3_3_IV3.version)
        assertEquals("3.4-IV0", MetadataVersion.IBP_3_4_IV0.version)
        assertEquals("3.5-IV0", MetadataVersion.IBP_3_5_IV0.version)
        assertEquals("3.5-IV1", MetadataVersion.IBP_3_5_IV1.version)
        assertEquals("3.5-IV2", MetadataVersion.IBP_3_5_IV2.version)
        assertEquals("3.6-IV0", MetadataVersion.IBP_3_6_IV0.version)
        assertEquals("3.6-IV1", MetadataVersion.IBP_3_6_IV1.version)
        assertEquals("3.6-IV2", MetadataVersion.IBP_3_6_IV2.version)
    }

    @Test
    fun testPrevious() {
        for (i in 1..<MetadataVersion.entries.size - 2) {
            val version = MetadataVersion.VERSIONS[i]
            assertNotNull(version.previous(), version.toString())
            assertEquals(MetadataVersion.VERSIONS[i - 1], version.previous())
        }
    }

    @Test
    fun testMetadataChanged() {
        assertFalse(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_2_IV0, MetadataVersion.IBP_3_2_IV0))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_2_IV0, MetadataVersion.IBP_3_1_IV0))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_2_IV0, MetadataVersion.IBP_3_0_IV1))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_2_IV0, MetadataVersion.IBP_3_0_IV0))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_2_IV0, MetadataVersion.IBP_2_8_IV1))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_3_IV1, MetadataVersion.IBP_3_3_IV0))

        // Check that argument order doesn't matter
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_3_0_IV0, MetadataVersion.IBP_3_2_IV0))
        assertTrue(MetadataVersion.checkIfMetadataChanged(MetadataVersion.IBP_2_8_IV1, MetadataVersion.IBP_3_2_IV0))
    }

    @Test
    fun testKRaftVersions() {
        for (metadataVersion in MetadataVersion.VERSIONS) {
            if (metadataVersion.isKRaftSupported) assertTrue(metadataVersion.featureLevel > 0)
            else assertEquals(-1, metadataVersion.featureLevel.toInt())
        }
        for (metadataVersion in MetadataVersion.VERSIONS) {
            if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_0_IV1))
                assertTrue(metadataVersion.isKRaftSupported, metadataVersion.toString())
            else assertFalse(metadataVersion.isKRaftSupported)
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testIsInControlledShutdownStateSupported(metadataVersion: MetadataVersion) {
        assertEquals(
            metadataVersion.isAtLeast(MetadataVersion.IBP_3_3_IV3),
            metadataVersion.isInControlledShutdownStateSupported
        )
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testIsDelegationTokenSupported(metadataVersion: MetadataVersion) {
        assertEquals(
            metadataVersion.isAtLeast(MetadataVersion.IBP_3_6_IV2),
            metadataVersion.isDelegationTokenSupported
        )
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testRegisterBrokerRecordVersion(metadataVersion: MetadataVersion) {
        val expectedVersion: Short =
            if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_4_IV0)) 2
            else if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_3_IV3)) 1
            else 0
        assertEquals(expectedVersion, metadataVersion.registerBrokerRecordVersion)
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testGroupMetadataValueVersion(metadataVersion: MetadataVersion) {
        val expectedVersion: Short =
            if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_3_IV0)) 3
            else if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_1_IV0)) 2
            else if (metadataVersion.isAtLeast(MetadataVersion.IBP_0_10_1_IV0)) 1
            else 0
        assertEquals(expectedVersion, metadataVersion.groupMetadataValueVersion)
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testOffsetCommitValueVersion(metadataVersion: MetadataVersion) {
        val expectedVersion: Short =
            if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_1_IV1)) 3
            else if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_1_IV0)) 2
            else 1
        assertEquals(expectedVersion, metadataVersion.offsetCommitValueVersion(false))
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion::class)
    fun testOffsetCommitValueVersionWithExpiredTimestamp(metadataVersion: MetadataVersion) {
        assertEquals(1.toShort(), metadataVersion.offsetCommitValueVersion(true))
    }
}
