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

package org.apache.kafka.timeline

import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Timeout(value = 40)
class SnapshotRegistryTest {

    @Test
    fun testEmptyRegistry() {
        val registry = SnapshotRegistry(LogContext())
        assertFailsWith<RuntimeException> { registry.getSnapshot(0) }
        assertIteratorContains(registry.iterator())
    }

    @Test
    fun testCreateSnapshots() {
        val registry = SnapshotRegistry(LogContext())
        val snapshot123 = registry.getOrCreateSnapshot(123)
        assertEquals(snapshot123, registry.getSnapshot(123))
        assertFailsWith<RuntimeException> { registry.getSnapshot(456) }
        assertIteratorContains(registry.iterator(), snapshot123)
        val message = assertFailsWith<RuntimeException> { registry.getOrCreateSnapshot(1) }.message
        assertEquals(
            expected = "Can't create a new in-memory snapshot at epoch 1 because there is already " +
                "a snapshot with epoch 123. Snapshot epochs are 123",
            actual = message,
        )
        val snapshot456 = registry.getOrCreateSnapshot(456)
        assertIteratorContains(registry.iterator(), snapshot123, snapshot456)
    }

    @Test
    fun testCreateAndDeleteSnapshots() {
        val registry = SnapshotRegistry(LogContext())
        val snapshot123 = registry.getOrCreateSnapshot(123)
        val snapshot456 = registry.getOrCreateSnapshot(456)
        val snapshot789 = registry.getOrCreateSnapshot(789)
        registry.deleteSnapshot(snapshot456.epoch)
        assertIteratorContains(registry.iterator(), snapshot123, snapshot789)
    }

    @Test
    fun testDeleteSnapshotUpTo() {
        val registry = SnapshotRegistry(LogContext())
        registry.getOrCreateSnapshot(10)
        registry.getOrCreateSnapshot(12)
        val snapshot14 = registry.getOrCreateSnapshot(14)
        registry.deleteSnapshotsUpTo(14)
        assertIteratorContains(registry.iterator(), snapshot14)
    }

    @Test
    fun testCreateSnapshotOfLatest() {
        val registry = SnapshotRegistry(LogContext())
        registry.getOrCreateSnapshot(10)
        val latest = registry.getOrCreateSnapshot(12)
        val duplicate = registry.getOrCreateSnapshot(12)
        assertEquals(latest, duplicate)
    }

    companion object {
        private fun assertIteratorContains(
            iter: Iterator<Snapshot>,
            vararg snapshots: Snapshot,
        ) {
            val expected = mutableListOf<Snapshot>()
            for (snapshot in snapshots) expected.add(snapshot)

            val actual = mutableListOf<Snapshot>()
            while (iter.hasNext()) {
                val snapshot = iter.next()
                actual.add(snapshot)
            }
            assertEquals(expected, actual)
        }
    }
}
