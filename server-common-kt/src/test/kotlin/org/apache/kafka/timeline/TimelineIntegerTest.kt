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

@Timeout(value = 40)
class TimelineIntegerTest {

    @Test
    fun testModifyValue() {
        val registry = SnapshotRegistry(LogContext())
        val integer = TimelineInteger(registry)
        assertEquals(0, integer.get())
        assertEquals(0, integer[Long.MAX_VALUE])
        integer.set(1)
        integer.set(2)
        assertEquals(2, integer.get())
        assertEquals(2, integer[Long.MAX_VALUE])
    }

    @Test
    fun testToStringAndEquals() {
        val registry = SnapshotRegistry(LogContext())
        val integer = TimelineInteger(registry)
        assertEquals("0", integer.toString())
        integer.set(1)
        val integer2 = TimelineInteger(registry)
        integer2.set(1)
        assertEquals("1", integer2.toString())
        assertEquals(integer, integer2)
    }

    @Test
    fun testSnapshot() {
        val registry = SnapshotRegistry(LogContext())
        val integer = TimelineInteger(registry)
        registry.getOrCreateSnapshot(2)
        integer.set(1)
        registry.getOrCreateSnapshot(3)
        integer.set(2)
        integer.increment()
        integer.increment()
        integer.decrement()
        registry.getOrCreateSnapshot(4)
        assertEquals(0, integer[2])
        assertEquals(1, integer[3])
        assertEquals(3, integer[4])
        registry.revertToSnapshot(3)
        assertEquals(1, integer.get())
        registry.revertToSnapshot(2)
        assertEquals(0, integer.get())
    }

    @Test
    fun testReset() {
        val registry = SnapshotRegistry(LogContext())
        val value = TimelineInteger(registry)
        registry.getOrCreateSnapshot(2)
        value.set(1)
        registry.getOrCreateSnapshot(3)
        value.set(2)
        registry.reset()
        assertEquals(emptyList<Any>(), registry.epochsList())
        assertEquals(TimelineInteger.INIT, value.get())
    }
}
