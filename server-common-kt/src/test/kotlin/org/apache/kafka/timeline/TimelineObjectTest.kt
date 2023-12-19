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
class TimelineObjectTest {

    @Test
    fun testModifyValue() {
        val registry = SnapshotRegistry(LogContext())
        val obj = TimelineObject(registry, "default")
        assertEquals("default", obj.get())
        assertEquals("default", obj[Long.MAX_VALUE])
        obj.set("1")
        obj.set("2")
        assertEquals("2", obj.get())
        assertEquals("2", obj[Long.MAX_VALUE])
    }

    @Test
    fun testToStringAndEquals() {
        val registry = SnapshotRegistry(LogContext())
        val obj = TimelineObject(registry, "")
        assertEquals("", obj.toString())
        obj.set("a")
        val object2 = TimelineObject(registry, "")
        object2.set("a")
        assertEquals("a", object2.toString())
        assertEquals(obj, object2)
    }

    @Test
    fun testSnapshot() {
        val registry = SnapshotRegistry(LogContext())
        val obj = TimelineObject(registry, "1000")
        registry.getOrCreateSnapshot(2)
        obj.set("1001")
        registry.getOrCreateSnapshot(3)
        obj.set("1002")
        obj.set("1003")
        obj.set("1002")
        registry.getOrCreateSnapshot(4)
        assertEquals("1000", obj[2])
        assertEquals("1001", obj[3])
        assertEquals("1002", obj[4])
        registry.revertToSnapshot(3)
        assertEquals("1001", obj.get())
        registry.revertToSnapshot(2)
        assertEquals("1000", obj.get())
    }

    @Test
    fun testReset() {
        val registry = SnapshotRegistry(LogContext())
        val value = TimelineObject(registry, "<default>")
        registry.getOrCreateSnapshot(2)
        value.set("first value")
        registry.getOrCreateSnapshot(3)
        value.set("second value")
        registry.reset()
        assertEquals(emptyList(), registry.epochsList())
        assertEquals("<default>", value.get())
    }
}
