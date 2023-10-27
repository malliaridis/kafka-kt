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

package org.apache.kafka.common.internals

import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PartitionStatesTest {

    @Test
    fun testSet() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        val expected = LinkedHashMap<TopicPartition, String>()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 2)] = "baz 2"
        expected[TopicPartition("baz", 3)] = "baz 3"
        checkState(states, expected)
        states.set(LinkedHashMap())
        checkState(states, LinkedHashMap())
    }

    private fun createMap(): LinkedHashMap<TopicPartition, String> {
        val map = LinkedHashMap<TopicPartition, String>()
        map[TopicPartition(topic = "foo", partition = 2)] = "foo 2"
        map[TopicPartition(topic = "blah", partition = 2)] = "blah 2"
        map[TopicPartition(topic = "blah", partition = 1)] = "blah 1"
        map[TopicPartition(topic = "baz", partition = 2)] = "baz 2"
        map[TopicPartition(topic = "foo", partition = 0)] = "foo 0"
        map[TopicPartition(topic = "baz", partition = 3)] = "baz 3"
        return map
    }

    private fun checkState(states: PartitionStates<String>, expected: LinkedHashMap<TopicPartition, String>) {
        assertEquals(expected.keys, states.partitionSet())
        assertEquals(expected.size, states.size())
        assertEquals(expected, states.partitionStateMap())
    }

    @Test
    fun testMoveToEnd() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        states.moveToEnd(TopicPartition("baz", 2))
        var expected = LinkedHashMap<TopicPartition, String>()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("baz", 2)] = "baz 2"
        checkState(states, expected)
        states.moveToEnd(TopicPartition("foo", 2))
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("baz", 2)] = "baz 2"
        expected[TopicPartition("foo", 2)] = "foo 2"
        checkState(states, expected)

        // no-op
        states.moveToEnd(TopicPartition("foo", 2))
        checkState(states, expected)

        // partition doesn't exist
        states.moveToEnd(TopicPartition("baz", 5))
        checkState(states, expected)

        // topic doesn't exist
        states.moveToEnd(TopicPartition("aaa", 2))
        checkState(states, expected)
    }

    @Test
    fun testUpdateAndMoveToEnd() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        states.updateAndMoveToEnd(TopicPartition("foo", 0), "foo 0 updated")
        var expected = LinkedHashMap<TopicPartition, String>()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 2)] = "baz 2"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("foo", 0)] = "foo 0 updated"
        checkState(states, expected)
        states.updateAndMoveToEnd(TopicPartition("baz", 2), "baz 2 updated")
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("foo", 0)] = "foo 0 updated"
        expected[TopicPartition("baz", 2)] = "baz 2 updated"
        checkState(states, expected)

        // partition doesn't exist
        states.updateAndMoveToEnd(TopicPartition("baz", 5), "baz 5 new")
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("foo", 0)] = "foo 0 updated"
        expected[TopicPartition("baz", 2)] = "baz 2 updated"
        expected[TopicPartition("baz", 5)] = "baz 5 new"
        checkState(states, expected)

        // topic doesn't exist
        states.updateAndMoveToEnd(TopicPartition("aaa", 2), "aaa 2 new")
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 2)] = "foo 2"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 3)] = "baz 3"
        expected[TopicPartition("foo", 0)] = "foo 0 updated"
        expected[TopicPartition("baz", 2)] = "baz 2 updated"
        expected[TopicPartition("baz", 5)] = "baz 5 new"
        expected[TopicPartition("aaa", 2)] = "aaa 2 new"
        checkState(states, expected)
    }

    @Test
    fun testPartitionValues() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        val expected: MutableList<String> = ArrayList()
        expected.add("foo 2")
        expected.add("foo 0")
        expected.add("blah 2")
        expected.add("blah 1")
        expected.add("baz 2")
        expected.add("baz 3")
        assertEquals(expected, states.partitionStateValues())
    }

    @Test
    fun testClear() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        states.clear()
        checkState(states, LinkedHashMap())
    }

    @Test
    fun testRemove() {
        val states = PartitionStates<String>()
        val map = createMap()
        states.set(map)
        states.remove(TopicPartition("foo", 2))
        var expected = LinkedHashMap<TopicPartition, String>()
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("blah", 1)] = "blah 1"
        expected[TopicPartition("baz", 2)] = "baz 2"
        expected[TopicPartition("baz", 3)] = "baz 3"
        checkState(states, expected)
        states.remove(TopicPartition("blah", 1))
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("baz", 2)] = "baz 2"
        expected[TopicPartition("baz", 3)] = "baz 3"
        checkState(states, expected)
        states.remove(TopicPartition("baz", 3))
        expected = LinkedHashMap()
        expected[TopicPartition("foo", 0)] = "foo 0"
        expected[TopicPartition("blah", 2)] = "blah 2"
        expected[TopicPartition("baz", 2)] = "baz 2"
        checkState(states, expected)
    }
}
