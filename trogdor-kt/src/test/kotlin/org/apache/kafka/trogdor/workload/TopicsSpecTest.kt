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

package org.apache.kafka.trogdor.workload

import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.common.JsonUtil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class TopicsSpecTest {

    @Test
    fun testMaterialize() {
        val parts = FOO.materialize()
        assertTrue(parts.containsKey("topicA0"))
        assertTrue(parts.containsKey("topicA1"))
        assertTrue(parts.containsKey("topicA2"))
        assertTrue(parts.containsKey("topicB"))

        assertEquals(4, parts.keys.size)
        assertEquals(PARTSA, parts["topicA0"])
        assertEquals(PARTSA, parts["topicA1"])
        assertEquals(PARTSA, parts["topicA2"])
        assertEquals(PARTSB, parts["topicB"])
    }

    @Test
    fun testPartitionNumbers() {
        val partsANumbers = PARTSA.partitionNumbers()
        assertEquals(0, partsANumbers[0])
        assertEquals(1, partsANumbers[1])
        assertEquals(2, partsANumbers[2])
        assertEquals(3, partsANumbers.size)

        val partsBNumbers = PARTSB.partitionNumbers()
        assertEquals(0, partsBNumbers[0])
        assertEquals(1, partsBNumbers[1])
        assertEquals(2, partsBNumbers.size)
    }

    @Test
    @Throws(Exception::class)
    fun testPartitionsSpec() {
        val text = "{\"numPartitions\": 5, \"configs\": {\"foo\": \"bar\"}}"
        val spec = JsonUtil.JSON_SERDE.readValue(text, PartitionsSpec::class.java)

        assertEquals(5, spec.numPartitions())
        assertEquals("bar", spec.configs()["foo"])
        assertEquals(1, spec.configs().size)
    }

    companion object {

        private val PARTSA: PartitionsSpec = PartitionsSpec(
            numPartitions = 3,
            replicationFactor = 3,
            partitionAssignments = null,
            configs = null
        )

        private val PARTSB: PartitionsSpec = PartitionsSpec(
            numPartitions = 0,
            replicationFactor = 0,
            partitionAssignments = mapOf<Int?, List<Int?>?>(
                0 to listOf(0, 1, 2),
                1 to listOf(2, 3, 4),
            ),
            configs = null,
        )

        private val FOO: TopicsSpec = TopicsSpec().apply {
            set("topicA[0-2]", PARTSA)
            set("topicB", PARTSB)
        }
    }
}
