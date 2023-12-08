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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PartitionInfoTest {

    @Test
    fun testToString() {
        val topic = "sample"
        val partition = 0
        val leader = Node(id = 0, host = "localhost", port = 9092)
        val r1 = Node(id = 1, host = "localhost", port = 9093)
        val r2 = Node(id = 2, host = "localhost", port = 9094)
        val replicas = listOf(leader, r1, r2)
        val inSyncReplicas = listOf(leader, r1)
        val offlineReplicas = listOf(r2)
        val partitionInfo = PartitionInfo(
            topic = topic,
            partition = partition,
            leader = leader,
            replicas = replicas,
            inSyncReplicas = inSyncReplicas,
            offlineReplicas = offlineReplicas,
        )
        val expected = "Partition(" +
                "topic = $topic, " +
                "partition = $partition, " +
                "leader = ${leader.idString()}, " +
                "replicas = [0,1,2], " +
                "isr = [0,1], " +
                "offlineReplicas = [2]" +
                ")"
        assertEquals(expected = expected, actual = partitionInfo.toString())
    }
}
