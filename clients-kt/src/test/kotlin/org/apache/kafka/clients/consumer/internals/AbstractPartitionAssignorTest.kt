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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals

class AbstractPartitionAssignorTest {
    
    @Test
    fun testMemberInfoSortingWithoutGroupInstanceId() {
        val m1 = MemberInfo("a", null)
        val m2 = MemberInfo("b", null)
        val m3 = MemberInfo("c", null)
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(memberInfoList, memberInfoList.sorted())
    }

    @Test
    fun testMemberInfoSortingWithAllGroupInstanceId() {
        val m1 = MemberInfo("a", "y")
        val m2 = MemberInfo("b", "z")
        val m3 = MemberInfo("c", "x")
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(listOf(m3, m1, m2), memberInfoList.sorted())
    }

    @Test
    fun testMemberInfoSortingSomeGroupInstanceId() {
        val m1 = MemberInfo("a", null)
        val m2 = MemberInfo("b", "y")
        val m3 = MemberInfo("c", "x")
        val memberInfoList = listOf(m1, m2, m3)
        assertEquals(listOf(m3, m2, m1), memberInfoList.sorted())
    }

    @Test
    fun testMergeSortManyMemberInfo() {
        val bound = 2
        val memberInfoList = mutableListOf<MemberInfo?>()
        val staticMemberList = mutableListOf<MemberInfo>()
        val dynamicMemberList = mutableListOf<MemberInfo>()
        repeat(100) { i ->
            // Need to make sure all the ids are defined as 3-digits otherwise
            // the comparison result will break.
            val id = (i + 100).toString()
            val groupInstanceId = if (Random.nextInt(bound) < bound / 2) id else null
            val m = MemberInfo(id, groupInstanceId)
            memberInfoList.add(m)
            if (m.groupInstanceId != null) staticMemberList.add(m)
            else dynamicMemberList.add(m)
        }
        staticMemberList.addAll(dynamicMemberList)
        memberInfoList.shuffle()
        assertEquals(staticMemberList, memberInfoList.sortedWith(nullsLast(naturalOrder())))
    }
}
