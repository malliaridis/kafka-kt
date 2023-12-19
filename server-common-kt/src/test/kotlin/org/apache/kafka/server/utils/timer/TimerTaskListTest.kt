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

package org.apache.kafka.server.utils.timer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.stream.IntStream
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.util.timer.TimerTask
import org.apache.kafka.server.util.timer.TimerTaskEntry
import org.apache.kafka.server.util.timer.TimerTaskList
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TimerTaskListTest {
    
    @Test
    fun testAll() {
        val sharedCounter = AtomicInteger(0)
        val list1 = TimerTaskList(sharedCounter)
        val list2 = TimerTaskList(sharedCounter)
        val list3 = TimerTaskList(sharedCounter)
        val tasks = (1..10).map { i ->
            val task = TestTask(0L)
            list1.add(TimerTaskEntry(task, 10L))
            assertEquals(i, sharedCounter.get())
            task
        }
        assertEquals(tasks.size, sharedCounter.get())

        // reinserting the existing tasks shouldn't change the task count
        tasks.take(4).forEach { task ->
            val prevCounter = sharedCounter.get()
            // new TimerTaskEntry(task) will remove the existing entry from the list
            list2.add(TimerTaskEntry(task, 10L))
            assertEquals(prevCounter, sharedCounter.get())
        }
        assertEquals(10 - 4, size(list1))
        assertEquals(4, size(list2))
        assertEquals(tasks.size, sharedCounter.get())

        // reinserting the existing tasks shouldn't change the task count
        tasks.stream().skip(4).forEach { task ->
            val prevCounter = sharedCounter.get()
            // new TimerTaskEntry(task) will remove the existing entry from the list
            list3.add(TimerTaskEntry(task, 10L))
            assertEquals(prevCounter, sharedCounter.get())
        }
        assertEquals(0, size(list1))
        assertEquals(4, size(list2))
        assertEquals(6, size(list3))
        assertEquals(tasks.size, sharedCounter.get())

        // cancel tasks in lists
        list1.foreach { it!!.cancel() }
        assertEquals(0, size(list1))
        assertEquals(4, size(list2))
        assertEquals(6, size(list3))

        list2.foreach { it!!.cancel() }
        assertEquals(0, size(list1))
        assertEquals(0, size(list2))
        assertEquals(6, size(list3))

        list3.foreach { it!!.cancel() }
        assertEquals(0, size(list1))
        assertEquals(0, size(list2))
        assertEquals(0, size(list3))
    }

    @Test
    fun testGetDelay() {
        val time = MockTime()
        val list = TimerTaskList(AtomicInteger(0), time)
        list.setExpiration(time.hiResClockMs() + 10000L)
        time.sleep(5000L)
        assertEquals(5L, list.getDelay(TimeUnit.SECONDS))
    }

    companion object {

        private fun size(list: TimerTaskList): Int {
            val count = AtomicInteger(0)
            list.foreach { count.incrementAndGet() }
            return count.get()
        }
    }

    private inner class TestTask(delayMs: Long) : TimerTask(delayMs) {
        override fun run() = Unit
    }
}
