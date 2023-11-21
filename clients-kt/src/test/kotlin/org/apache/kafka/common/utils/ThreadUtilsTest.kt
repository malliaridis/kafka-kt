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

package org.apache.kafka.common.utils

import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ThreadUtilsTest {

    @Test
    fun testThreadNameWithoutNumberNoDemon() {
        assertEquals(
            expected = THREAD_NAME,
            actual = createThreadFactory(THREAD_NAME, false).newThread(EMPTY_RUNNABLE).name,
        )
    }

    @Test
    fun testThreadNameWithoutNumberDemon() {
        val daemonThread = createThreadFactory(THREAD_NAME, true).newThread(EMPTY_RUNNABLE)
        try {
            assertEquals(THREAD_NAME, daemonThread.name)
            assertTrue(daemonThread.isDaemon)
        } finally {
            try {
                daemonThread.join()
            } catch (e: InterruptedException) {
                // can be ignored
            }
        }
    }

    @Test
    fun testThreadNameWithNumberNoDemon() {
        val localThreadFactory = createThreadFactory(THREAD_NAME_WITH_NUMBER, false)
        assertEquals(THREAD_NAME + "1", localThreadFactory.newThread(EMPTY_RUNNABLE).name)
        assertEquals(THREAD_NAME + "2", localThreadFactory.newThread(EMPTY_RUNNABLE).name)
    }

    @Test
    fun testThreadNameWithNumberDemon() {
        val localThreadFactory = createThreadFactory(THREAD_NAME_WITH_NUMBER, true)
        val daemonThread1 = localThreadFactory.newThread(EMPTY_RUNNABLE)
        val daemonThread2 = localThreadFactory.newThread(EMPTY_RUNNABLE)
        try {
            assertEquals(THREAD_NAME + "1", daemonThread1.name)
            assertTrue(daemonThread1.isDaemon)
        } finally {
            try {
                daemonThread1.join()
            } catch (e: InterruptedException) {
                // can be ignored
            }
        }
        try {
            assertEquals(THREAD_NAME + "2", daemonThread2.name)
            assertTrue(daemonThread2.isDaemon)
        } finally {
            try {
                daemonThread2.join()
            } catch (e: InterruptedException) {
                // can be ignored
            }
        }
    }

    companion object {
        
        private val EMPTY_RUNNABLE = Runnable {}
        
        private const val THREAD_NAME = "ThreadName"
        
        private const val THREAD_NAME_WITH_NUMBER = "$THREAD_NAME%d"
    }
}
