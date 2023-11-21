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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Test
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

abstract class TimeTest {
    
    protected abstract fun createTime(): Time
    
    @Test
    @Throws(InterruptedException::class)
    fun testWaitObjectTimeout() {
        val obj = Object()
        val time = createTime()
        val timeoutMs: Long = 100
        val deadlineMs = time.milliseconds() + timeoutMs
        val caughtException = AtomicReference<Exception>()
        val t = Thread {
            try {
                time.waitObject(obj, { false }, deadlineMs)
            } catch (e: Exception) {
                caughtException.set(e)
            }
        }
        t.start()
        time.sleep(timeoutMs)
        t.join()
        assertIs<TimeoutException>(caughtException.get())
    }

    @Test
    @Throws(InterruptedException::class)
    fun testWaitObjectConditionSatisfied() {
        val obj = Object()
        val time = createTime()
        val timeoutMs: Long = 1000000000
        val deadlineMs = time.milliseconds() + timeoutMs
        val condition = AtomicBoolean(false)
        val caughtException = AtomicReference<Exception>()
        val t = Thread {
            try {
                time.waitObject(obj, { condition.get() }, deadlineMs)
            } catch (e: Exception) {
                caughtException.set(e)
            }
        }
        t.start()
        synchronized(obj) {
            condition.set(true)
            obj.notify()
        }
        t.join()
        assertTrue(time.milliseconds() < deadlineMs)
        assertNull(caughtException.get())
    }
}
