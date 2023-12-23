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

import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ThrottleTest {

    @Test
    @Throws(Exception::class)
    fun testThrottle() {
        val time = MockTime(autoTickMs = 0, currentTimeMs = 0, currentHighResTimeNs = 0)
        val throttle = ThrottleMock(time, 3)
        assertFalse(throttle.increment())
        assertEquals(0, time.milliseconds())
        assertFalse(throttle.increment())
        assertEquals(0, time.milliseconds())
        assertFalse(throttle.increment())
        assertEquals(0, time.milliseconds())
        assertTrue(throttle.increment())
        assertEquals(100, time.milliseconds())
        time.sleep(50)
        assertFalse(throttle.increment())
        assertEquals(150, time.milliseconds())
        assertFalse(throttle.increment())
        assertEquals(150, time.milliseconds())
        assertTrue(throttle.increment())
        assertEquals(200, time.milliseconds())
    }

    /**
     * ThrottleMock is a subclass of Throttle that uses a MockTime object.  It calls
     * MockTime#sleep instead of Object#wait.
     */
    private class ThrottleMock(
        override val time: MockTime,
        maxPerSec: Int,
    ) : Throttle(maxPerSec, 100) {

        @Deprecated("Use property instead", replaceWith = ReplaceWith("time"))
        override fun time(): Time = time

        @Synchronized
        @Throws(InterruptedException::class)
        override fun delay(amount: Long) = time.sleep(amount)
    }
}
