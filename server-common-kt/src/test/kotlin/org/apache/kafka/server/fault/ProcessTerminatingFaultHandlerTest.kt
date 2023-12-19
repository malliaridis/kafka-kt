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

package org.apache.kafka.server.fault

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.common.utils.Exit.Procedure
import org.apache.kafka.common.utils.Exit.resetExitProcedure
import org.apache.kafka.common.utils.Exit.resetHaltProcedure
import org.apache.kafka.common.utils.Exit.setExitProcedure
import org.apache.kafka.common.utils.Exit.setHaltProcedure
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProcessTerminatingFaultHandlerTest {

    @Test
    fun testExitIsCalled() {
        val exitCalled = AtomicBoolean(false)
        setExitProcedure(terminatingProcedure(exitCalled))
        val actionCalled = AtomicBoolean(false)
        val action = Runnable {
            assertFalse(exitCalled.get())
            actionCalled.set(true)
        }
        try {
            ProcessTerminatingFaultHandler.Builder()
                .setShouldHalt(false)
                .setAction(action)
                .build()
                .handleFault("", null)
        } finally {
            resetExitProcedure()
        }
        assertTrue(exitCalled.get())
        assertTrue(actionCalled.get())
    }

    @Test
    fun testHaltIsCalled() {
        val haltCalled = AtomicBoolean(false)
        setHaltProcedure(terminatingProcedure(haltCalled))
        val actionCalled = AtomicBoolean(false)
        val action = Runnable {
            assertFalse(haltCalled.get())
            actionCalled.set(true)
        }
        try {
            ProcessTerminatingFaultHandler.Builder()
                .setAction(action)
                .build()
                .handleFault("", null)
        } finally {
            resetHaltProcedure()
        }
        assertTrue(haltCalled.get())
        assertTrue(actionCalled.get())
    }

    companion object {

        private fun terminatingProcedure(called: AtomicBoolean): Procedure {
            return Procedure { statusCode, message ->
                assertEquals(1, statusCode)
                assertNull(message)
                called.set(true)
            }
        }
    }
}
