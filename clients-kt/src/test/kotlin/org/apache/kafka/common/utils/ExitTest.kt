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

import org.apache.kafka.common.utils.Exit.addShutdownHook
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.common.utils.Exit.halt
import org.apache.kafka.common.utils.Exit.resetExitProcedure
import org.apache.kafka.common.utils.Exit.resetHaltProcedure
import org.apache.kafka.common.utils.Exit.resetShutdownHookAdder
import org.apache.kafka.common.utils.Exit.setExitProcedure
import org.apache.kafka.common.utils.Exit.setHaltProcedure
import org.apache.kafka.common.utils.Exit.setShutdownHookAdder
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ExitTest {

    @Test
    fun shouldHaltImmediately() {
        val list = mutableListOf<Any?>()
        setHaltProcedure { statusCode, message ->
            list.add(statusCode)
            list.add(message)
        }
        try {
            val statusCode = 0
            val message = "message"
            halt(statusCode)
            halt(statusCode, message)
            assertEquals(listOf<Any?>(statusCode, null, statusCode, message), list)
        } finally {
            resetHaltProcedure()
        }
    }

    @Test
    fun shouldExitImmediately() {
        val list = mutableListOf<Any?>()
        setExitProcedure { statusCode, message ->
            list.add(statusCode)
            list.add(message)
        }
        try {
            val statusCode = 0
            val message = "message"
            exit(statusCode)
            exit(statusCode, message)
            assertEquals(listOf<Any?>(statusCode, null, statusCode, message), list)
        } finally {
            resetExitProcedure()
        }
    }

    @Test
    fun shouldAddShutdownHookImmediately() {
        val list = mutableListOf<Any?>()
        setShutdownHookAdder { name, runnable ->
            list.add(name)
            list.add(runnable)
        }
        try {
            val runnable = Runnable {}
            val name = "name"
            addShutdownHook(name, runnable)
            assertEquals(listOf<Any?>(name, runnable), list)
        } finally {
            resetShutdownHookAdder()
        }
    }

    @Test
    fun shouldNotInvokeShutdownHookImmediately() {
        val list = mutableListOf<Any>()
        val runnable = Runnable { list.add(this) }
        addShutdownHook("message", runnable)
        assertEquals(0, list.size)
    }
}
