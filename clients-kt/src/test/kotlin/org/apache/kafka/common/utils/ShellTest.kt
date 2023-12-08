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

import java.io.IOException
import org.apache.kafka.common.utils.Shell.ExitCodeException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.condition.DisabledOnOs
import org.junit.jupiter.api.condition.OS
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@Timeout(180)
@DisabledOnOs(OS.WINDOWS)
class ShellTest {

    @Test
    @Throws(Exception::class)
    fun testEchoHello() {
        val output = Shell.execCommand("echo", "hello")
        assertEquals("hello\n", output)
    }

    @Test
    @Throws(Exception::class)
    fun testHeadDevZero() {
        val length = 100000
        val output = Shell.execCommand("head", "-c", length.toString(), "/dev/zero")
        assertEquals(length, output.length)
    }

    @Test
    fun testAttemptToRunNonExistentProgram() {
        val e = assertFailsWith<IOException>(
            message = "Expected to get an exception when trying to run a program that does not exist",
        ) { Shell.execCommand(NONEXISTENT_PATH) }
        assertTrue(e.message!!.contains("No such file"), "Unexpected error message '${e.message}'")
    }

    @Test
    fun testRunProgramWithErrorReturn() {
        val e = assertFailsWith<ExitCodeException> {
            Shell.execCommand("head", "-c", "0", NONEXISTENT_PATH)
        }
        val message = e.message
        assertTrue(
            actual = message!!.contains("No such file") || message.contains("illegal byte count"),
            message = "Unexpected error message '$message'",
        )
    }

    companion object {
        private const val NONEXISTENT_PATH = "/dev/a/path/that/does/not/exist/in/the/filesystem"
    }
}
