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

package org.apache.kafka.trogdor.task

import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class TaskSpecTest {
    
    @Test
    @Throws(Exception::class)
    fun testTaskSpecSerialization() {
        assertFailsWith<InvalidTypeIdException>(
            message = "Missing type id should cause exception to be thrown",
        ) {
            JsonUtil.JSON_SERDE.readValue(
                """{"startMs":123,"durationMs":456,"exitMs":1000,"error":"foo"}""",
                SampleTaskSpec::class.java,
            )
        }
        val inputJson = """{"class":"org.apache.kafka.trogdor.task.SampleTaskSpec",""" +
                """"startMs":123,"durationMs":456,"nodeToExitMs":{"node01":1000},"error":"foo"}"""
        val spec = JsonUtil.JSON_SERDE.readValue(inputJson, SampleTaskSpec::class.java)
        assertEquals(123, spec.startMs())
        assertEquals(456, spec.durationMs())
        assertEquals(1000, spec.nodeToExitMs()["node01"])
        assertEquals("foo", spec.error())
        val outputJson = toJsonString(spec)
        assertEquals(inputJson, outputJson)
    }
}
