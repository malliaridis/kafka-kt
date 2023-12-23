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

package org.apache.kafka.trogdor.common

import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.common.StringExpander.expand
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class StringExpanderTest {

    @Test
    fun testNoExpansionNeeded() {
        assertEquals(setOf("foo"), expand("foo"))
        assertEquals(setOf("bar"), expand("bar"))
        assertEquals(setOf(""), expand(""))
    }

    @Test
    fun testExpansions() {
        val expected1 = setOf("foo1", "foo2", "foo3")
        assertEquals(expected1, expand("foo[1-3]"))

        val expected2 = setOf("foo bar baz 0")
        assertEquals(expected2, expand("foo bar baz [0-0]"))

        val expected3 = setOf("[[ wow50 ]]", "[[ wow51 ]]", "[[ wow52 ]]")
        assertEquals(expected3, expand("[[ wow[50-52] ]]"))

        val expected4 = setOf("foo1bar", "foo2bar", "foo3bar")
        assertEquals(expected4, expand("foo[1-3]bar"))

        // should expand latest range first
        val expected5 = setOf(
            "start[1-3]middle1epilogue",
            "start[1-3]middle2epilogue",
            "start[1-3]middle3epilogue",
        )
        assertEquals(expected5, expand("start[1-3]middle[1-3]epilogue"))
    }
}
