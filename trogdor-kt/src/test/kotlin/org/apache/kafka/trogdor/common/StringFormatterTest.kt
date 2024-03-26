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

import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.common.StringFormatter.dateString
import org.apache.kafka.trogdor.common.StringFormatter.durationString
import org.apache.kafka.trogdor.common.StringFormatter.prettyPrintGrid
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class StringFormatterTest() {
    @Test
    fun testDateString() {
        assertEquals("2019-01-08T20:59:29.85Z", dateString(1546981169850L, ZoneOffset.UTC))
    }

    @Test
    fun testDurationString() {
        assertEquals("1m", durationString(60000))
        assertEquals("1m1s", durationString(61000))
        assertEquals("1m1s", durationString(61200))
        assertEquals("5s", durationString(5000))
        assertEquals("2h", durationString(7200000))
        assertEquals("2h1s", durationString(7201000))
        assertEquals("2h5m3s", durationString(7503000))
    }

    @Test
    fun testPrettyPrintGrid() {
        assertEquals(
            String.format(
                "ANIMAL  NUMBER INDEX %n" +
                        "lion    1      12345 %n" +
                        "manatee 50     1     %n"
            ),
            prettyPrintGrid(
                listOf(
                    listOf("ANIMAL", "NUMBER", "INDEX"),
                    listOf("lion", "1", "12345"),
                    listOf("manatee", "50", "1"),
                )
            )
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(StringFormatterTest::class.java)
    }
}
