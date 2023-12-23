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

import java.time.Duration
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * Utilities for formatting strings.
 */
object StringFormatter {

    /**
     * Pretty-print a date string.
     *
     * @param timeMs The time since the epoch in milliseconds.
     * @param zoneOffset The time zone offset.
     * @return The date string in ISO format.
     */
    fun dateString(timeMs: Long, zoneOffset: ZoneOffset?): String = Date(timeMs)
        .toInstant()
        .atOffset(zoneOffset)
        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

    /**
     * Pretty-print a duration.
     *
     * @param periodMs The duration in milliseconds.
     * @return A human-readable duration string.
     */
    fun durationString(periodMs: Long): String {
        val bld = StringBuilder()
        var duration = Duration.ofMillis(periodMs)
        val hours = duration.toHours()
        if (hours > 0) {
            bld.append(hours).append("h")
            duration = duration.minusHours(hours)
        }
        val minutes = duration.toMinutes()
        if (minutes > 0) {
            bld.append(minutes).append("m")
            duration = duration.minusMinutes(minutes)
        }
        val seconds = duration.seconds
        if (seconds != 0L || bld.toString().isEmpty()) {
            bld.append(seconds).append("s")
        }
        return bld.toString()
    }

    /**
     * Formats strings in a grid pattern.
     *
     * All entries in the same column will have the same width.
     *
     * @param lines A list of lines. Each line contains a list of columns. Each line must contain
     * the same number of columns.
     * @return The string.
     */
    fun prettyPrintGrid(lines: List<List<String>>): String {
        var numColumns = -1
        var rowIndex = 0
        for (col: List<String> in lines) {
            if (numColumns == -1) numColumns = col.size
            else if (numColumns != col.size)
                throw RuntimeException("Expected $numColumns columns in row $rowIndex, but got ${col.size}")

            rowIndex++
        }
        val widths: MutableList<Int> = ArrayList(numColumns)
        for (x in 0..<numColumns) {
            var w = 0
            for (cols in lines)
                w = (cols[x].length + 1).coerceAtLeast(w)

            widths.add(w)
        }
        val bld = StringBuilder()
        for (y in lines.indices) {
            val cols = lines[y]
            for (x in cols.indices) {
                val value = cols[x]
                val minWidth = widths[x]
                bld.append(value)
                for (i in 0..<(minWidth - value.length)) bld.append(" ")
            }
            bld.append(String.format("%n"))
        }
        return bld.toString()
    }
}
