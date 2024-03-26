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

import java.util.regex.Pattern

/**
 * Utilities for expanding strings that have range expressions in them.
 *
 * For example, 'foo[1-3]' would be expanded to foo1, foo2, foo3.
 * Strings that have no range expressions will not be expanded.
 */
object StringExpander {

    private val NUMERIC_RANGE_PATTERN = Pattern.compile("(.*)\\[([0-9]*)-([0-9]*)](.*)")

    fun expand(value: String): MutableSet<String> {
        val set = mutableSetOf<String>()
        val matcher = NUMERIC_RANGE_PATTERN.matcher(value)
        if (!matcher.matches()) {
            set.add(value)
            return set
        }
        val prequel = matcher.group(1)
        val rangeStart = matcher.group(2)
        val rangeEnd = matcher.group(3)
        val epilog = matcher.group(4)
        val rangeStartInt = rangeStart.toInt()
        val rangeEndInt = rangeEnd.toInt()
        if (rangeEndInt < rangeStartInt)
            throw RuntimeException("Invalid range: start $rangeStartInt is higher than end $rangeEndInt")

        for (i in rangeStartInt..rangeEndInt)
            set.add("$prequel$i$epilog")

        return set
    }
}
