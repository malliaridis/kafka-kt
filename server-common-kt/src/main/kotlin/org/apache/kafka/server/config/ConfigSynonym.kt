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

package org.apache.kafka.server.config

import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory

/**
 * Represents a synonym for a configuration plus a conversion function. The conversion
 * function is necessary for cases where the synonym is denominated in different units
 * (hours versus milliseconds, etc.)
 */
class ConfigSynonym(
    val name: String,
    val converter: (String) -> String = { it },
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String = name

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("converter")
    )
    fun converter(): (String) -> String = converter

    companion object {

        private val log = LoggerFactory.getLogger(ConfigSynonym::class.java)

        val HOURS_TO_MILLISECONDS = { input: String ->
            val hours = valueToInt(input, 0, "hoursToMilliseconds")
            TimeUnit.MILLISECONDS.convert(hours.toLong(), TimeUnit.HOURS).toString()
        }

        val MINUTES_TO_MILLISECONDS = { input: String ->
            val hours = valueToInt(input, 0, "minutesToMilliseconds")
            TimeUnit.MILLISECONDS.convert(hours.toLong(), TimeUnit.MINUTES).toString()
        }

        private fun valueToInt(input: String?, defaultValue: Int, what: String): Int {
            if (input == null) return defaultValue
            val trimmedInput = input.trim { it <= ' ' }

            return when {
                trimmedInput.isEmpty() -> defaultValue
                else -> try {
                    trimmedInput.toInt()
                } catch (e: Exception) {
                    log.error("{} failed: unable to parse '{}' as an integer.", what, trimmedInput, e)
                    defaultValue
                }
            }
        }
    }
}
