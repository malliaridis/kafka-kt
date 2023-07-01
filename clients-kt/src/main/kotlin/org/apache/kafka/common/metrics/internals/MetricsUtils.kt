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

package org.apache.kafka.common.metrics.internals

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.metrics.Metrics

object MetricsUtils {

    /**
     * Converts the provided time from milliseconds to the requested time unit.
     */
    fun convert(timeMs: Long, unit: TimeUnit): Double = when (unit) {
        TimeUnit.NANOSECONDS -> timeMs * 1000.0 * 1000.0
        TimeUnit.MICROSECONDS -> timeMs * 1000.0
        TimeUnit.MILLISECONDS -> timeMs.toDouble()
        TimeUnit.SECONDS -> timeMs / 1000.0
        TimeUnit.MINUTES -> timeMs / (60.0 * 1000.0)
        TimeUnit.HOURS -> timeMs / (60.0 * 60.0 * 1000.0)
        TimeUnit.DAYS -> timeMs / (24.0 * 60.0 * 60.0 * 1000.0)
        else -> error("Unknown unit: $unit")
    }

    /**
     * Create a set of tags using the supplied key and value pairs. The order of the tags will be
     * kept.
     *
     * @param keyValue the key and value pairs for the tags; must be an even number
     * @return the map of tags that can be supplied to the [Metrics] methods; never null
     */
    fun getTags(vararg keyValue: String): Map<String, String> {
        require(keyValue.size % 2 == 0) { "keyValue needs to be specified in pairs" }

        val tags: MutableMap<String, String> = LinkedHashMap(keyValue.size / 2)
        var i = 0

        while (i < keyValue.size) {
            tags[keyValue[i]] = keyValue[i + 1]
            i += 2
        }
        return tags
    }
}
