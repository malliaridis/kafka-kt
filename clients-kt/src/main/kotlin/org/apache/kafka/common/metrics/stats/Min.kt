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

package org.apache.kafka.common.metrics.stats

import org.apache.kafka.common.metrics.MetricConfig
import kotlin.math.min

/**
 * A [SampledStat] that gives the min over its samples.
 */
class Min : SampledStat(Double.MAX_VALUE) {
    override fun update(sample: Sample, config: MetricConfig?, value: Double, now: Long) {
        sample.value = min(sample.value, value)
    }

    override fun combine(samples: List<Sample>, config: MetricConfig?, now: Long): Double {
        var min = Double.MAX_VALUE
        var count: Long = 0
        for (sample in samples) {
            min = min(min, sample.value)
            count += sample.eventCount
        }
        return if (count == 0L) Double.NaN else min
    }
}
