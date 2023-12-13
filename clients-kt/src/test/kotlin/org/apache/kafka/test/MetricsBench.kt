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

package org.apache.kafka.test

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Percentile
import org.apache.kafka.common.metrics.stats.Percentiles
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing
import org.apache.kafka.common.metrics.stats.WindowedCount

object MetricsBench {

    @JvmStatic
    fun main(args: Array<String>) {
        val iters = args[0].toLong()
        Metrics().use { metrics ->
            val parent = metrics.sensor("parent")
            val child = metrics.sensor(
                name = "child",
                parents = listOf(parent),
            )
            for (sensor in listOf(parent, child)) {
                sensor.add(metrics.metricName(sensor.name + ".avg", "grp1"), Avg())
                sensor.add(metrics.metricName(sensor.name + ".count", "grp1"), WindowedCount())
                sensor.add(metrics.metricName(sensor.name + ".max", "grp1"), Max())
                sensor.add(
                    Percentiles(
                        sizeInBytes = 1024,
                        min = 0.0,
                        max = iters.toDouble(),
                        bucketing = BucketSizing.CONSTANT,
                        percentiles = listOf(
                            Percentile(
                                name = metrics.metricName(
                                    name = "${sensor.name}.median",
                                    group = "grp1",
                                ),
                                percentile = 50.0,
                            ),
                            Percentile(
                                name = metrics.metricName(
                                    name = "${sensor.name}.p_99",
                                    group = "grp1",
                                ),
                                percentile = 99.0,
                            )
                        )
                    )
                )
            }
            val start = System.nanoTime()
            for (i in 0..<iters) parent.record(i.toDouble())
            val elapsed = (System.nanoTime() - start) / iters.toDouble()
            println(String.format("%.2f ns per metric recording.", elapsed))
        }
    }
}
