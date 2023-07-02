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

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import java.util.concurrent.atomic.AtomicInteger

class MockPartitioner : Partitioner {

    init {
        INIT_COUNT.incrementAndGet()
    }

    override fun configure(configs: Map<String, *>) = Unit

    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int = 0

    override fun close() {
        CLOSE_COUNT.incrementAndGet()
    }

    companion object {

        val INIT_COUNT = AtomicInteger(0)

        val CLOSE_COUNT = AtomicInteger(0)

        fun resetCounters() {
            INIT_COUNT.set(0)
            CLOSE_COUNT.set(0)
        }
    }
}
