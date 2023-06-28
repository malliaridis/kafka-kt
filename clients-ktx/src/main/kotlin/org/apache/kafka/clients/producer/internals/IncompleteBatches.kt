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

package org.apache.kafka.clients.producer.internals

/**
 * A thread-safe helper class to hold batches that haven't been acknowledged yet (including those
 * which have and have not been sent).
 */
internal class IncompleteBatches {

    private val incomplete = mutableSetOf<ProducerBatch>()

    fun add(batch: ProducerBatch) = synchronized(incomplete) { incomplete.add(batch) }

    fun remove(batch: ProducerBatch) = synchronized(incomplete) {
        val removed = incomplete.remove(batch)
        check(removed) { "Remove from the incomplete set failed. This should be impossible." }
    }

    fun copyAll(): Iterable<ProducerBatch> = synchronized(incomplete) {
        return incomplete.toList()
    }

    fun requestResults(): Iterable<ProduceRequestResult> = synchronized(incomplete) {
        return incomplete.map { batch -> batch.produceFuture }
    }

    val isEmpty: Boolean
        get() = synchronized(incomplete) { return incomplete.isEmpty() }
}
