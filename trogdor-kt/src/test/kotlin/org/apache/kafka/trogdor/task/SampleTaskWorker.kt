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

package org.apache.kafka.trogdor.task

import com.fasterxml.jackson.databind.node.TextNode
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.trogdor.common.Platform

class SampleTaskWorker internal constructor(private val spec: SampleTaskSpec) : TaskWorker {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        createThreadFactory("SampleTaskWorker", false)
    )

    private var future: Future<Unit>? = null

    private var status: WorkerStatusTracker? = null

    @Synchronized
    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        if (future != null) return
        this.status = status
        this.status!!.update(TextNode("active"))
        val exitMs = spec.nodeToExitMs()[platform!!.curNode().name()] ?: Long.MAX_VALUE
        future = platform.scheduler().schedule(
            executor = executor,
            callable = { haltFuture.complete(spec.error()) },
            delayMs = exitMs,
        )
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        future!!.cancel(false)
        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.DAYS)
        status!!.update(TextNode("halted"))
    }
}
