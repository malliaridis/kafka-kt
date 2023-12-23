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
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.trogdor.common.Platform
import org.slf4j.LoggerFactory

class NoOpTaskWorker(private val id: String) : TaskWorker {

    private var status: WorkerStatusTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        log.info("{}: Activating NoOpTask.", id)
        this.status = status.also {
            it.update(TextNode("active"))
        }
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform) {
        log.info("{}: Deactivating NoOpTask.", id)
        status!!.update(TextNode("done"))
    }

    companion object {
        private val log = LoggerFactory.getLogger(NoOpTaskWorker::class.java)
    }
}
