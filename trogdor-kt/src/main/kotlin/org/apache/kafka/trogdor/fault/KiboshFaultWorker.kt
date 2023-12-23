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

package org.apache.kafka.trogdor.fault

import com.fasterxml.jackson.databind.node.TextNode
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.fault.Kibosh.KiboshFaultSpec
import org.apache.kafka.trogdor.fault.Kibosh.addFault
import org.apache.kafka.trogdor.fault.Kibosh.removeFault
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class KiboshFaultWorker(
    private val id: String,
    private val spec: KiboshFaultSpec,
    private val mountPath: String,
) : TaskWorker {

    private var status: WorkerStatusTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        log.info("Activating {} {}: {}.", spec.javaClass.getSimpleName(), id, spec)
        this.status = status
        this.status!!.update(TextNode("Adding fault $id"))
        addFault(mountPath, spec)
        this.status!!.update(TextNode("Added fault $id"))
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform) {
        log.info("Deactivating {} {}: {}.", spec.javaClass.getSimpleName(), id, spec)
        status!!.update(TextNode("Removing fault $id"))
        removeFault(mountPath, spec)
        status!!.update(TextNode("Removed fault $id"))
    }

    companion object {
        private val log = LoggerFactory.getLogger(KiboshFaultWorker::class.java)
    }
}
