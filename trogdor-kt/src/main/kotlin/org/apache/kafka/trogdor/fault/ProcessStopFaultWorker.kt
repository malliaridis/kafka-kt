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
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class ProcessStopFaultWorker(
    private val id: String,
    private val javaProcessName: String,
) : TaskWorker {
    
    private var status: WorkerStatusTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        this.status = status
        log.info("Activating ProcessStopFault {}.", id)
        this.status!!.update(TextNode("stopping $javaProcessName"))
        sendSignals(platform!!, "SIGSTOP")
        this.status!!.update(TextNode("stopped $javaProcessName"))
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        log.info("Deactivating ProcessStopFault {}.", id)
        status!!.update(TextNode("resuming $javaProcessName"))
        sendSignals(platform!!, "SIGCONT")
        status!!.update(TextNode("resumed $javaProcessName"))
    }

    @Throws(Exception::class)
    private fun sendSignals(platform: Platform, signalName: String) {
        val jcmdOutput = platform.runCommand(arrayOf("jcmd"))
        val lines = jcmdOutput.split("\n".toRegex())
        val pids: MutableList<Int> = ArrayList()
        for (line in lines) {
            if (line.contains(javaProcessName)) {
                val components = line.split(" ".toRegex())
                try {
                    pids.add(components[0].toInt())
                } catch (e: NumberFormatException) {
                    log.error("Failed to parse process ID from line", e)
                }
            }
        }
        if (pids.isEmpty())
            log.error("{}: no processes containing {} found to send {} to.", id, javaProcessName, signalName)
        else {
            log.info("{}: sending {} to {} pid(s) {}", id, signalName, javaProcessName, join(pids, ", "))
            for (pid in pids)
                platform.runCommand(arrayOf("kill", "-$signalName", pid.toString()))
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ProcessStopFaultWorker::class.java)
    }
}
