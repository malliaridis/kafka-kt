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
import java.net.InetAddress
import java.util.TreeSet
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class NetworkPartitionFaultWorker(
    private val id: String,
    private val partitionSets: List<Set<String>>,
) : TaskWorker {

    private var status: WorkerStatusTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        log.info("Activating NetworkPartitionFault {}.", id)
        this.status = status
        this.status!!.update(TextNode("creating network partition $id"))
        runIptablesCommands(platform!!, "-A")
        this.status!!.update(TextNode("created network partition $id"))
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        log.info("Deactivating NetworkPartitionFault {}.", id)
        status!!.update(TextNode("removing network partition $id"))
        runIptablesCommands(platform!!, "-D")
        status!!.update(TextNode("removed network partition $id"))
    }

    @Throws(Exception::class)
    private fun runIptablesCommands(platform: Platform, iptablesAction: String) {
        val curNode = platform.curNode()
        val topology = platform.topology()
        val toBlock = TreeSet<String>()
        for (partitionSet in partitionSets) {
            if (!partitionSet.contains(curNode.name())) {
                for (nodeName in partitionSet) toBlock.add(nodeName)
            }
        }
        for (nodeName in toBlock) {
            val node = topology.node(nodeName)
            val addr = InetAddress.getByName(node!!.hostname())
            platform.runCommand(
                arrayOf(
                    "sudo", "iptables", iptablesAction, "INPUT", "-p", "tcp", "-s",
                    addr.hostAddress, "-j", "DROP", "-m", "comment", "--comment", nodeName,
                )
            )
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(NetworkPartitionFaultWorker::class.java)
    }
}
