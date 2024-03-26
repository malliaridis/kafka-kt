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
import java.io.IOException
import java.net.NetworkInterface
import java.util.function.Consumer
import java.util.stream.Stream
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.fault.DegradedNetworkFaultSpec.NodeDegradeSpec
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory
import kotlin.math.sqrt

/**
 * Uses the linux utility `tc` (traffic controller) to degrade performance on a specified network device.
 */
class DegradedNetworkFaultWorker(
    private val id: String,
    private val nodeSpecs: Map<String, NodeDegradeSpec>,
) : TaskWorker {

    private var status: WorkerStatusTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        log.info("Activating DegradedNetworkFaultWorker {}.", id)
        this.status = status
        this.status!!.update(TextNode("enabling traffic control $id"))
        val curNode = platform!!.curNode()
        val nodeSpec = nodeSpecs[curNode.name()]
        if (nodeSpec != null) {
            for (device in devicesForSpec(nodeSpec)) {
                if (nodeSpec.latencyMs() < 0 || nodeSpec.rateLimitKbit() < 0) throw RuntimeException(
                    "Expected non-negative values for latencyMs and rateLimitKbit, but got $nodeSpec"
                )
                else enableTrafficControl(
                    platform = platform,
                    networkDevice = device,
                    delayMs = nodeSpec.latencyMs(),
                    rateLimitKbps = nodeSpec.rateLimitKbit(),
                )
            }
        }
        this.status!!.update(TextNode("enabled traffic control $id"))
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        log.info("Deactivating DegradedNetworkFaultWorker {}.", id)
        status!!.update(TextNode("disabling traffic control $id"))
        val curNode = platform!!.curNode()
        val nodeSpec = nodeSpecs[curNode.name()]
        if (nodeSpec != null) {
            for (device in devicesForSpec(nodeSpec)) {
                disableTrafficControl(platform, device)
            }
        }
        status!!.update(TextNode("disabled traffic control $id"))
    }

    @Throws(Exception::class)
    private fun devicesForSpec(nodeSpec: NodeDegradeSpec): Set<String> {
        val devices = mutableSetOf<String>()
        if (nodeSpec.networkDevice().isEmpty()) {
            for (networkInterface in NetworkInterface.getNetworkInterfaces()) {
                if (!networkInterface.isLoopback) devices.add(networkInterface.name)
            }
        } else devices.add(nodeSpec.networkDevice())
        return devices
    }

    /**
     * Constructs the appropriate "tc" commands to apply latency and rate limiting, if they are non zero.
     */
    @Throws(IOException::class)
    private fun enableTrafficControl(
        platform: Platform,
        networkDevice: String,
        delayMs: Int,
        rateLimitKbps: Int,
    ) {
        if (delayMs > 0) {
            val deviationMs = sqrt(delayMs.toDouble()).coerceAtMost(1.0).toInt()
            val delay = mutableListOf<String>()
            rootHandler(networkDevice) { e -> delay.add(e) }
            netemDelay(delayMs, deviationMs) { e -> delay.add(e) }
            platform.runCommand(delay.toTypedArray())
            if (rateLimitKbps > 0) {
                val rate = mutableListOf<String>()
                childHandler(networkDevice) { e -> rate.add(e) }
                tbfRate(rateLimitKbps) { e -> rate.add(e) }
                platform.runCommand(rate.toTypedArray())
            }
        } else if (rateLimitKbps > 0) {
            val rate = mutableListOf<String>()
            rootHandler(networkDevice) { e -> rate.add(e) }
            tbfRate(rateLimitKbps) { e -> rate.add(e) }
            platform.runCommand(rate.toTypedArray())
        } else log.warn("Not applying any rate limiting or latency")
    }

    /**
     * Construct the first part of a "tc" command to define a qdisc root handler for the given network interface
     */
    private fun rootHandler(networkDevice: String, consume: (String) -> Unit) {
        listOf("sudo", "tc", "qdisc", "add", "dev", networkDevice, "root", "handle", "1:0").forEach(consume)
    }

    /**
     * Construct the first part of a "tc" command to define a qdisc child handler for the given interface. This can
     * only be used if a root handler has been appropriately defined first (as in [.rootHandler]).
     */
    private fun childHandler(networkDevice: String, consume: (String) -> Unit) {
        listOf("sudo", "tc", "qdisc", "add", "dev", networkDevice, "parent", "1:1", "handle", "10:").forEach(consume)
    }

    /**
     * Construct the second part of a "tc" command that defines a netem (Network Emulator) filter that will apply some
     * amount of latency with a small amount of deviation. The distribution of the latency deviation follows a so-called
     * Pareto-normal distribution. This is the formal name for the 80/20 rule, which might better represent real-world
     * patterns.
     */
    private fun netemDelay(delayMs: Int, deviationMs: Int, consume: (String) -> Unit) {
        listOf("netem", "delay", "${delayMs}ms", "${deviationMs}ms", "distribution", "paretonormal").forEach(consume)
    }

    /**
     * Construct the second part of a "tc" command that defines a tbf (token buffer filter) that will rate limit the
     * packets going through a qdisc.
     */
    private fun tbfRate(rateLimitKbit: Int, consume: (String) -> Unit) {
        listOf("tbf", "rate", "${rateLimitKbit}kbit", "burst", "1mbit", "latency", "500ms").forEach(consume)
    }

    /**
     * Delete any previously defined qdisc for the given network interface.
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun disableTrafficControl(platform: Platform, networkDevice: String) {
        platform.runCommand(arrayOf("sudo", "tc", "qdisc", "del", "dev", networkDevice, "root"))
    }

    companion object {
        private val log = LoggerFactory.getLogger(DegradedNetworkFaultWorker::class.java)
    }
}
