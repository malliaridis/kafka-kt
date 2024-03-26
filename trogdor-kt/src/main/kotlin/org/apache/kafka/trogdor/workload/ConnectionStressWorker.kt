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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import java.io.IOException
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.createChannelBuilder
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.ManualMetadataUpdater
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.NetworkClientUtils.awaitReady
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.propsToMap
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.WorkerUtils.abort
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.common.WorkerUtils.perSecToPerPeriod
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.apache.kafka.trogdor.workload.ConnectionStressSpec.ConnectionStressAction
import org.slf4j.LoggerFactory

class ConnectionStressWorker(
    private val id: String,
    private val spec: ConnectionStressSpec,
) : TaskWorker {

    private val running = AtomicBoolean(false)

    private var doneFuture: KafkaFutureImpl<String>? = null

    private var status: WorkerStatusTracker? = null

    private var totalConnections: Long = 0

    private var totalFailedConnections: Long = 0

    private var startTimeMs: Long = 0

    private var statusUpdaterFuture: Future<*>? = null

    private var workerExecutor: ExecutorService? = null

    private var statusUpdaterExecutor: ScheduledExecutorService? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) {
            "ConnectionStressWorker is already running."
        }
        log.info("{}: Activating ConnectionStressWorker with {}", id, spec)
        this.doneFuture = haltFuture
        this.status = status
        synchronized(this@ConnectionStressWorker) {
            totalConnections = 0
            totalFailedConnections = 0
            startTimeMs = TIME.milliseconds()
        }
        statusUpdaterExecutor = Executors.newScheduledThreadPool(
            1,
            createThreadFactory("StatusUpdaterWorkerThread%d", false),
        )
        statusUpdaterFuture = statusUpdaterExecutor!!.scheduleAtFixedRate(
            StatusUpdater(),
            0,
            REPORT_INTERVAL_MS.toLong(),
            TimeUnit.MILLISECONDS,
        )
        workerExecutor = Executors.newFixedThreadPool(
            spec.numThreads(),
            createThreadFactory("ConnectionStressWorkerThread%d", false)
        )
        for (i in 0..<spec.numThreads())
            workerExecutor!!.submit(ConnectLoop())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        check(running.compareAndSet(true, false)) {
            "ConnectionStressWorker is not running."
        }
        log.info("{}: Deactivating ConnectionStressWorker.", id)

        // Shut down the periodic status updater and perform a final update on the
        // statistics.  We want to do this first, before deactivating any threads.
        // Otherwise, if some threads take a while to terminate, this could lead
        // to a misleading rate getting reported.
        statusUpdaterFuture!!.cancel(false)
        statusUpdaterExecutor!!.shutdown()
        statusUpdaterExecutor!!.awaitTermination(1, TimeUnit.DAYS)
        statusUpdaterExecutor = null
        StatusUpdater().run()
        doneFuture!!.complete("")
        workerExecutor!!.shutdownNow()
        workerExecutor!!.awaitTermination(1, TimeUnit.DAYS)
        workerExecutor = null
        status = null
    }

    private class ConnectStressThrottle(maxPerPeriod: Int) : Throttle(maxPerPeriod, THROTTLE_PERIOD_MS)

    internal interface Stressor : AutoCloseable {

        fun tryConnect(): Boolean

        companion object {
            fun fromSpec(spec: ConnectionStressSpec): Stressor {
                return when (spec.action()) {
                    ConnectionStressAction.CONNECT -> ConnectStressor(spec)
                    ConnectionStressAction.FETCH_METADATA -> FetchMetadataStressor(spec)
                }
                throw RuntimeException("invalid spec.action " + spec.action())
            }
        }
    }

    internal class ConnectStressor(spec: ConnectionStressSpec) : Stressor {

        private val conf: AdminClientConfig

        private val updater: ManualMetadataUpdater

        private val logContext = LogContext()

        init {
            val props = Properties()
            props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            addConfigsToProperties(props, spec.commonClientConf(), spec.commonClientConf())
            conf = AdminClientConfig(propsToMap(props))
            val addresses = parseAndValidateAddresses(
                urls = conf.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)!!,
                clientDnsLookupConfig = conf.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG)!!
            )
            updater = ManualMetadataUpdater(Cluster.bootstrap(addresses).nodes)
        }

        override fun tryConnect(): Boolean {
            return try {
                val nodes = updater.fetchNodes()
                val targetNode = nodes[ThreadLocalRandom.current().nextInt(nodes.size)]
                // channelBuilder will be closed as part of Selector.close()
                val channelBuilder = createChannelBuilder(conf, TIME, logContext)
                Metrics().use { metrics ->
                    Selector(
                        connectionMaxIdleMs = conf.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)!!,
                        metrics = metrics,
                        time = TIME,
                        metricGrpPrefix = "",
                        channelBuilder = channelBuilder,
                        logContext = logContext,
                    ).use { selector ->
                        NetworkClient(
                            selector = selector,
                            metadataUpdater = updater,
                            clientId = "ConnectionStressWorker",
                            maxInFlightRequestsPerConnection = 1,
                            reconnectBackoffMs = 1000,
                            reconnectBackoffMax = 1000,
                            socketSendBuffer = 4096,
                            socketReceiveBuffer = 4096,
                            defaultRequestTimeoutMs = 1000,
                            connectionSetupTimeoutMs = 10 * 1000,
                            connectionSetupTimeoutMaxMs = 127 * 1000,
                            time = TIME,
                            discoverBrokerVersions = false,
                            apiVersions = ApiVersions(),
                            logContext = logContext,
                        ).use { client ->
                            awaitReady(
                                client = client,
                                node = targetNode,
                                time = TIME,
                                timeoutMs = 500,
                            )
                        }
                    }
                }
                true
            } catch (_: IOException) {
                false
            }
        }

        @Throws(Exception::class)
        override fun close() {
            closeQuietly(updater, "ManualMetadataUpdater")
        }
    }

    internal class FetchMetadataStressor(spec: ConnectionStressSpec) : Stressor {

        private val props: Properties = Properties()

        init {
            props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            addConfigsToProperties(
                props = props,
                commonConf = spec.commonClientConf(),
                clientConf = spec.commonClientConf(),
            )
        }

        override fun tryConnect(): Boolean {
            try {
                Admin.create(props).use { client -> client.describeCluster().nodes.get() }
            } catch (_: RuntimeException) {
                return false
            } catch (_: Exception) {
                return false
            }
            return true
        }

        @Throws(Exception::class)
        override fun close() = Unit
    }

    inner class ConnectLoop : Runnable {
        override fun run() {
            val stressor = Stressor.fromSpec(spec)
            val rate = perSecToPerPeriod(
                perSec = spec.targetConnectionsPerSec().toFloat() / spec.numThreads(),
                periodMs = THROTTLE_PERIOD_MS.toLong(),
            )
            val throttle: Throttle = ConnectStressThrottle(rate)
            try {
                while (!doneFuture!!.isDone) {
                    throttle.increment()
                    val success = stressor.tryConnect()
                    synchronized(this@ConnectionStressWorker) {
                        totalConnections++
                        if (!success) totalFailedConnections++
                    }
                }
            } catch (exception: Exception) {
                abort(log, "ConnectLoop", exception, doneFuture!!)
            } finally {
                closeQuietly(stressor, "stressor")
            }
        }
    }

    private inner class StatusUpdater : Runnable {
        override fun run() {
            try {
                val lastTimeMs = Time.SYSTEM.milliseconds()
                var node: JsonNode
                synchronized(this@ConnectionStressWorker) {
                    node = JsonUtil.JSON_SERDE.valueToTree(
                        StatusData(
                            totalConnections = totalConnections,
                            totalFailedConnections = totalFailedConnections,
                            connectsPerSec = totalConnections * 1000.0 / (lastTimeMs - startTimeMs),
                        )
                    )
                }
                status!!.update(node)
            } catch (e: Exception) {
                abort(log, "StatusUpdater", e, doneFuture!!)
            }
        }
    }

    class StatusData @JsonCreator internal constructor(
        @param:JsonProperty("totalConnections") private val totalConnections: Long,
        @param:JsonProperty("totalFailedConnections") private val totalFailedConnections: Long,
        @param:JsonProperty("connectsPerSec") private val connectsPerSec: Double,
    ) {
        @JsonProperty
        fun totalConnections(): Long = totalConnections

        @JsonProperty
        fun totalFailedConnections(): Long = totalFailedConnections

        @JsonProperty
        fun connectsPerSec(): Double = connectsPerSec
    }

    companion object {

        private val log = LoggerFactory.getLogger(ConnectionStressWorker::class.java)

        private val TIME = Time.SYSTEM

        private const val THROTTLE_PERIOD_MS = 100

        private const val REPORT_INTERVAL_MS = 5000
    }
}
