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
import java.time.Duration
import java.util.Collections
import java.util.Optional
import java.util.Properties
import java.util.Random
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.WorkerUtils.abort
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class SustainedConnectionWorker(
    // This is the metadata for the test itself.
    private val id: String,
    private val spec: SustainedConnectionSpec,
) : TaskWorker {

    private var workerExecutor: ExecutorService? = null

    private val running = AtomicBoolean(false)

    private var doneFuture: KafkaFutureImpl<String>? = null

    private var connections: ArrayList<SustainedConnection>? = null

    private var status: WorkerStatusTracker? = null

    private var totalProducerConnections: AtomicLong? = null

    private var totalProducerFailedConnections: AtomicLong? = null

    private var totalConsumerConnections: AtomicLong? = null

    private var totalConsumerFailedConnections: AtomicLong? = null

    private var totalMetadataConnections: AtomicLong? = null

    private var totalMetadataFailedConnections: AtomicLong? = null

    private var totalAbortedThreads: AtomicLong? = null

    private var statusUpdaterFuture: Future<*>? = null

    private var statusUpdaterExecutor: ScheduledExecutorService? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) {
            "SustainedConnectionWorker is already running."
        }
        log.info("{}: Activating SustainedConnectionWorker with {}", id, spec)
        this.doneFuture = haltFuture
        this.status = status
        connections = ArrayList()

        // Initialize all status reporting metrics to 0.
        totalProducerConnections = AtomicLong(0)
        totalProducerFailedConnections = AtomicLong(0)
        totalConsumerConnections = AtomicLong(0)
        totalConsumerFailedConnections = AtomicLong(0)
        totalMetadataConnections = AtomicLong(0)
        totalMetadataFailedConnections = AtomicLong(0)
        totalAbortedThreads = AtomicLong(0)

        // Create the worker classes and add them to the list of items to act on.
        for (i in 0..<spec.producerConnectionCount())
            connections!!.add(ProducerSustainedConnection())

        for (i in 0..<spec.consumerConnectionCount())
            connections!!.add(ConsumerSustainedConnection())

        for (i in 0..<spec.metadataConnectionCount())
            connections!!.add(MetadataSustainedConnection())

        // Create the status reporter thread and schedule it.
        statusUpdaterExecutor = Executors.newScheduledThreadPool(
            1,
            createThreadFactory("StatusUpdaterWorkerThread%d", false)
        )
        statusUpdaterFuture = statusUpdaterExecutor!!.scheduleAtFixedRate(
            StatusUpdater(),
            0,
            REPORT_INTERVAL_MS.toLong(),
            TimeUnit.MILLISECONDS,
        )

        // Create the maintainer pool, add all the maintainer threads, then start it.
        workerExecutor = Executors.newFixedThreadPool(
            spec.numThreads(),
            createThreadFactory("SustainedConnectionWorkerThread%d", false)
        )
        for (i in 0..<spec.numThreads()) workerExecutor!!.submit(MaintainLoop())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        check(running.compareAndSet(true, false)) { "SustainedConnectionWorker is not running." }
        log.info("{}: Deactivating SustainedConnectionWorker.", id)

        // Shut down the periodic status updater and perform a final update on the
        // statistics.  We want to do this first, before deactivating any threads.
        // Otherwise, if some threads take a while to terminate, this could lead
        // to a misleading rate getting reported.
        statusUpdaterFuture!!.cancel(false)
        statusUpdaterExecutor!!.shutdown()
        statusUpdaterExecutor!!.awaitTermination(1, TimeUnit.HOURS)
        statusUpdaterExecutor = null
        StatusUpdater().run()
        doneFuture!!.complete("")
        connections!!.forEach(SustainedConnection::close)

        workerExecutor!!.shutdownNow()
        workerExecutor!!.awaitTermination(1, TimeUnit.HOURS)
        workerExecutor = null
        status = null
        connections = null
    }

    private interface SustainedConnection : AutoCloseable {

        fun needsRefresh(milliseconds: Long): Boolean

        fun refresh()

        fun claim()
    }

    private abstract inner class ClaimableConnection : SustainedConnection {

        protected var nextUpdate: Long = 0

        protected var inUse = false

        protected var refreshRate: Long = 0

        override fun needsRefresh(milliseconds: Long): Boolean {
            return !inUse && milliseconds > nextUpdate
        }

        override fun claim() {
            inUse = true
        }

        @Throws(Exception::class)
        override fun close() = closeQuietly()

        protected fun completeRefresh() {
            nextUpdate = SYSTEM_TIME.milliseconds() + refreshRate
            inUse = false
        }

        protected abstract fun closeQuietly()
    }

    private inner class MetadataSustainedConnection : ClaimableConnection() {

        private var client: Admin? = null

        private val props: Properties

        init {

            // These variables are used to maintain the connection itself.
            refreshRate = spec.refreshRateMs().toLong()

            // This variable is used to maintain the connection properties.
            props = Properties()
            props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            addConfigsToProperties(
                props = props,
                commonConf = spec.commonClientConf(),
                clientConf = spec.commonClientConf(),
            )
        }

        override fun refresh() {
            try {
                if (client == null) {
                    // Housekeeping to track the number of opened connections.
                    totalMetadataConnections!!.incrementAndGet()

                    // Create the admin client connection.
                    client = Admin.create(props)
                }

                // Fetch some metadata to keep the connection alive.
                client!!.describeCluster().nodes.get()
            } catch (exception: Throwable) {
                // Set the admin client to be recreated on the next cycle.
                this.closeQuietly()

                // Housekeeping to track the number of opened connections and failed connection attempts.
                totalMetadataConnections!!.decrementAndGet()
                totalMetadataFailedConnections!!.incrementAndGet()
                log.error("Error while refreshing sustained AdminClient connection", exception)
            }

            // Schedule this again and set to not in use.
            completeRefresh()
        }

        override fun closeQuietly() {
            closeQuietly(client, "AdminClient")
            client = null
        }
    }

    private inner class ProducerSustainedConnection : ClaimableConnection() {

        private var producer: KafkaProducer<ByteArray?, ByteArray?>? = null

        private var partitions: List<TopicPartition>? = null

        private var partitionsIterator: Iterator<TopicPartition?>? = null

        private val topicName = spec.topicName()

        private val keys = PayloadIterator(spec.keyGenerator())

        private val values = PayloadIterator(spec.valueGenerator())

        // This variable is used to maintain the connection properties.
        private val props: Properties = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers())
        }

        init {
            // These variables are used to maintain the connection itself.
            refreshRate = spec.refreshRateMs().toLong()

            addConfigsToProperties(
                props = props,
                commonConf = spec.commonClientConf(),
                clientConf = spec.producerConf(),
            )
        }

        override fun refresh() {
            try {
                if (producer == null) {
                    // Housekeeping to track the number of opened connections.
                    totalProducerConnections!!.incrementAndGet()

                    // Create the producer, fetch the specified topic's partitions and randomize them.
                    producer = KafkaProducer(
                        properties = props,
                        keySerializer = ByteArraySerializer(),
                        valueSerializer = ByteArraySerializer(),
                    )
                    partitions = producer!!.partitionsFor(topicName)
                        .map { (topic, partition) -> TopicPartition(topic, partition) }
                        .shuffled()
                }

                // Create a new iterator over the partitions if the current one doesn't exist or is exhausted.
                if (partitionsIterator == null || !partitionsIterator!!.hasNext()) {
                    partitionsIterator = partitions!!.iterator()
                }

                // Produce a single record and send it synchronously.
                val partition = partitionsIterator!!.next()
                val record = ProducerRecord<ByteArray?, ByteArray?>(
                    topic = partition!!.topic,
                    partition = partition.partition,
                    key = keys.next(),
                    value = values.next(),
                )
                producer!!.send(record).get()
            } catch (exception: Throwable) {
                // Set the producer to be recreated on the next cycle.
                this.closeQuietly()

                // Housekeeping to track the number of opened connections and failed connection attempts.
                totalProducerConnections!!.decrementAndGet()
                totalProducerFailedConnections!!.incrementAndGet()
                log.error("Error while refreshing sustained KafkaProducer connection", exception)
            }

            // Schedule this again and set to not in use.
            completeRefresh()
        }

        override fun closeQuietly() {
            closeQuietly(producer, "KafkaProducer")
            producer = null
            partitions = null
            partitionsIterator = null
        }
    }

    private inner class ConsumerSustainedConnection() : ClaimableConnection() {

        private var consumer: KafkaConsumer<ByteArray?, ByteArray?>? = null

        private var activePartition: TopicPartition? = null

        // These variables are used to maintain the connection itself.
        private val topicName: String = spec.topicName()
        private val rand: Random = Random()

        // This variable is used to maintain the connection properties.
        private val props: Properties = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers())
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
            put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024)
        }

        init {
            // This variable is used to maintain the connection itself.
            refreshRate = spec.refreshRateMs().toLong()

            addConfigsToProperties(
                props = props,
                commonConf = spec.commonClientConf(),
                clientConf = spec.consumerConf(),
            )
        }

        override fun refresh() {
            try {
                if (consumer == null) {

                    // Housekeeping to track the number of opened connections.
                    totalConsumerConnections!!.incrementAndGet()

                    // Create the consumer and fetch the partitions for the specified topic.
                    consumer = KafkaConsumer(
                        properties = props,
                        keyDeserializer = ByteArrayDeserializer(),
                        valueDeserializer = ByteArrayDeserializer(),
                    )
                    val partitions = consumer!!.partitionsFor(topicName)
                        .map { (topic, partition) -> TopicPartition(topic, partition) }

                    // Select a random partition and assign it.
                    activePartition = partitions[rand.nextInt(partitions.size)]
                    consumer!!.assign(listOf(activePartition!!))
                }

                // The behavior when passing in an empty list is to seek to the end of all subscribed partitions.
                consumer!!.seekToEnd(emptyList())

                // Poll to keep the connection alive, ignoring any records returned.
                consumer!!.poll(Duration.ofMillis(50))
            } catch (exception: Throwable) {

                // Set the consumer to be recreated on the next cycle.
                this.closeQuietly()

                // Housekeeping to track the number of opened connections and failed connection attempts.
                totalConsumerConnections!!.decrementAndGet()
                totalConsumerFailedConnections!!.incrementAndGet()
                log.error("Error while refreshing sustained KafkaConsumer connection", exception)
            }

            // Schedule this again and set to not in use.
            completeRefresh()
        }

        override fun closeQuietly() {
            closeQuietly(consumer, "KafkaConsumer")
            consumer = null
            activePartition = null
        }
    }

    inner class MaintainLoop : Runnable {

        override fun run() {
            try {
                while (!doneFuture!!.isDone) {
                    findConnectionToMaintain()?.refresh() ?: run {
                        SYSTEM_TIME.sleep(BACKOFF_PERIOD_MS.toLong())
                    }
                }
            } catch (exception: Exception) {
                totalAbortedThreads!!.incrementAndGet()
                log.error("Aborted thread while maintaining sustained connections", exception)
            }
        }
    }

    @Synchronized
    private fun findConnectionToMaintain(): SustainedConnection? {
        val milliseconds = SYSTEM_TIME.milliseconds()
        for (connection in connections!!) {
            if (connection.needsRefresh(milliseconds)) {
                connection.claim()
                return connection
            }
        }
        return null
    }

    private inner class StatusUpdater : Runnable {
        override fun run() {
            try {
                val node = JsonUtil.JSON_SERDE.valueToTree<JsonNode>(
                    StatusData(
                        totalProducerConnections = totalProducerConnections!!.get(),
                        totalProducerFailedConnections = totalProducerFailedConnections!!.get(),
                        totalConsumerConnections = totalConsumerConnections!!.get(),
                        totalConsumerFailedConnections = totalConsumerFailedConnections!!.get(),
                        totalMetadataConnections = totalMetadataConnections!!.get(),
                        totalMetadataFailedConnections = totalMetadataFailedConnections!!.get(),
                        totalAbortedThreads = totalAbortedThreads!!.get(),
                        updatedMs = SYSTEM_TIME.milliseconds(),
                    )
                )
                status!!.update(node)
            } catch (exception: Exception) {
                log.error("Aborted test while running StatusUpdater", exception)
                abort(log, "StatusUpdater", exception, doneFuture!!)
            }
        }
    }

    class StatusData @JsonCreator internal constructor(
        @param:JsonProperty("totalProducerConnections") private val totalProducerConnections: Long,
        @param:JsonProperty("totalProducerFailedConnections") private val totalProducerFailedConnections: Long,
        @param:JsonProperty("totalConsumerConnections") private val totalConsumerConnections: Long,
        @param:JsonProperty("totalConsumerFailedConnections") private val totalConsumerFailedConnections: Long,
        @param:JsonProperty("totalMetadataConnections") private val totalMetadataConnections: Long,
        @param:JsonProperty("totalMetadataFailedConnections") private val totalMetadataFailedConnections: Long,
        @param:JsonProperty("totalAbortedThreads") private val totalAbortedThreads: Long,
        @param:JsonProperty("updatedMs") private val updatedMs: Long,
    ) {

        @JsonProperty
        fun totalProducerConnections(): Long = totalProducerConnections

        @JsonProperty
        fun totalProducerFailedConnections(): Long = totalProducerFailedConnections

        @JsonProperty
        fun totalConsumerConnections(): Long = totalConsumerConnections

        @JsonProperty
        fun totalConsumerFailedConnections(): Long = totalConsumerFailedConnections

        @JsonProperty
        fun totalMetadataConnections(): Long = totalMetadataConnections

        @JsonProperty
        fun totalMetadataFailedConnections(): Long = totalMetadataFailedConnections

        @JsonProperty
        fun totalAbortedThreads(): Long = totalAbortedThreads

        @JsonProperty
        fun updatedMs(): Long = updatedMs
    }

    companion object {

        private val log = LoggerFactory.getLogger(SustainedConnectionWorker::class.java)

        private val SYSTEM_TIME = SystemTime()

        // These variables are used to maintain the connections.
        private const val BACKOFF_PERIOD_MS = 10

        // These variables are used when tracking the reported status of the worker.
        private const val REPORT_INTERVAL_MS = 5000
    }
}
