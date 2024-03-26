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
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Collectors
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.JsonUtil.toJsonString
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.WorkerUtils.abort
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.common.WorkerUtils.perSecToPerPeriod
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class ConsumeBenchWorker(private val id: String, private val spec: ConsumeBenchSpec) : TaskWorker {

    private val running = AtomicBoolean(false)

    private var executor: ScheduledExecutorService? = null

    private var workerStatus: WorkerStatusTracker? = null

    private var statusUpdater: StatusUpdater? = null

    private var statusUpdaterFuture: Future<*>? = null

    private var doneFuture: KafkaFutureImpl<String>? = null

    private var consumer: ThreadSafeConsumer? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) {
            throw IllegalStateException("ConsumeBenchWorker is already running.")
        }
        log.info("{}: Activating ConsumeBenchWorker with {}", id, spec)
        statusUpdater = StatusUpdater()
        executor = Executors.newScheduledThreadPool(
            spec.threadsPerWorker() + 2, // 1 thread for all the ConsumeStatusUpdater and 1 for the StatusUpdater
            createThreadFactory("ConsumeBenchWorkerThread%d", false)
        )
        statusUpdaterFuture = executor!!.scheduleAtFixedRate(statusUpdater, 1, 1, TimeUnit.MINUTES)
        workerStatus = status
        this.doneFuture = haltFuture
        executor!!.submit(Prepare())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        check(running.compareAndSet(true, false)) { "ConsumeBenchWorker is not running." }
        log.info("{}: Deactivating ConsumeBenchWorker.", id)
        doneFuture!!.complete("")
        executor!!.shutdownNow()
        executor!!.awaitTermination(1, TimeUnit.DAYS)
        consumer!!.close()
        consumer = null
        executor = null
        statusUpdater = null
        statusUpdaterFuture = null
        workerStatus = null
        doneFuture = null
    }

    inner class Prepare : Runnable {
        override fun run() {
            try {
                val consumeTasks: MutableList<Future<Unit>> = ArrayList()
                for (task: ConsumeMessages in consumeTasks()) {
                    consumeTasks.add(executor!!.submit(task))
                }
                executor!!.submit(CloseStatusUpdater(consumeTasks))
            } catch (throwable: Throwable) {
                abort(log, "Prepare", throwable, doneFuture!!)
            }
        }

        private fun consumeTasks(): List<ConsumeMessages> {
            val tasks: MutableList<ConsumeMessages> = ArrayList()
            val consumerGroup = consumerGroup()
            val consumerCount = spec.threadsPerWorker()
            val partitionsByTopic = spec.materializeTopics()
            val toUseGroupPartitionAssignment = partitionsByTopic.values.all { it.isEmpty() }
            if (!toUseGroupPartitionAssignment && !toUseRandomConsumeGroup() && consumerCount > 1)
                throw ConfigException(
                    "You may not specify an explicit partition assignment when using multiple consumers " +
                            "in the same group.Please leave the consumer group unset, specify topics " +
                            "instead of partitions or use a single consumer."
            )
            consumer = consumer(consumerGroup, clientId(0))
            if (toUseGroupPartitionAssignment) {
                val topics = partitionsByTopic.keys
                tasks.add(ConsumeMessages(consumer!!, spec.recordProcessor(), topics))
                for (i in 0..<(consumerCount - 1)) {
                    tasks.add(
                        ConsumeMessages(
                            consumer = consumer(consumerGroup(), clientId(i + 1)),
                            recordProcessor = spec.recordProcessor(),
                            topics = topics,
                        )
                    )
                }
            } else {
                val partitions = populatePartitionsByTopic(
                    consumer = consumer!!.consumer,
                    materializedTopics = partitionsByTopic,
                ).values.flatten()

                tasks.add(ConsumeMessages(consumer!!, spec.recordProcessor(), partitions))
                for (i in 0..<(consumerCount - 1)) tasks.add(
                    ConsumeMessages(
                        consumer = consumer(consumerGroup(), clientId(i + 1)),
                        recordProcessor = spec.recordProcessor(),
                        partitions = partitions,
                    )
                )
            }
            return tasks
        }

        private fun clientId(idx: Int): String = "consumer.$id-$idx"

        /**
         * Creates a new KafkaConsumer instance
         */
        private fun consumer(consumerGroup: String, clientId: String): ThreadSafeConsumer {
            val props = Properties()
            props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            props[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
            props[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroup
            props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 100000
            // these defaults maybe over-written by the user-specified commonClientConf or consumerConf
            addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf())
            return ThreadSafeConsumer(
                consumer = KafkaConsumer(
                    properties = props,
                    keyDeserializer = ByteArrayDeserializer(),
                    valueDeserializer = ByteArrayDeserializer(),
                ),
                clientId = clientId,
            )
        }

        private fun consumerGroup(): String {
            return if (toUseRandomConsumeGroup()) "consume-bench-" + UUID.randomUUID()
            else spec.consumerGroup()
        }

        private fun toUseRandomConsumeGroup(): Boolean = spec.consumerGroup().isEmpty()

        private fun populatePartitionsByTopic(
            consumer: KafkaConsumer<ByteArray?, ByteArray?>,
            materializedTopics: MutableMap<String, MutableList<TopicPartition>>,
        ): Map<String, List<TopicPartition>> {
            // fetch partitions for topics who do not have any listed
            for ((topicName, partitions) in materializedTopics) {
                if (partitions.isEmpty()) {
                    val fetchedPartitions = consumer.partitionsFor(topicName)
                        .map { partitionInfo -> TopicPartition(partitionInfo.topic, partitionInfo.partition) }
                    partitions.addAll(fetchedPartitions)
                }
                materializedTopics[topicName] = partitions
            }
            return materializedTopics
        }
    }

    inner class ConsumeMessages private constructor(
        consumer: ThreadSafeConsumer,
        recordProcessor: RecordProcessor?,
    ) : Callable<Unit> {

        private val latencyHistogram: Histogram

        private val messageSizeHistogram: Histogram

        private val statusUpdaterFuture: Future<*>

        private val throttle: Throttle

        private val clientId: String

        private val consumer: ThreadSafeConsumer

        private val recordProcessor: RecordProcessor?

        init {
            latencyHistogram = Histogram(10000)
            messageSizeHistogram = Histogram(2 * 1024 * 1024)
            clientId = consumer.clientId
            this.statusUpdaterFuture = executor!!.scheduleAtFixedRate(
                ConsumeStatusUpdater(
                    latencyHistogram = latencyHistogram,
                    messageSizeHistogram = messageSizeHistogram,
                    consumer = consumer,
                    recordProcessor = recordProcessor,
                ),
                1,
                1,
                TimeUnit.MINUTES,
            )
            val perPeriod = if (spec.targetMessagesPerSec() <= 0) Int.MAX_VALUE
            else perSecToPerPeriod(spec.targetMessagesPerSec().toFloat(), THROTTLE_PERIOD_MS.toLong())

            throttle = Throttle(perPeriod, THROTTLE_PERIOD_MS)
            this.consumer = consumer
            this.recordProcessor = recordProcessor
        }

        internal constructor(
            consumer: ThreadSafeConsumer,
            recordProcessor: RecordProcessor?,
            topics: Set<String>,
        ) : this(consumer, recordProcessor) {
            log.info("Will consume from topics {} via dynamic group assignment.", topics)
            this.consumer.subscribe(topics)
        }

        internal constructor(
            consumer: ThreadSafeConsumer,
            recordProcessor: RecordProcessor?,
            partitions: List<TopicPartition>,
        ) : this(consumer, recordProcessor) {
            log.info("Will consume from topic partitions {} via manual assignment.", partitions)
            this.consumer.assign(partitions)
        }

        @Throws(Exception::class)
        override fun call() {
            var messagesConsumed: Long = 0
            var bytesConsumed: Long = 0
            val startTimeMs = Time.SYSTEM.milliseconds()
            var startBatchMs = startTimeMs
            val maxMessages = spec.maxMessages()
            try {
                while (messagesConsumed < maxMessages) {
                    val records = consumer.poll()
                    if (records.isEmpty) {
                        continue
                    }
                    val endBatchMs = Time.SYSTEM.milliseconds()
                    val elapsedBatchMs = endBatchMs - startBatchMs

                    // Do the record batch processing immediately to avoid latency skew.
                    recordProcessor?.processRecords(records)

                    for (record in records) {
                        messagesConsumed++
                        var messageBytes: Long = 0
                        if (record.key != null) {
                            messageBytes += record.serializedKeySize.toLong()
                        }
                        if (record.value != null) {
                            messageBytes += record.serializedValueSize.toLong()
                        }
                        latencyHistogram.add(elapsedBatchMs)
                        messageSizeHistogram.add(messageBytes)
                        bytesConsumed += messageBytes
                        if (messagesConsumed >= maxMessages) break
                        throttle.increment()
                    }
                    startBatchMs = Time.SYSTEM.milliseconds()
                }
            } catch (exception: Exception) {
                abort(log, "ConsumeRecords", exception, (doneFuture)!!)
            } finally {
                statusUpdaterFuture.cancel(false)
                val statusData: StatusData = ConsumeStatusUpdater(
                    latencyHistogram,
                    messageSizeHistogram,
                    consumer,
                    spec.recordProcessor()
                ).update()
                val curTimeMs = Time.SYSTEM.milliseconds()
                log.info(
                    "{} Consumed total number of messages={}, bytes={} in {} ms.  status: {}",
                    clientId, messagesConsumed, bytesConsumed, curTimeMs - startTimeMs, statusData
                )
            }
            consumer.close()
        }
    }

    inner class CloseStatusUpdater internal constructor(private val consumeTasks: List<Future<Unit>>) :
        Runnable {
        override fun run() {
            while (!consumeTasks.all { it.isDone }) {
                try {
                    Thread.sleep(60000)
                } catch (e: InterruptedException) {
                    log.debug("{} was interrupted. Closing...", this.javaClass.getName())
                    break // close the thread
                }
            }
            statusUpdaterFuture!!.cancel(false)
            statusUpdater!!.update()
            doneFuture!!.complete("")
        }
    }

    internal inner class StatusUpdater : Runnable {

        val statuses = mutableMapOf<String, JsonNode>()

        override fun run() {
            try {
                update()
            } catch (exception: Exception) {
                abort(log, "ConsumeStatusUpdater", exception, doneFuture!!)
            }
        }

        @Synchronized
        fun update() = workerStatus!!.update(JsonUtil.JSON_SERDE.valueToTree(statuses))

        @Synchronized
        fun updateConsumeStatus(clientId: String, status: StatusData?) {
            statuses[clientId] = JsonUtil.JSON_SERDE.valueToTree(status)
        }
    }

    /**
     * Runnable class that updates the status of a single consumer
     */
    inner class ConsumeStatusUpdater internal constructor(
        private val latencyHistogram: Histogram,
        private val messageSizeHistogram: Histogram,
        private val consumer: ThreadSafeConsumer,
        private val recordProcessor: RecordProcessor?,
    ) : Runnable {

        override fun run() {
            try {
                update()
            } catch (exception: Exception) {
                abort(log, "ConsumeStatusUpdater", exception, (doneFuture)!!)
            }
        }

        fun update(): StatusData {
            val latSummary = latencyHistogram.summarize(StatusData.PERCENTILES)
            val msgSummary = messageSizeHistogram.summarize(StatusData.PERCENTILES)

            // Parse out the RecordProcessor's status, id specified.
            var recordProcessorStatus: JsonNode? = null
            if (recordProcessor != null) {
                recordProcessorStatus = recordProcessor.processorStatus()!!
            }
            val statusData = StatusData(
                assignedPartitions = consumer.assignedPartitions(),
                totalMessagesReceived = latSummary.numSamples,
                totalBytesReceived = (msgSummary.numSamples * msgSummary.average).toLong(),
                averageMessageSizeBytes = msgSummary.average.toLong(),
                averageLatencyMs = latSummary.average,
                p50LatencyMs = latSummary.percentiles[0].value,
                p95LatencyMs = latSummary.percentiles[1].value,
                p99LatencyMs = latSummary.percentiles[2].value,
                recordProcessorStatus = recordProcessorStatus,
            )
            statusUpdater!!.updateConsumeStatus(consumer.clientId, statusData)
            log.info("Status={}", toJsonString(statusData))
            return statusData
        }
    }

    class StatusData @JsonCreator internal constructor(
        @param:JsonProperty("assignedPartitions") private val assignedPartitions: List<String>,
        @param:JsonProperty("totalMessagesReceived") private val totalMessagesReceived: Long,
        @param:JsonProperty("totalBytesReceived") private val totalBytesReceived: Long,
        @param:JsonProperty("averageMessageSizeBytes") private val averageMessageSizeBytes: Long,
        @param:JsonProperty("averageLatencyMs") private val averageLatencyMs: Float,
        @param:JsonProperty("p50LatencyMs") private val p50LatencyMs: Int,
        @param:JsonProperty("p95LatencyMs") private val p95LatencyMs: Int,
        @param:JsonProperty("p99LatencyMs") private val p99LatencyMs: Int,
        @param:JsonProperty("recordProcessorStatus") private val recordProcessorStatus: JsonNode?,
    ) {
        @JsonProperty
        fun assignedPartitions(): List<String> = assignedPartitions

        @JsonProperty
        fun totalMessagesReceived(): Long = totalMessagesReceived

        @JsonProperty
        fun totalBytesReceived(): Long = totalBytesReceived

        @JsonProperty
        fun averageMessageSizeBytes(): Long = averageMessageSizeBytes

        @JsonProperty
        fun averageLatencyMs(): Float = averageLatencyMs

        @JsonProperty
        fun p50LatencyMs(): Int = p50LatencyMs

        @JsonProperty
        fun p95LatencyMs(): Int = p95LatencyMs

        @JsonProperty
        fun p99LatencyMs(): Int = p99LatencyMs

        @JsonProperty
        fun recordProcessorStatus(): JsonNode? = recordProcessorStatus

        companion object {

            /**
             * The percentiles to use when calculating the histogram data.
             * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
             */
            val PERCENTILES = floatArrayOf(0.5f, 0.95f, 0.99f)
        }
    }

    /**
     * A thread-safe KafkaConsumer wrapper
     */
    internal class ThreadSafeConsumer internal constructor(
        val consumer: KafkaConsumer<ByteArray?, ByteArray?>,
        val clientId: String,
    ) {

        private val consumerLock = ReentrantLock()

        private var closed = false

        fun poll(): ConsumerRecords<ByteArray?, ByteArray?> {
            consumerLock.lock()
            try {
                return consumer.poll(Duration.ofMillis(50))
            } finally {
                consumerLock.unlock()
            }
        }

        fun close() {
            if (closed) return
            consumerLock.lock()
            try {
                consumer.unsubscribe()
                closeQuietly(consumer, "consumer")
                closed = true
            } finally {
                consumerLock.unlock()
            }
        }

        fun subscribe(topics: Set<String>) {
            consumerLock.lock()
            try {
                consumer.subscribe(topics)
            } finally {
                consumerLock.unlock()
            }
        }

        fun assign(partitions: Collection<TopicPartition>) {
            consumerLock.lock()
            try {
                consumer.assign(partitions)
            } finally {
                consumerLock.unlock()
            }
        }

        fun assignedPartitions(): List<String> {
            consumerLock.lock()
            try {
                return consumer.assignment().map { it.toString() }
            } finally {
                consumerLock.unlock()
            }
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("clientId"),
        )
        fun clientId(): String = clientId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("consumer"),
        )
        fun consumer(): KafkaConsumer<ByteArray?, ByteArray?> = consumer
    }

    companion object {

        private val log = LoggerFactory.getLogger(ConsumeBenchWorker::class.java)

        private val THROTTLE_PERIOD_MS = 100
    }
}
