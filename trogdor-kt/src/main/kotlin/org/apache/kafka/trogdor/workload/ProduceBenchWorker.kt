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
import com.fasterxml.jackson.databind.node.TextNode
import java.util.Optional
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.WorkerUtils.abort
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.common.WorkerUtils.createTopics
import org.apache.kafka.trogdor.common.WorkerUtils.perSecToPerPeriod
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.apache.kafka.trogdor.workload.TransactionGenerator.TransactionAction
import org.slf4j.LoggerFactory

class ProduceBenchWorker(
    private val id: String,
    private val spec: ProduceBenchSpec,
) : TaskWorker {
    
    private val running = AtomicBoolean(false)
    
    private var executor: ScheduledExecutorService? = null
    
    private var status: WorkerStatusTracker? = null
    
    private var doneFuture: KafkaFutureImpl<String>? = null
    
    override fun start(
        platform: Platform?,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) {
            "ProducerBenchWorker is already running."
        }
        log.info("{}: Activating ProduceBenchWorker with {}", id, spec)
        // Create an executor with 2 threads.  We need the second thread so
        // that the StatusUpdater can run in parallel with SendRecords.
        executor = Executors.newScheduledThreadPool(
            2,
            createThreadFactory("ProduceBenchWorkerThread%d", false)
        )
        this.status = status
        this.doneFuture = haltFuture
        executor!!.submit(Prepare())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform?) {
        check(running.compareAndSet(true, false)) { "ProduceBenchWorker is not running." }
        log.info("{}: Deactivating ProduceBenchWorker.", id)
        doneFuture!!.complete("")
        executor!!.shutdownNow()
        executor!!.awaitTermination(1, TimeUnit.DAYS)
        executor = null
        status = null
        doneFuture = null
    }

    inner class Prepare : Runnable {

        override fun run() {
            try {
                val newTopics = mutableMapOf<String, NewTopic>()
                val active = mutableSetOf<TopicPartition>()
                for ((topicName, partSpec) in spec.activeTopics().materialize()) {
                    newTopics[topicName] = partSpec.newTopic(topicName)
                    for (partitionNumber in partSpec.partitionNumbers()) {
                        active.add(TopicPartition(topicName, partitionNumber))
                    }
                }
                if (active.isEmpty()) throw RuntimeException("You must specify at least one active topic.")
                
                for ((topicName, partSpec) in spec.inactiveTopics().materialize()) {
                    newTopics[topicName] = partSpec.newTopic(topicName)
                }
                status!!.update(TextNode("Creating " + newTopics.keys.size + " topic(s)"))
                createTopics(
                    log = log,
                    bootstrapServers = spec.bootstrapServers(),
                    commonClientConf = spec.commonClientConf(),
                    adminClientConf = spec.adminClientConf(),
                    topics = newTopics,
                    failOnExisting = false,
                )
                status!!.update(TextNode("Created " + newTopics.keys.size + " topic(s)"))
                executor!!.submit(SendRecords(active))
            } catch (exception: Throwable) {
                abort(log, "Prepare", exception, doneFuture!!)
            }
        }
    }

    private class SendRecordsCallback(
        private val sendRecords: SendRecords,
        private val startMs: Long,
    ) : Callback {

        override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
            val now = Time.SYSTEM.milliseconds()
            val durationMs = now - startMs
            sendRecords.recordDuration(durationMs)
            if (exception != null) log.error("SendRecordsCallback: error", exception)
        }
    }

    /**
     * A subclass of Throttle which flushes the Producer right before the throttle injects a delay.
     * This avoids including throttling latency in latency measurements.
     */
    private class SendRecordsThrottle(
        maxPerPeriod: Int,
        private val producer: KafkaProducer<*, *>,
    ) : Throttle(maxPerPeriod, THROTTLE_PERIOD_MS) {
        
        @Synchronized
        @Throws(InterruptedException::class)
        override fun delay(amount: Long) {
            val startMs = time.milliseconds()
            producer.flush()
            val endMs = time.milliseconds()
            val delta = endMs - startMs
            super.delay(amount - delta)
        }
    }

    inner class SendRecords internal constructor(
        private val activePartitions: MutableSet<TopicPartition>,
    ) : Callable<Unit> {

        private val histogram: Histogram

        private val statusUpdaterFuture: Future<*>

        private val producer: KafkaProducer<ByteArray?, ByteArray?>

        private val keys: PayloadIterator

        private val values: PayloadIterator

        private val transactionGenerator: TransactionGenerator?

        private var throttle: Throttle? = null

        private var partitionsIterator: Iterator<TopicPartition>

        private var sendFuture: Future<RecordMetadata>? = null

        private val transactionsCommitted: AtomicLong

        private val enableTransactions: Boolean

        init {
            partitionsIterator = activePartitions.iterator()
            histogram = Histogram(10000)
            transactionGenerator = spec.transactionGenerator()
            enableTransactions = transactionGenerator != null
            transactionsCommitted = AtomicLong()
            val perPeriod = perSecToPerPeriod(
                perSec = spec.targetMessagesPerSec().toFloat(),
                periodMs = THROTTLE_PERIOD_MS.toLong(),
            )
            statusUpdaterFuture = executor!!.scheduleWithFixedDelay(
                StatusUpdater(histogram, transactionsCommitted),
                30,
                30,
                TimeUnit.SECONDS,
            )
            val props = Properties()
            props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            if (enableTransactions)
                props[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "produce-bench-transaction-id-${UUID.randomUUID()}"
            // add common client configs to producer properties, and then user-specified producer configs
            addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf())
            producer = KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer())
            keys = PayloadIterator(spec.keyGenerator())
            values = PayloadIterator(spec.valueGenerator())
            throttle = if (spec.skipFlush()) Throttle(perPeriod, THROTTLE_PERIOD_MS)
            else SendRecordsThrottle(perPeriod, producer)
        }

        @Throws(Exception::class)
        override fun call() {
            val startTimeMs = Time.SYSTEM.milliseconds()
            try {
                try {
                    if (enableTransactions) producer.initTransactions()
                    var sentMessages: Long = 0
                    while (sentMessages < spec.maxMessages()) {
                        if (enableTransactions) {
                            val tookAction = takeTransactionAction()
                            if (tookAction) continue
                        }
                        sendMessage()
                        sentMessages++
                    }
                    // give the transactionGenerator a chance to commit if configured evenly
                    if (enableTransactions) takeTransactionAction()
                } catch (exception: Exception) {
                    if (enableTransactions) producer.abortTransaction()
                    throw exception
                } finally {
                    if (sendFuture != null) try {
                        sendFuture!!.get()
                    } catch (exception: Exception) {
                        log.error("Exception on final future", exception)
                    }

                    producer.close()
                }
            } catch (exception: Exception) {
                abort(log, "SendRecords", exception, doneFuture!!)
            } finally {
                statusUpdaterFuture.cancel(false)
                val statusData: StatusData = StatusUpdater(histogram, transactionsCommitted).update()
                val curTimeMs = Time.SYSTEM.milliseconds()
                log.info(
                    "Sent {} total record(s) in {} ms.  status: {}",
                    histogram.summarize().numSamples, curTimeMs - startTimeMs, statusData
                )
            }
            doneFuture!!.complete("")
        }

        private fun takeTransactionAction(): Boolean {
            var tookAction = true
            val nextAction = transactionGenerator!!.nextAction()
            when (nextAction) {
                TransactionAction.BEGIN_TRANSACTION -> {
                    log.debug("Beginning transaction.")
                    producer.beginTransaction()
                }

                TransactionAction.COMMIT_TRANSACTION -> {
                    log.debug("Committing transaction.")
                    producer.commitTransaction()
                    transactionsCommitted.getAndIncrement()
                }

                TransactionAction.ABORT_TRANSACTION -> {
                    log.debug("Aborting transaction.")
                    producer.abortTransaction()
                }

                TransactionAction.NO_OP -> tookAction = false
                null -> Unit
            }
            return tookAction
        }

        @Throws(InterruptedException::class)
        private fun sendMessage() {
            if (!partitionsIterator.hasNext()) partitionsIterator = activePartitions.iterator()
            val (topic, partition1) = partitionsIterator.next()
            val record: ProducerRecord<ByteArray?, ByteArray?> =
                if (spec.useConfiguredPartitioner()) ProducerRecord(
                    topic = topic,
                    key = keys.next(),
                    value = values.next(),
                )
                else ProducerRecord(
                    topic = topic,
                    partition = partition1,
                    key = keys.next(),
                    value = values.next(),
                )

            sendFuture = producer.send(
                record = record,
                callback = SendRecordsCallback(
                    sendRecords = this,
                    startMs = Time.SYSTEM.milliseconds(),
                )
            )
            throttle!!.increment()
        }

        fun recordDuration(durationMs: Long) = histogram.add(durationMs)
    }

    inner class StatusUpdater internal constructor(
        private val histogram: Histogram,
        private val transactionsCommitted: AtomicLong,
    ) : Runnable {

        override fun run() {
            try {
                update()
            } catch (exception: Exception) {
                abort(log, "StatusUpdater", exception, doneFuture!!)
            }
        }

        fun update(): StatusData {
            val summary = histogram.summarize(StatusData.PERCENTILES)
            val statusData = StatusData(
                totalSent = summary.numSamples, averageLatencyMs = summary.average,
                p50LatencyMs = summary.percentiles[0].value,
                p95LatencyMs = summary.percentiles[1].value,
                p99LatencyMs = summary.percentiles[2].value,
                transactionsCommitted = transactionsCommitted.get(),
            )
            status!!.update(JsonUtil.JSON_SERDE.valueToTree(statusData))
            return statusData
        }
    }

    class StatusData @JsonCreator internal constructor(
        @param:JsonProperty("totalSent") private val totalSent: Long,
        @param:JsonProperty("averageLatencyMs") private val averageLatencyMs: Float,
        @param:JsonProperty("p50LatencyMs") private val p50LatencyMs: Int,
        @param:JsonProperty("p95LatencyMs") private val p95LatencyMs: Int,
        @param:JsonProperty("p99LatencyMs") private val p99LatencyMs: Int,
        @param:JsonProperty("transactionsCommitted") private val transactionsCommitted: Long,
    ) {

        @JsonProperty
        fun totalSent(): Long = totalSent

        @JsonProperty
        fun transactionsCommitted(): Long = transactionsCommitted

        @JsonProperty
        fun averageLatencyMs(): Float = averageLatencyMs

        @JsonProperty
        fun p50LatencyMs(): Int = p50LatencyMs

        @JsonProperty
        fun p95LatencyMs(): Int = p95LatencyMs

        @JsonProperty
        fun p99LatencyMs(): Int = p99LatencyMs

        companion object {

            /**
             * The percentiles to use when calculating the histogram data.
             * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
             */
            val PERCENTILES = floatArrayOf(0.5f, 0.95f, 0.99f)
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(ProduceBenchWorker::class.java)

        private const val THROTTLE_PERIOD_MS = 100
    }
}
