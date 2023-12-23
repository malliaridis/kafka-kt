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
import java.util.Properties
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
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
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

/**
 * This workload allows for customized and even variable configurations in terms of messages per second,
 * message size, batch size, key size, and even the ability to target a specific partition out of a topic.
 *
 * See [ConfigurableProducerSpec] for a more detailed description.
 */
class ConfigurableProducerWorker(
    private val id: String,
    private val spec: ConfigurableProducerSpec,
) : TaskWorker {

    private val running = AtomicBoolean(false)

    private var executor: ScheduledExecutorService? = null

    private var status: WorkerStatusTracker? = null

    private var doneFuture: KafkaFutureImpl<String>? = null

    override fun start(
        platform: Platform,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) {
            "ConfigurableProducerWorker is already running."
        }
        log.info("{}: Activating ConfigurableProducerWorker with {}", id, spec)
        // Create an executor with 2 threads.  We need the second thread so
        // that the StatusUpdater can run in parallel with SendRecords.
        executor = Executors.newScheduledThreadPool(
            2,
            createThreadFactory("ConfigurableProducerWorkerThread%d", false)
        )
        this.status = status
        this.doneFuture = haltFuture
        executor!!.submit(Prepare())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform) {
        check(running.compareAndSet(true, false)) {
            "ConfigurableProducerWorker is not running."
        }
        log.info("{}: Deactivating ConfigurableProducerWorker.", id)
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
                if (spec.activeTopic().materialize().size != 1) {
                    throw RuntimeException("Can only run against 1 topic.")
                }
                val active = mutableListOf<TopicPartition>()
                for ((topicName, partSpec) in spec.activeTopic().materialize().entries) {
                    newTopics[topicName] = partSpec.newTopic(topicName)
                    for (partitionNumber in partSpec.partitionNumbers()) {
                        active.add(TopicPartition(topicName, partitionNumber))
                    }
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
                status!!.update(TextNode("Created ${newTopics.keys.size} topic(s)"))
                executor!!.submit(SendRecords(active[0].topic, spec.activePartition()))
            } catch (e: Throwable) {
                abort(log, "Prepare", e, doneFuture!!)
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

    inner class SendRecords internal constructor(
        private val activeTopic: String,
        private val activePartition: Int,
    ) : Callable<Unit> {

        private val histogram: Histogram = Histogram(10000)

        private val statusUpdaterFuture: Future<*> = executor!!.scheduleWithFixedDelay(
            StatusUpdater(histogram), 30, 30, TimeUnit.SECONDS
        )

        private val producer: KafkaProducer<ByteArray?, ByteArray?>

        private val keys: PayloadIterator

        private val values: PayloadIterator

        private var sendFuture: Future<RecordMetadata>? = null

        init {
            val props = Properties()
            props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = spec.bootstrapServers()
            addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf())
            producer = KafkaProducer(
                properties = props,
                keySerializer = ByteArraySerializer(),
                valueSerializer = ByteArraySerializer(),
            )
            keys = PayloadIterator(spec.keyGenerator())
            values = PayloadIterator(spec.valueGenerator())
        }

        @Throws(Exception::class)
        override fun call() {
            val startTimeMs = Time.SYSTEM.milliseconds()
            try {
                try {
                    var sentMessages = 0L
                    while (true) {
                        sendMessage()
                        sentMessages++
                    }
                } catch (exception: Exception) {
                    throw exception
                } finally {
                    if (sendFuture != null) {
                        try {
                            sendFuture!!.get()
                        } catch (e: Exception) {
                            log.error("Exception on final future", e)
                        }
                    }
                    producer.close()
                }
            } catch (exception: Exception) {
                abort(log, "SendRecords", exception, doneFuture!!)
            } finally {
                statusUpdaterFuture.cancel(false)
                val statusData: StatusData = StatusUpdater(histogram).update()
                val curTimeMs = Time.SYSTEM.milliseconds()
                log.info(
                    "Sent {} total record(s) in {} ms.  status: {}",
                    histogram.summarize().numSamples, curTimeMs - startTimeMs, statusData
                )
            }
            doneFuture!!.complete("")
        }

        @Throws(InterruptedException::class)
        private fun sendMessage() {
            val record =
                if (activePartition != -1) ProducerRecord<ByteArray?, ByteArray?>(
                    topic = activeTopic,
                    partition = activePartition,
                    key = keys.next(),
                    value = values.next(),
                ) else ProducerRecord<ByteArray?, ByteArray?>(
                    topic = activeTopic,
                    key = keys.next(),
                    value = values.next(),
                )
            sendFuture = producer.send(record, SendRecordsCallback(this, Time.SYSTEM.milliseconds()))
            spec.flushGenerator()?.increment(producer)
            spec.throughputGenerator().throttle()
        }

        fun recordDuration(durationMs: Long) = histogram.add(durationMs)
    }

    inner class StatusUpdater internal constructor(
        private val histogram: Histogram,
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
                totalSent = summary.numSamples,
                averageLatencyMs = summary.average,
                p50LatencyMs = summary.percentiles[0].value,
                p95LatencyMs = summary.percentiles[1].value,
                p99LatencyMs = summary.percentiles[2].value,
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
    ) {

        @JsonProperty
        fun totalSent(): Long = totalSent

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
        private val log = LoggerFactory.getLogger(ConfigurableProducerWorker::class.java)
    }
}
