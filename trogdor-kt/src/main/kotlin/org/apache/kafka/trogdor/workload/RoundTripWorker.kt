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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Duration
import java.util.Properties
import java.util.TreeSet
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.trogdor.common.JsonUtil
import org.apache.kafka.trogdor.common.Platform
import org.apache.kafka.trogdor.common.WorkerUtils.abort
import org.apache.kafka.trogdor.common.WorkerUtils.addConfigsToProperties
import org.apache.kafka.trogdor.common.WorkerUtils.createTopics
import org.apache.kafka.trogdor.common.WorkerUtils.perSecToPerPeriod
import org.apache.kafka.trogdor.task.TaskWorker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.slf4j.LoggerFactory

class RoundTripWorker(
    private val id: String,
    private val spec: RoundTripWorkloadSpec,
) : TaskWorker {

    private var toReceiveTracker: ToReceiveTracker? = null

    private val running = AtomicBoolean(false)

    private val lock: Lock = ReentrantLock()

    private val unackedSendsAreZero = lock.newCondition()

    private var executor: ScheduledExecutorService? = null

    private var status: WorkerStatusTracker? = null

    private var doneFuture: KafkaFutureImpl<String>? = null

    private var producer: KafkaProducer<ByteArray?, ByteArray?>? = null

    private var consumer: KafkaConsumer<ByteArray?, ByteArray?>? = null

    private var unackedSends: Long? = null

    private var toSendTracker: ToSendTracker? = null

    @Throws(Exception::class)
    override fun start(
        platform: Platform,
        status: WorkerStatusTracker,
        haltFuture: KafkaFutureImpl<String>,
    ) {
        check(running.compareAndSet(false, true)) { "RoundTripWorker is already running." }
        log.info("{}: Activating RoundTripWorker.", id)
        executor = Executors.newScheduledThreadPool(
            3,
            createThreadFactory("RoundTripWorker%d", false)
        )
        this.status = status
        this.doneFuture = haltFuture
        producer = null
        consumer = null
        unackedSends = spec.maxMessages()
        executor!!.submit(Prepare())
    }

    @Throws(Exception::class)
    override fun stop(platform: Platform) {
        check(running.compareAndSet(true, false)) { "RoundTripWorker is not running." }
        log.info("{}: Deactivating RoundTripWorker.", id)
        doneFuture!!.complete("")
        executor!!.shutdownNow()
        executor!!.awaitTermination(1, TimeUnit.DAYS)
        closeQuietly(consumer, "consumer")
        closeQuietly(producer, "producer")
        consumer = null
        producer = null
        unackedSends = null
        executor = null
        doneFuture = null
        log.info("{}: Deactivated RoundTripWorker.", id)
    }

    internal inner class Prepare : Runnable {
        override fun run() {
            try {
                if (spec.targetMessagesPerSec() <= 0)
                    throw ConfigException("Can't have targetMessagesPerSec <= 0.")

                val newTopics = mutableMapOf<String, NewTopic>()
                val active = mutableSetOf<TopicPartition>()
                for ((topicName, partSpec) in spec.activeTopics().materialize()) {
                    newTopics[topicName] = partSpec.newTopic(topicName)
                    for (partitionNumber in partSpec.partitionNumbers()) {
                        active.add(TopicPartition(topicName, partitionNumber))
                    }
                }
                if (active.isEmpty()) throw RuntimeException("You must specify at least one active topic.")

                status!!.update(TextNode("Creating ${newTopics.keys.size} topic(s)"))
                createTopics(
                    log = log,
                    bootstrapServers = spec.bootstrapServers(),
                    commonClientConf = spec.commonClientConf(),
                    adminClientConf = spec.adminClientConf(),
                    topics = newTopics,
                    failOnExisting = false,
                )
                status!!.update(TextNode("Created " + newTopics.keys.size + " topic(s)"))
                toSendTracker = ToSendTracker(spec.maxMessages())
                toReceiveTracker = ToReceiveTracker()
                with (executor!!) {
                    submit(ProducerRunnable(active))
                    submit(ConsumerRunnable(active))
                    submit(StatusUpdater())
                    scheduleWithFixedDelay(StatusUpdater(), 30, 30, TimeUnit.SECONDS)
                }
            } catch (exception: Throwable) {
                abort(log, "Prepare", exception, doneFuture!!)
            }
        }
    }

    private class ToSendTrackerResult (val index: Long, val firstSend: Boolean)

    private class ToSendTracker(private val maxMessages: Long) {

        private val failed = mutableListOf<Long>()

        private var frontier: Long = 0

        @Synchronized
        fun addFailed(index: Long) {
            failed.add(index)
        }

        @Synchronized
        fun frontier(): Long = frontier

        @Synchronized
        operator fun next(): ToSendTrackerResult? {
            return if (failed.isEmpty()) {
                if (frontier >= maxMessages) null
                else ToSendTrackerResult(frontier++, true)
            } else ToSendTrackerResult(failed.removeAt(0), false)
        }
    }

    internal inner class ProducerRunnable(
        private val partitions: MutableSet<TopicPartition>,
    ) : Runnable {

        private val throttle: Throttle

        init {
            val props = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers())
                put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024)
                put(ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 16 * 1024L)
                put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000L)
                put(ProducerConfig.CLIENT_ID_CONFIG, "producer.$id")
                put(ProducerConfig.ACKS_CONFIG, "all")
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000)
            }
            // user may over-write the defaults with common client config and producer config
            addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf())
            producer = KafkaProducer(
                properties = props,
                keySerializer = ByteArraySerializer(),
                valueSerializer = ByteArraySerializer(),
            )
            val perPeriod = perSecToPerPeriod(
                perSec = spec.targetMessagesPerSec().toFloat(),
                periodMs = THROTTLE_PERIOD_MS.toLong(),
            )
            throttle = Throttle(perPeriod, THROTTLE_PERIOD_MS)
        }

        override fun run() {
            var messagesSent = 0L
            var uniqueMessagesSent = 0L
            log.debug("{}: Starting RoundTripWorker#ProducerRunnable.", id)
            try {
                var iter: Iterator<TopicPartition> = partitions.iterator()
                while (true) {
                    val result = toSendTracker!!.next() ?: break
                    throttle.increment()
                    val messageIndex = result.index
                    if (result.firstSend) {
                        toReceiveTracker!!.addPending(messageIndex)
                        uniqueMessagesSent++
                    }
                    messagesSent++
                    if (!iter.hasNext()) {
                        iter = partitions.iterator()
                    }
                    val (topic, partition1) = iter.next()
                    // we explicitly specify generator position based on message index
                    val record = ProducerRecord<ByteArray?, ByteArray?>(
                        topic = topic,
                        partition = partition1,
                        key = KEY_GENERATOR.generate(messageIndex),
                        value = spec.valueGenerator().generate(messageIndex),
                    )
                    producer!!.send(record) { _, exception ->
                        if (exception == null) {
                            lock.lock()
                            try {
                                unackedSends = unackedSends!! - 1
                                if (unackedSends!! <= 0) unackedSendsAreZero.signalAll()
                            } finally {
                                lock.unlock()
                            }
                        } else {
                            log.info(
                                "{}: Got exception when sending message {}: {}",
                                id, messageIndex, exception.message
                            )
                            toSendTracker!!.addFailed(messageIndex)
                        }
                    }
                }
            } catch (exception: Throwable) {
                abort(log, "ProducerRunnable", exception, doneFuture!!)
            } finally {
                lock.lock()
                try {
                    log.info(
                        "{}: ProducerRunnable is exiting.  messagesSent={}; uniqueMessagesSent={}; " +
                                "ackedSends={}/{}.", id, messagesSent, uniqueMessagesSent,
                        spec.maxMessages() - unackedSends!!, spec.maxMessages()
                    )
                } finally {
                    lock.unlock()
                }
            }
        }
    }

    private inner class ToReceiveTracker {

        private val pending = TreeSet<Long>()

        private var totalReceived: Long = 0

        @Synchronized
        fun addPending(messageIndex: Long) {
            pending.add(messageIndex)
        }

        @Synchronized
        fun removePending(messageIndex: Long): Boolean {
            return if (pending.remove(messageIndex)) {
                totalReceived++
                true
            } else false
        }

        @Synchronized
        fun totalReceived(): Long = totalReceived

        fun log() {
            var numToReceive: Long
            val list: MutableList<Long> = ArrayList(LOG_NUM_MESSAGES)
            synchronized(this) {
                numToReceive = pending.size.toLong()
                val iter: Iterator<Long> = pending.iterator()
                while (iter.hasNext() && list.size < LOG_NUM_MESSAGES) {
                    val i = iter.next()
                    list.add(i)
                }
            }
            log.info(
                "{}: consumer waiting for {} message(s), starting with: {}",
                id, numToReceive, list.joinToString()
            )
        }
    }

    internal inner class ConsumerRunnable(partitions: Set<TopicPartition>) : Runnable {

        private val props: Properties = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers())
            put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.$id")
            put(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-consumer-group-$id")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000)
        }

        init {
            // user may over-write the defaults with common client config and consumer config
            addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf())
            consumer = KafkaConsumer(
                properties = props,
                keyDeserializer = ByteArrayDeserializer(),
                valueDeserializer = ByteArrayDeserializer(),
            )
            consumer!!.assign(partitions)
        }

        override fun run() {
            var uniqueMessagesReceived: Long = 0
            var messagesReceived: Long = 0
            var pollInvoked: Long = 0
            log.debug("{}: Starting RoundTripWorker#ConsumerRunnable.", id)
            try {
                var lastLogTimeMs = Time.SYSTEM.milliseconds()
                while (true) {
                    try {
                        pollInvoked++
                        val records = consumer!!.poll(Duration.ofMillis(50))
                        val iter = records.iterator()
                        while (iter.hasNext()) {
                            val record = iter.next()
                            val messageIndex = ByteBuffer.wrap(record.key)
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .getInt()
                                .toLong()
                            messagesReceived++
                            if (toReceiveTracker!!.removePending(messageIndex)) {
                                uniqueMessagesReceived++
                                if (uniqueMessagesReceived >= spec.maxMessages()) {
                                    lock.lock()
                                    try {
                                        log.info(
                                            "{}: Consumer received the full count of {} unique messages.  " +
                                                    "Waiting for all {} sends to be acked...",
                                            id,
                                            spec.maxMessages(),
                                            unackedSends
                                        )
                                        while (unackedSends!! > 0) unackedSendsAreZero.await()
                                    } finally {
                                        lock.unlock()
                                    }
                                    log.info("{}: all sends have been acked.", id)
                                    StatusUpdater().update()
                                    doneFuture!!.complete("")
                                    return
                                }
                            }
                        }
                        val curTimeMs = Time.SYSTEM.milliseconds()
                        if (curTimeMs > lastLogTimeMs + LOG_INTERVAL_MS) {
                            toReceiveTracker!!.log()
                            lastLogTimeMs = curTimeMs
                        }
                    } catch (e: WakeupException) {
                        log.debug("{}: Consumer got WakeupException", id, e)
                    } catch (e: TimeoutException) {
                        log.debug("{}: Consumer got TimeoutException", id, e)
                    }
                }
            } catch (exception: Throwable) {
                abort(log, "ConsumerRunnable", exception, doneFuture!!)
            } finally {
                log.info(
                    "{}: ConsumerRunnable is exiting.  Invoked poll {} time(s).  " +
                            "messagesReceived = {}; uniqueMessagesReceived = {}.",
                    id, pollInvoked, messagesReceived, uniqueMessagesReceived
                )
            }
        }
    }

    inner class StatusUpdater : Runnable {
        override fun run() {
            try {
                update()
            } catch (exception: Exception) {
                abort(log, "StatusUpdater", exception, doneFuture!!)
            }
        }

        fun update(): StatusData {
            val statusData = StatusData(toSendTracker!!.frontier(), toReceiveTracker!!.totalReceived())
            status!!.update(JsonUtil.JSON_SERDE.valueToTree(statusData))
            return statusData
        }
    }

    class StatusData @JsonCreator constructor(
        @param:JsonProperty("totalUniqueSent") private val totalUniqueSent: Long,
        @param:JsonProperty("totalReceived") private val totalReceived: Long,
    ) {
        @JsonProperty
        fun totalUniqueSent(): Long = totalUniqueSent

        @JsonProperty
        fun totalReceived(): Long = totalReceived
    }

    companion object {

        private const val THROTTLE_PERIOD_MS = 100

        private const val LOG_INTERVAL_MS = 5000

        private const val LOG_NUM_MESSAGES = 10

        private val log = LoggerFactory.getLogger(RoundTripWorker::class.java)

        private val KEY_GENERATOR: PayloadGenerator = SequentialPayloadGenerator(4, 0)
    }
}
