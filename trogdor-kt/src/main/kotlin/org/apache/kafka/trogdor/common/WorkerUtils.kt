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

package org.apache.kafka.trogdor.common

import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.regex.Pattern
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger
import kotlin.math.max

/**
 * Utilities for Trogdor TaskWorkers.
 */
object WorkerUtils {

    private const val ADMIN_REQUEST_TIMEOUT = 25000

    private const val CREATE_TOPICS_CALL_TIMEOUT = 180000

    private const val MAX_CREATE_TOPICS_BATCH_SIZE = 10

    /**
     * Handle an exception in a TaskWorker.
     *
     * @param log The logger to use.
     * @param what The component that had the exception.
     * @param exception The exception.
     * @param doneFuture The TaskWorker's doneFuture
     * @throws KafkaException A wrapped version of the exception.
     */
    @Throws(KafkaException::class)
    fun abort(
        log: Logger,
        what: String,
        exception: Throwable,
        doneFuture: KafkaFutureImpl<String>,
    ) {
        log.warn("{} caught an exception", what, exception)
        if (exception.message.isNullOrEmpty()) doneFuture.complete(exception.javaClass.getCanonicalName())
        else doneFuture.complete(exception.message!!)
        throw KafkaException(exception)
    }

    /**
     * Convert a rate expressed per second to a rate expressed per the given period.
     *
     * @param perSec The per-second rate.
     * @param periodMs The new period to use.
     * @return The rate per period.  This will never be less than 1.
     */
    fun perSecToPerPeriod(perSec: Float, periodMs: Long): Int {
        val period = periodMs.toFloat() / 1000f
        var perPeriod = perSec * period
        perPeriod = max(1.0, perPeriod.toDouble()).toFloat()
        return perPeriod.toInt()
    }

    /**
     * Adds all properties from commonConf and then from clientConf to given 'props' (in that order,
     * over-writing properties with the same keys).
     *
     * @param props Properties object that may contain zero or more properties
     * @param commonConf Map with common client properties
     * @param clientConf Map with client properties
     */
    fun addConfigsToProperties(
        props: Properties,
        commonConf: Map<String, String>,
        clientConf: Map<String, String>,
    ) {
        for ((key, value) in commonConf) props.setProperty(key, value)
        for ((key, value) in clientConf) props.setProperty(key, value)
    }

    /**
     * Create some Kafka topics.
     *
     * @param log The logger to use.
     * @param bootstrapServers The bootstrap server list.
     * @param commonClientConf Common client config
     * @param adminClientConf AdminClient config. This config has precedence over fields in common client config.
     * @param topics Maps topic names to partition assignments.
     * @param failOnExisting If true, the method will throw TopicExistsException if one or more topics already exist.
     * Otherwise, the existing topics are verified for number of partitions. In this case, if number of partitions
     * of an existing topic does not match the requested number of partitions, the method throws RuntimeException.
     */
    @Throws(Throwable::class)
    fun createTopics(
        log: Logger,
        bootstrapServers: String,
        commonClientConf: Map<String, String>,
        adminClientConf: Map<String, String>,
        topics: Map<String, NewTopic>,
        failOnExisting: Boolean,
    ) {
        // this method wraps the call to createTopics() that takes admin client, so that we can
        // unit test the functionality with MockAdminClient. The exception is caught and
        // re-thrown so that admin client is closed when the method returns.
        try {
            createAdminClient(
                bootstrapServers = bootstrapServers,
                commonClientConf = commonClientConf,
                adminClientConf = adminClientConf,
            ).use { adminClient ->
                createTopics(
                    log = log,
                    adminClient = adminClient,
                    topics = topics,
                    failOnExisting = failOnExisting,
                )
            }
        } catch (exception: Exception) {
            log.warn("Failed to create or verify topics {}", topics, exception)
            throw exception
        }
    }

    /**
     * The actual create topics functionality is separated into this method and called from the above method
     * to be able to unit test with mock adminClient.
     * @throws TopicExistsException if the specified topic already exists.
     * @throws UnknownTopicOrPartitionException if topic creation was issued but failed to verify if it was created.
     * @throws Throwable if creation of one or more topics fails (except for the cases above).
     */
    @Throws(Throwable::class)
    fun createTopics(
        log: Logger,
        adminClient: Admin,
        topics: Map<String, NewTopic>,
        failOnExisting: Boolean,
    ) {
        if (topics.isEmpty()) {
            log.warn("Request to create topics has an empty topic list.")
            return
        }
        val topicsExists = createTopics(log, adminClient, topics.values)
        if (!topicsExists.isEmpty()) {
            if (failOnExisting) {
                log.warn("Topic(s) {} already exist.", topicsExists)
                throw TopicExistsException("One or more topics already exist.")
            } else verifyTopics(
                log = log,
                adminClient = adminClient,
                topicsToVerify = topicsExists,
                topicsInfo = topics,
                retryCount = 3,
                retryBackoffMs = 2500,
            )
        }
    }

    /**
     * Creates Kafka topics and returns a list of topics that already exist
     * @param log The logger to use
     * @param adminClient AdminClient
     * @param topics List of topics to create
     * @return Collection of topics names that already exist.
     * @throws Throwable if creation of one or more topics fails (except for topic exists case).
     */
    @Throws(Throwable::class)
    private fun createTopics(
        log: Logger,
        adminClient: Admin,
        topics: Collection<NewTopic>,
    ): Collection<String> {
        val startMs = Time.SYSTEM.milliseconds()
        var tries = 0
        val existingTopics = mutableListOf<String>()
        val newTopics = topics.associateBy { it.name }
        val topicsToCreate = newTopics.keys.toMutableList()

        while (true) {
            log.info("Attempting to create {} topics (try {})...", topicsToCreate.size, ++tries)
            val creations = mutableMapOf<String, Future<Unit>>()
            while (topicsToCreate.isNotEmpty()) {
                val newTopicsBatch = mutableListOf<NewTopic>()
                var i = 0
                while (i < MAX_CREATE_TOPICS_BATCH_SIZE && topicsToCreate.isNotEmpty()) {
                    val topicName = topicsToCreate.removeAt(0)
                    newTopicsBatch.add(newTopics[topicName]!!)
                    i++
                }
                creations.putAll(adminClient.createTopics(newTopicsBatch).values())
            }
            // We retry cases where the topic creation failed with a
            // timeout.  This is a workaround for KAFKA-6368.
            for ((topicName, future) in creations) {
                try {
                    future.get()
                    log.debug("Successfully created {}.", topicName)
                } catch (e: Exception) {
                    when (e.cause) {
                        is TimeoutException,
                        is NotEnoughReplicasException,
                        -> {
                            log.warn(
                                "Attempt to create topic `{}` failed: {}",
                                topicName, e.cause!!.message,
                            )
                            topicsToCreate.add(topicName)
                        }

                        is TopicExistsException -> {
                            log.info("Topic {} already exists.", topicName)
                            existingTopics.add(topicName)
                        }

                        else -> {
                            log.warn("Failed to create {}", topicName, e.cause)
                            throw e.cause!!
                        }
                    }
                }
            }
            if (topicsToCreate.isEmpty()) break

            if (Time.SYSTEM.milliseconds() > startMs + CREATE_TOPICS_CALL_TIMEOUT) {
                val str = "Unable to create topic(s): ${topicsToCreate.joinToString()}after $tries attempt(s)"
                log.warn(str)
                throw TimeoutException(str)
            }
        }
        return existingTopics
    }

    /**
     * Verifies that topics in 'topicsToVerify' list have the same number of partitions as
     * described in 'topicsInfo'
     * @param log The logger to use
     * @param adminClient AdminClient
     * @param topicsToVerify List of topics to verify
     * @param topicsInfo Map of topic name to topic description, which includes topics in 'topicsToVerify' list.
     * @param retryCount The number of times to retry the fetching of the topics
     * @param retryBackoffMs The amount of time, in milliseconds, to wait in between retries
     * @throws UnknownTopicOrPartitionException If at least one topic contained in 'topicsInfo'
     * does not exist after retrying.
     * @throws RuntimeException  If one or more topics have different number of partitions than
     * described in 'topicsInfo'
     */
    @Throws(Throwable::class)
    fun verifyTopics(
        log: Logger,
        adminClient: Admin,
        topicsToVerify: Collection<String>,
        topicsInfo: Map<String, NewTopic>,
        retryCount: Int,
        retryBackoffMs: Long,
    ) {
        val topicDescriptionMap = topicDescriptions(
            topicsToVerify = topicsToVerify,
            adminClient = adminClient,
            retryCount = retryCount,
            retryBackoffMs = retryBackoffMs,
        )
        for (desc in topicDescriptionMap.values) {
            // map will always contain the topic since all topics in 'topicsExists' are in given
            // 'topics' map
            val partitions = topicsInfo[desc.name]!!.numPartitions
            if (partitions != CreateTopicsRequest.NO_NUM_PARTITIONS && desc.partitions.size != partitions) {
                val str = "Topic '${desc.name}' exists, but has ${desc.partitions.size} partitions, " +
                        "while requested  number of partitions is $partitions"
                log.warn(str)
                throw RuntimeException(str)
            }
        }
    }

    @Throws(ExecutionException::class, InterruptedException::class)
    private fun topicDescriptions(
        topicsToVerify: Collection<String>,
        adminClient: Admin,
        retryCount: Int,
        retryBackoffMs: Long,
    ): Map<String, TopicDescription> {
        lateinit var lastException: UnknownTopicOrPartitionException

        for (i in 0..<retryCount) {
            try {
                val topicsResult = adminClient.describeTopics(
                    topicNames = topicsToVerify,
                    options = DescribeTopicsOptions().apply { timeoutMs = ADMIN_REQUEST_TIMEOUT }
                )
                return topicsResult.allTopicNames().get()
            } catch (exception: ExecutionException) {
                val cause = exception.cause
                if (cause is UnknownTopicOrPartitionException) {
                    lastException = cause
                    Thread.sleep(retryBackoffMs)
                } else throw exception
            }
        }

        throw lastException
    }

    /**
     * Returns list of existing, not internal, topics/partitions that match given pattern and where partitions
     * are in range [startPartition, endPartition]
     * @param adminClient AdminClient
     * @param topicRegex Topic regular expression to match
     * @return List of topic names
     * @throws Throwable If failed to get list of existing topics
     */
    @Throws(Throwable::class)
    fun getMatchingTopicPartitions(
        adminClient: Admin,
        topicRegex: String?,
        startPartition: Int,
        endPartition: Int,
    ): Collection<TopicPartition> {
        val topicNamePattern = Pattern.compile(topicRegex)

        // first get list of matching topics
        val matchedTopics: MutableList<String> = ArrayList()
        val res = adminClient.listTopics(
            ListTopicsOptions().apply { timeoutMs = ADMIN_REQUEST_TIMEOUT }
        )
        val topicListingMap = res.namesToListings.get()
        for ((key, value) in topicListingMap) {
            if (!value.isInternal && topicNamePattern.matcher(key).matches()) matchedTopics.add(key)
        }

        // create a list of topic/partitions
        val out: MutableList<TopicPartition> = ArrayList()
        val topicsResult = adminClient.describeTopics(
            topicNames = matchedTopics,
            options = DescribeTopicsOptions().apply { timeoutMs = ADMIN_REQUEST_TIMEOUT }
        )
        val topicDescriptionMap = topicsResult.allTopicNames().get()
        for (desc in topicDescriptionMap.values) {
            val partitions = desc.partitions
            for ((partition) in partitions) {
                if (partition in startPartition..endPartition)
                    out.add(TopicPartition(desc.name, partition))
            }
        }
        return out
    }

    private fun createAdminClient(
        bootstrapServers: String,
        commonClientConf: Map<String, String>,
        adminClientConf: Map<String, String>,
    ): Admin {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = ADMIN_REQUEST_TIMEOUT
        // first add common client config, and then admin client config to properties, possibly
        // over-writing default or common properties.
        addConfigsToProperties(props, commonClientConf, adminClientConf)
        return Admin.create(props)
    }
}
