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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.NetworkClientUtils.awaitReady
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.clients.producer.internals.TransactionManager.TxnRequestHandler
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.InvalidMetadataException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TransactionAbortedException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection
import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.slf4j.Logger
import java.io.IOException
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors
import kotlin.math.min

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This
 * thread makes metadata requests to renew its view of the cluster and then sends produce requests
 * to the appropriate nodes.
 *
 * @property client The state of each nodes' connection
 * @property metadata The metadata for the client
 * @property accumulator The record accumulator that batches records
 * @property guaranteeMessageOrder The flag indicating whether the producer should guarantee the
 * message order on the broker or not.
 * @property maxRequestSize The maximum request size to attempt to send to the server
 * @property acks The number of acknowledgements to request from the server
 * @property retries The number of times to retry a failed request before giving up
 * @property time The clock instance used for getting the time
 * @property requestTimeoutMs The max time to wait for the server to respond to the request
 * @property retryBackoffMs The max time to wait before retrying a request which has failed
 * @property transactionManager All the state related to transactions, in particular the producer
 * id, producer epoch, and sequence numbers
 * @property apiVersions Current request API versions supported by the known brokers
 */
class Sender(
    logContext: LogContext,
    private val client: KafkaClient,
    private val metadata: ProducerMetadata,
    private val accumulator: RecordAccumulator,
    private val guaranteeMessageOrder: Boolean,
    private val maxRequestSize: Int,
    private val acks: Short,
    private val retries: Int,
    metricsRegistry: SenderMetricsRegistry,
    private val time: Time,
    private val requestTimeoutMs: Int,
    private val retryBackoffMs: Long,
    private val transactionManager: TransactionManager?,
    private val apiVersions: ApiVersions,
) : Runnable {

    private val log: Logger = logContext.logger(Sender::class.java)

    /**
     * `true` while the sender thread is still running
     */
    @Volatile
    var isRunning = true
        private set

    /**
     * `true` when the caller wants to ignore all unsent/inflight messages and force close.
     */
    @Volatile
    private var forceClose = false

    /**
     * Metrics
     */
    private val sensors: SenderMetrics = SenderMetrics(metricsRegistry, metadata, client, time)

    /**
     * A per-partition queue of batches ordered by creation time for tracking the in-flight batches
     */
    private val inFlightBatches = mutableMapOf<TopicPartition, MutableList<ProducerBatch>?>()

    fun inFlightBatches(tp: TopicPartition): List<ProducerBatch?> =
        inFlightBatches[tp] ?: emptyList()

    private fun maybeRemoveFromInflightBatches(batch: ProducerBatch) {
        val batches = inFlightBatches[batch.topicPartition]
        if (batches != null) {
            batches.remove(batch)
            if (batches.isEmpty()) inFlightBatches.remove(batch.topicPartition)
        }
    }

    private fun maybeRemoveAndDeallocateBatch(batch: ProducerBatch) {
        maybeRemoveFromInflightBatches(batch)
        accumulator.deallocate(batch)
    }

    /**
     * Get the in-flight batches that has reached delivery timeout.
     */
    private fun getExpiredInflightBatches(now: Long): List<ProducerBatch> {
        val expiredBatches: MutableList<ProducerBatch> = ArrayList()
        val batchIt = inFlightBatches.entries.iterator()
        while (batchIt.hasNext()) {
            val entry = batchIt.next()
            val partitionInFlightBatches = entry.value ?: continue

            val iter = partitionInFlightBatches.iterator()
            while (iter.hasNext()) {
                val batch = iter.next()
                if (batch.hasReachedDeliveryTimeout(accumulator.deliveryTimeoutMs.toLong(), now)) {
                    iter.remove()
                    // expireBatches is called in Sender.sendProducerData, before client.poll.
                    // The !batch.isDone() invariant should always hold. An IllegalStateException
                    // exception will be thrown if the invariant is violated.
                    if (!batch.isDone) expiredBatches.add(batch)
                    else error(
                        batch.topicPartition.toString() + " batch created at ${batch.createdMs} " +
                                "gets unexpected final state ${batch.finalState()}"
                    )
                } else {
                    accumulator.maybeUpdateNextBatchExpiryTime(batch)
                    break
                }
            }
            if (partitionInFlightBatches.isEmpty()) batchIt.remove()
        }
        return expiredBatches
    }

    private fun addToInflightBatches(batches: List<ProducerBatch>) {
        for (batch in batches) inFlightBatches
            .computeIfAbsent(batch.topicPartition) { mutableListOf() }
            ?.add(batch)
    }

    fun addToInflightBatches(batches: Map<Int, List<ProducerBatch>>) {
        for (batchList in batches.values) addToInflightBatches(batchList)
    }

    private fun hasPendingTransactionalRequests(): Boolean =
        transactionManager != null
                && transactionManager.hasPendingRequests
                && transactionManager.hasOngoingTransaction

    /**
     * The main run loop for the sender thread
     */
    override fun run() {
        log.debug("Starting Kafka producer I/O thread.")

        // main loop, runs until close is called
        while (isRunning) {
            try {
                runOnce()
            } catch (e: Exception) {
                log.error("Uncaught error in kafka producer I/O thread: ", e)
            }
        }
        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.")

        // okay we stopped accepting requests but there may still be requests in the transaction
        // manager, accumulator or waiting for acknowledgment, wait until these are completed.
        while (
            !forceClose
            && ((accumulator.hasUndrained() || client.inFlightRequestCount() > 0)
                    || hasPendingTransactionalRequests())
        ) try {
            runOnce()
        } catch (e: Exception) {
            log.error("Uncaught error in kafka producer I/O thread: ", e)
        }

        // Abort the transaction if any commit or abort didn't go through the transaction manager's queue
        while (
            !forceClose
            && transactionManager != null
            && transactionManager.hasOngoingTransaction
        ) {
            if (!transactionManager.isCompleting) {
                log.info("Aborting incomplete transaction due to shutdown")
                transactionManager.beginAbort()
            }
            try {
                runOnce()
            } catch (e: Exception) {
                log.error("Uncaught error in kafka producer I/O thread: ", e)
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete transactional requests and batches and wake up the
            // threads waiting on the futures.
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown")
                transactionManager.close()
            }
            log.debug("Aborting incomplete batches due to forced shutdown")
            accumulator.abortIncompleteBatches()
        }
        try {
            client.close()
        } catch (e: Exception) {
            log.error("Failed to close network client", e)
        }
        log.debug("Shutdown of Kafka producer I/O thread has completed.")
    }

    /**
     * Run a single iteration of sending
     */
    fun runOnce() {
        if (transactionManager != null) try {
            transactionManager.maybeResolveSequences()

            // do not continue sending if the transaction manager is in a failed state
            if (transactionManager.hasFatalError) {
                val lastError = transactionManager.lastError()
                lastError?.let { maybeAbortBatches(it) }
                client.poll(retryBackoffMs, time.milliseconds())
                return
            }

            // Check whether we need a new producerId. If so, we will enqueue an InitProducerId
            // request which will be sent below
            transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()
            if (maybeSendAndPollTransactionalRequest()) return
        } catch (e: AuthenticationException) {
            // This is already logged as error, but propagated here to perform any cleanups.
            log.trace("Authentication exception while processing transactional request", e)
            transactionManager.authenticationFailed(e)
        }
        val currentTimeMs = time.milliseconds()
        val pollTimeout = sendProducerData(currentTimeMs)
        client.poll(pollTimeout, currentTimeMs)
    }

    private fun sendProducerData(now: Long): Long {
        val cluster = metadata.fetch()
        // get the list of partitions with data ready to send
        val result = accumulator.ready(cluster, now)

        // if there are any partitions whose leaders are not known yet, force metadata update
        if (result.unknownLeaderTopics.isNotEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as
            // well as topics which may have expired. Add the topic again to metadata to ensure it
            // is included and request metadata update, since there are messages to send to the
            // topic.
            for (topic in result.unknownLeaderTopics) metadata.add(topic, now)
            log.debug(
                "Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics,
            )
            metadata.requestUpdate()
        }

        // remove any nodes we aren't ready to send to
        val iter = result.readyNodes.toMutableSet().iterator()
        var notReadyTimeout = Long.MAX_VALUE
        while (iter.hasNext()) {
            val node = iter.next()
            if (!client.ready(node, now)) {
                // Update just the readyTimeMs of the latency stats, so that it moves forward every
                // time the batch is ready (then the difference between readyTimeMs and drainTimeMs
                // would represent how long data is waiting for the node).
                accumulator.updateNodeLatencyStats(node.id, now, false)
                iter.remove()
                notReadyTimeout = min(notReadyTimeout, client.pollDelayMs(node, now))
            } else {
                // Update both readyTimeMs and drainTimeMs, this would "reset" the node latency.
                accumulator.updateNodeLatencyStats(node.id, now, true)
            }
        }

        // create produce requests
        val batches = accumulator.drain(cluster, result.readyNodes, maxRequestSize, now)
        addToInflightBatches(batches)
        if (guaranteeMessageOrder)
        // Mute all the partitions drained
            for (batchList in batches.values)
                for (batch in batchList)
                    accumulator.mutePartition(batch.topicPartition)

        accumulator.resetNextBatchExpiryTime()
        val expiredInflightBatches = getExpiredInflightBatches(now)
        val expiredBatches = accumulator.expiredBatches(now) + expiredInflightBatches

        // Reset the producer id if an expired batch has previously been sent to the broker. Also
        // update the metrics for expired batches. see the documentation of
        // @TransactionState.resetIdempotentProducerId to understand why we need to reset the
        // producer id here.
        if (expiredBatches.isNotEmpty()) log.trace(
            "Expired {} batches in accumulator",
            expiredBatches.size
        )
        for (expiredBatch in expiredBatches) {
            val errorMessage = "Expiring ${expiredBatch.recordCount} record(s) for " +
                    "${expiredBatch.topicPartition}:${now - expiredBatch.createdMs} ms has " +
                    "passed since batch creation"

            failBatch(
                batch = expiredBatch,
                topLevelException = TimeoutException(errorMessage),
                adjustSequenceNumbers = false,
            )
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches
                // are fully resolved.
                transactionManager.markSequenceUnresolved(expiredBatch)
            }
        }
        sensors.updateProduceRequestMetrics(batches)

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so
        // this can immediately loop and try sending more data. Otherwise, the timeout will be the
        // smaller value between next batch expiry time, and the delay time for checking data
        // availability. Note that the nodes may have data that isn't yet sendable due to lingering,
        // backing off, etc. This specifically does not include nodes with sendable data that aren't
        // ready to send since they would cause busy looping.
        var pollTimeout = min(result.nextReadyCheckDelayMs, notReadyTimeout)
        pollTimeout = min(pollTimeout, accumulator.nextExpiryTimeMs() - now)
        pollTimeout = pollTimeout.coerceAtLeast(0)
        if (result.readyNodes.isNotEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes)
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata
            // expiry time;
            pollTimeout = 0
        }
        sendProduceRequests(batches, now)
        return pollTimeout
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is
     * enqueued
     */
    private fun maybeSendAndPollTransactionalRequest(): Boolean {
        if (transactionManager!!.hasInFlightRequest) {
            // as long as there are outstanding transactional requests, we simply wait for them to
            // return
            client.poll(retryBackoffMs, time.milliseconds())
            return true
        }
        if (transactionManager.hasAbortableError || transactionManager.isAborting) {
            if (accumulator.hasIncomplete()) {
                // Attempt to get the last error that caused this abort. If there was no error, but
                // we are still aborting, then this is most likely a case where there was no fatal
                // error.
                val exception = transactionManager.lastError() ?: TransactionAbortedException()
                accumulator.abortUndrainedBatches(exception)
            }
        }
        val nextRequestHandler =
            transactionManager.nextRequest(accumulator.hasIncomplete()) ?: return false
        val requestBuilder = nextRequestHandler.requestBuilder()
        var targetNode: Node? = null

        try {
            val coordinatorType = nextRequestHandler.coordinatorType()
            targetNode =
                if (coordinatorType != null) transactionManager.coordinator(coordinatorType)
                else client.leastLoadedNode(time.milliseconds())

            if (targetNode != null) {
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace(
                        "Target node {} not ready within request timeout, will retry when node " +
                                "is ready.",
                        targetNode
                    )
                    maybeFindCoordinatorAndRetry(nextRequestHandler)
                    return true
                }
            } else if (coordinatorType != null) {
                log.trace(
                    "Coordinator not known for {}, will retry {} after finding coordinator.",
                    coordinatorType,
                    requestBuilder.apiKey
                )
                maybeFindCoordinatorAndRetry(nextRequestHandler)
                return true
            } else {
                log.trace(
                    "No nodes available to send requests, will poll and retry when until a node " +
                            "is ready."
                )
                transactionManager.retry(nextRequestHandler)
                client.poll(retryBackoffMs, time.milliseconds())
                return true
            }
            if (nextRequestHandler.isRetry) time.sleep(nextRequestHandler.retryBackoffMs())
            val currentTimeMs = time.milliseconds()
            val clientRequest = client.newClientRequest(
                nodeId = targetNode.idString(),
                requestBuilder = requestBuilder,
                createdTimeMs = currentTimeMs,
                expectResponse = true,
                requestTimeoutMs = requestTimeoutMs,
                callback = nextRequestHandler,
            )
            log.debug(
                "Sending transactional request {} to node {} with correlation ID {}",
                requestBuilder,
                targetNode,
                clientRequest.correlationId,
            )
            client.send((clientRequest), currentTimeMs)
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId)
            client.poll(retryBackoffMs, time.milliseconds())
            return true
        } catch (e: IOException) {
            log.debug(
                "Disconnect from {} while trying to send request {}. Going to back off and retry.",
                targetNode,
                requestBuilder,
                e,
            )
            // We break here so that we pick up the FindCoordinator request immediately.
            maybeFindCoordinatorAndRetry(nextRequestHandler)
            return true
        }
    }

    private fun maybeFindCoordinatorAndRetry(nextRequestHandler: TxnRequestHandler) {
        if (nextRequestHandler.needsCoordinator)
            transactionManager!!.lookupCoordinator(nextRequestHandler)
        else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is
            // available
            time.sleep(retryBackoffMs)
            metadata.requestUpdate()
        }
        transactionManager!!.retry(nextRequestHandler)
    }

    private fun maybeAbortBatches(exception: RuntimeException) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception)
            accumulator.abortBatches(exception)
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    fun initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        accumulator.close()
        isRunning = false
        wakeup()
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    fun forceClose() {
        forceClose = true
        initiateClose()
    }

    @Throws(IOException::class)
    private fun awaitNodeReady(node: Node, coordinatorType: CoordinatorType?): Boolean {
        if (awaitReady(client, node, time, requestTimeoutMs.toLong())) {
            if (coordinatorType === CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to
                // check ApiVersions This allows us to bump transactional epochs even if the
                // coordinator is temporarily unavailable at the time when the abortable error is
                // handled
                transactionManager!!.handleCoordinatorReady()
            }
            return true
        }
        return false
    }

    /**
     * Handle a produce response
     */
    private fun handleProduceResponse(
        response: ClientResponse,
        batches: Map<TopicPartition, ProducerBatch>,
        now: Long,
    ) {
        val requestHeader = response.requestHeader
        val correlationId = requestHeader.correlationId
        if (response.disconnected) {
            log.trace(
                "Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination
            )
            for (batch in batches.values) completeBatch(
                batch, ProduceResponse.PartitionResponse(
                    error = Errors.NETWORK_EXCEPTION,
                    errorMessage = "Disconnected from node ${response.destination}",
                ),
                correlationId.toLong(), now
            )
        } else if (response.versionMismatch != null) {
            log.warn(
                "Cancelled request {} due to a version mismatch with node {}",
                response,
                response.destination,
                response.versionMismatch,
            )
            for (batch in batches.values) completeBatch(
                batch = batch,
                response = ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION),
                correlationId = correlationId.toLong(),
                now = now,
            )
        } else {
            log.trace(
                "Received produce response from node {} with correlation id {}",
                response.destination,
                correlationId
            )
            // if we have a response, parse it
            if (response.hasResponse) {
                // Sender should exercise PartitionProduceResponse rather than
                // ProduceResponse.PartitionResponse
                // https://issues.apache.org/jira/browse/KAFKA-10696
                val produceResponse = response.responseBody as ProduceResponse
                produceResponse.data().responses.forEach { r ->
                    r.partitionResponses.forEach { p ->
                        val tp = TopicPartition(r.name, p.index)
                        val partResp = ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode),
                            p.baseOffset,
                            p.logAppendTimeMs,
                            p.logStartOffset,
                            p.recordErrors.map { e ->
                                RecordError(
                                    batchIndex = e.batchIndex,
                                    message = e.batchIndexErrorMessage,
                                )
                            },
                            p.errorMessage,
                        )
                        val batch = batches[tp]!!
                        completeBatch(
                            batch = batch,
                            response = partResp,
                            correlationId = correlationId.toLong(),
                            now = now,
                        )
                    }
                }
                sensors.recordLatency(response.destination, response.requestLatencyMs)
            } else {
                // this is the acks = 0 case, just complete all requests
                for (batch in batches.values) {
                    completeBatch(
                        batch = batch,
                        response = ProduceResponse.PartitionResponse(Errors.NONE),
                        correlationId = correlationId.toLong(),
                        now = now,
                    )
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     */
    private fun completeBatch(
        batch: ProducerBatch,
        response: ProduceResponse.PartitionResponse,
        correlationId: Long,
        now: Long,
    ) {
        val error = response.error
        if (
            error === Errors.MESSAGE_TOO_LARGE
            && batch.recordCount > 1
            && !batch.isDone
            && (batch.magic >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed)
        ) {
            // If the batch is too large, we split the batch and send the split batches again. We do
            // not decrement the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, " +
                        "splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                retries - batch.attempts(),
                formatErrMsg(response),
            )
            transactionManager?.removeInFlightBatch(batch)
            accumulator.splitAndReenqueue(batch)
            maybeRemoveAndDeallocateBatch(batch)
            sensors.recordBatchSplit()
        } else if (error !== Errors.NONE) {
            if (canRetry(batch, response, now)) {
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, " +
                            "retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    retries - batch.attempts() - 1,
                    formatErrMsg(response)
                )
                reenqueueBatch(batch, now)
            } else if (error === Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number
                // has advanced beyond the sequence of the current batch, and we haven't retained
                // batch metadata on the broker to return the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid
                // offset and timestamp.
                completeBatch(batch, response)
            } else {
                // tell the user the result of their request. We only adjust sequence numbers if the
                // batch didn't exhaust its retries -- if it did, we don't know whether the sequence
                // number was accepted or not, and thus it is not safe to reassign the sequence.
                failBatch(batch, response, batch.attempts() < retries)
            }
            if (error.exception is InvalidMetadataException) {
                if (error.exception is UnknownTopicOrPartitionException) log.warn(
                    "Received unknown topic or partition error in produce request on " +
                            "partition {}. The topic-partition may not exist or the user may " +
                            "not have Describe access to it",
                    batch.topicPartition,
                )
                else log.warn(
                    "Received invalid metadata error in produce request on partition {} due " +
                            "to {}. Going to request metadata update now",
                    batch.topicPartition,
                    error.exception(response.errorMessage).toString(),
                )
                metadata.requestUpdate()
            }
        } else completeBatch(batch, response)

        // Unmute the completed partition.
        if (guaranteeMessageOrder) accumulator.unmutePartition(batch.topicPartition)
    }

    /**
     * Format the error from a [ProduceResponse.PartitionResponse] in a user-friendly string
     * e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    private fun formatErrMsg(response: ProduceResponse.PartitionResponse): String {
        val errorMessageSuffix =
            if (response.errorMessage.isNullOrEmpty()) ""
            else ". Error Message: ${response.errorMessage}"
        return "${response.error}$errorMessageSuffix"
    }

    private fun reenqueueBatch(batch: ProducerBatch, currentTimeMs: Long) {
        accumulator.reenqueue(batch, currentTimeMs)
        maybeRemoveFromInflightBatches(batch)
        sensors.recordRetries(batch.topicPartition.topic, batch.recordCount)
    }

    private fun completeBatch(batch: ProducerBatch, response: ProduceResponse.PartitionResponse) {
        transactionManager?.handleCompletedBatch(batch, response)
        if (batch.complete(response.baseOffset, response.logAppendTime))
            maybeRemoveAndDeallocateBatch(batch)
    }

    private fun failBatch(
        batch: ProducerBatch,
        response: ProduceResponse.PartitionResponse,
        adjustSequenceNumbers: Boolean,
    ) {
        val topLevelException: RuntimeException = when (response.error) {
            Errors.TOPIC_AUTHORIZATION_FAILED ->
                TopicAuthorizationException(setOf(batch.topicPartition.topic))

            Errors.CLUSTER_AUTHORIZATION_FAILED -> ClusterAuthorizationException(
                "The producer is not authorized to do idempotent sends"
            )

            else -> response.error.exception(response.errorMessage)
        }

        if (response.recordErrors.isEmpty()) failBatch(
            batch = batch,
            topLevelException = topLevelException,
            adjustSequenceNumbers = adjustSequenceNumbers,
        )
        else {
            val recordErrorMap = response.recordErrors.associate { recordError ->
                // The API leaves us with some awkwardness interpreting the errors in the response.
                // We cannot differentiate between different error cases (such as INVALID_TIMESTAMP)
                // from the single error code at the partition level, so instead we use INVALID_RECORD
                // for all failed records and rely on the message to distinguish the cases.
                val errorMessage: String =
                    recordError.message ?: response.errorMessage ?: response.error.message

                // If the batch contained only a single record error, then we can unambiguously
                // use the exception type corresponding to the partition-level error code.
                recordError.batchIndex to
                        if (response.recordErrors.size == 1) response.error.exception(errorMessage)
                        else InvalidRecordException(errorMessage)
            }
            val recordExceptions: (Int) -> RuntimeException = { batchIndex: Int ->
                // If the response contains record errors, then the records which failed validation
                // will be present in the response. To avoid confusion for the remaining records, we
                // return a generic exception.
                recordErrorMap[batchIndex] ?: KafkaException(
                    "Failed to append record because it was part of a batch which had " +
                            "one more more invalid records"
                )
            }
            failBatch(
                batch = batch,
                topLevelException = topLevelException,
                recordExceptions = recordExceptions,
                adjustSequenceNumbers = adjustSequenceNumbers,
            )
        }
    }

    private fun failBatch(
        batch: ProducerBatch,
        topLevelException: RuntimeException,
        recordExceptions: (Int) -> RuntimeException = { topLevelException },
        adjustSequenceNumbers: Boolean,
    ) {
        transactionManager?.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers)
        sensors.recordErrors(batch.topicPartition.topic, batch.recordCount)
        if (batch.completeExceptionally(topLevelException, recordExceptions))
            maybeRemoveAndDeallocateBatch(batch)
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than
     * the maximum allowed. We can also retry OutOfOrderSequence exceptions for future batches,
     * since if the first batch has failed, the future batches are certain to fail with an
     * OutOfOrderSequence exception.
     */
    private fun canRetry(
        batch: ProducerBatch,
        response: ProduceResponse.PartitionResponse,
        now: Long,
    ): Boolean =
        (!batch.hasReachedDeliveryTimeout(accumulator.deliveryTimeoutMs.toLong(), now)
                && batch.attempts() < retries
                && !batch.isDone
                && ((transactionManager?.canRetry(response, batch)
            ?: response.error.exception) is RetriableException))

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private fun sendProduceRequests(collated: Map<Int, List<ProducerBatch>>, now: Long) {
        for ((destination, batches) in collated) sendProduceRequest(
            now = now,
            destination = destination,
            acks = acks,
            timeout = requestTimeoutMs,
            batches = batches,
        )
    }

    /**
     * Create a produce request from the given record batches
     */
    private fun sendProduceRequest(
        now: Long,
        destination: Int,
        acks: Short,
        timeout: Int,
        batches: List<ProducerBatch>,
    ) {
        if (batches.isEmpty()) return
        val recordsByPartition: MutableMap<TopicPartition, ProducerBatch> = HashMap(batches.size)

        // find the minimum magic version used when creating the record sets
        var minUsedMagic = apiVersions.maxUsableProduceMagic()
        for (batch in batches) if (batch.magic < minUsedMagic) minUsedMagic = batch.magic

        val tpd = TopicProduceDataCollection()
        for (batch in batches) {
            val tp = batch.topicPartition
            var records = batch.records()

            // down convert if necessary to the minimum magic used. In general, there can be a delay
            // between the time that the producer starts building the batch and the time that we
            // send the request, and we may have chosen the message format based on out-dated
            // metadata. In the worst case, we optimistically chose to use the new message format,
            // but found that the broker didn't support it, so we need to down-convert on the client
            // before sending. This is intended to handle edge cases around cluster upgrades where
            // brokers may not all support the same message format version. For example, if a
            // partition migrates from a broker which is supporting the new magic version to one
            // which doesn't, then we will need to convert.
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(
                    toMagic = minUsedMagic,
                    firstOffset = 0,
                    time = time,
                ).records

            var tpData = tpd.find(tp.topic)
            if (tpData == null) {
                tpData = TopicProduceData().setName(tp.topic)
                tpd.add(tpData)
            }
            tpData.partitionData += PartitionProduceData()
                .setIndex(tp.partition)
                .setRecords(records)

            recordsByPartition[tp] = batch
        }
        var transactionalId: String? = null
        if (transactionManager != null && transactionManager.isTransactional)
            transactionalId = transactionManager.transactionalId

        val requestBuilder = ProduceRequest.forMagic(
            minUsedMagic,
            ProduceRequestData()
                .setAcks(acks)
                .setTimeoutMs(timeout)
                .setTransactionalId(transactionalId)
                .setTopicData(tpd)
        )
        val callback = RequestCompletionHandler { response ->
            handleProduceResponse(
                response,
                recordsByPartition,
                time.milliseconds()
            )
        }
        val nodeId = destination.toString()
        val clientRequest = client.newClientRequest(
            nodeId = nodeId,
            requestBuilder = requestBuilder,
            createdTimeMs = now,
            expectResponse = acks.toInt() != 0,
            requestTimeoutMs = requestTimeoutMs,
            callback = callback,
        )
        client.send(clientRequest, now)
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder)
    }

    /**
     * Wake up the selector associated with this send thread
     */
    fun wakeup() = client.wakeup()

    /**
     * A collection of sensors for the sender
     */
    private class SenderMetrics(
        private val metrics: SenderMetricsRegistry,
        metadata: Metadata,
        client: KafkaClient,
        private val time: Time,
    ) {

        val retrySensor: Sensor = metrics.sensor("record-retries").apply {
            add(Meter(metrics.recordRetryRate, metrics.recordRetryTotal))
        }

        val errorSensor: Sensor = metrics.sensor("errors").apply {
            add(Meter(metrics.recordErrorRate, metrics.recordErrorTotal))
        }

        val queueTimeSensor: Sensor = metrics.sensor("queue-time").apply {
            add(metrics.recordQueueTimeAvg, Avg())
            add(metrics.recordQueueTimeMax, Max())
        }

        val requestTimeSensor: Sensor = metrics.sensor("request-time").apply {
            add(metrics.requestLatencyAvg, Avg())
            add(metrics.requestLatencyMax, Max())
        }

        val recordsPerRequestSensor: Sensor = metrics.sensor("records-per-request").apply {
            add(Meter(metrics.recordSendRate, metrics.recordSendTotal))
            add(metrics.recordsPerRequestAvg, Avg())
        }

        val batchSizeSensor: Sensor = metrics.sensor("batch-size").apply {
            add(metrics.batchSizeAvg, Avg())
            add(metrics.batchSizeMax, Max())
        }

        val compressionRateSensor: Sensor = metrics.sensor("compression-rate").apply {
            add(metrics.compressionRateAvg, Avg())
        }

        val maxRecordSizeSensor: Sensor = metrics.sensor("record-size").apply {
            add(metrics.recordSizeMax, Max())
            add(metrics.recordSizeAvg, Avg())
        }

        val batchSplitSensor: Sensor

        init {
            metrics.addMetric(
                metrics.requestsInFlight
            ) { _, _ -> client.inFlightRequestCount().toDouble() }
            metrics.addMetric(metrics.metadataAge) { _, now ->
                (now - metadata.lastSuccessfulUpdate()) / 1000.0
            }
            batchSplitSensor = metrics.sensor("batch-split-rate")
            batchSplitSensor.add(Meter(metrics.batchSplitRate, metrics.batchSplitTotal))
        }

        private fun maybeRegisterTopicMetrics(topic: String) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            val topicRecordsCountName = "topic.$topic.records-per-batch"
            var topicRecordCount = metrics.getSensor(topicRecordsCountName)
            if (topicRecordCount == null) {
                val metricTags = mapOf("topic" to topic)
                topicRecordCount = metrics.sensor(topicRecordsCountName)

                var rateMetricName: MetricName = metrics.topicRecordSendRate(metricTags)
                var totalMetricName: MetricName = metrics.topicRecordSendTotal(metricTags)
                topicRecordCount.add(Meter(rateMetricName, totalMetricName))

                val topicByteRateName = "topic.$topic.bytes"
                val topicByteRate = metrics.sensor(topicByteRateName)
                rateMetricName = metrics.topicByteRate(metricTags)
                totalMetricName = metrics.topicByteTotal(metricTags)
                topicByteRate.add(Meter(rateMetricName, totalMetricName))

                val topicCompressionRateName = "topic.$topic.compression-rate"
                val topicCompressionRate = metrics.sensor(topicCompressionRateName)
                val m = metrics.topicCompressionRate(metricTags)
                topicCompressionRate.add(m, Avg())

                val topicRetryName = "topic.$topic.record-retries"
                val topicRetrySensor = metrics.sensor(topicRetryName)
                rateMetricName = metrics.topicRecordRetryRate(metricTags)
                totalMetricName = metrics.topicRecordRetryTotal(metricTags)
                topicRetrySensor.add(Meter(rateMetricName, totalMetricName))

                val topicErrorName = "topic.$topic.record-errors"
                val topicErrorSensor = metrics.sensor(topicErrorName)
                rateMetricName = metrics.topicRecordErrorRate(metricTags)
                totalMetricName = metrics.topicRecordErrorTotal(metricTags)
                topicErrorSensor.add(Meter(rateMetricName, totalMetricName))
            }
        }

        fun updateProduceRequestMetrics(batches: Map<Int, List<ProducerBatch>>) {
            val now = time.milliseconds()
            for (nodeBatch in batches.values) {
                var records = 0
                for (batch in nodeBatch) {
                    // register all per-topic metrics at once
                    val topic = batch.topicPartition.topic
                    maybeRegisterTopicMetrics(topic)

                    // per-topic record send rate
                    val topicRecordsCountName = "topic.$topic.records-per-batch"
                    val topicRecordCount = checkNotNull(metrics.getSensor(topicRecordsCountName))
                    topicRecordCount.record(batch.recordCount.toDouble())

                    // per-topic bytes send rate
                    val topicByteRateName = "topic.$topic.bytes"
                    val topicByteRate = checkNotNull(metrics.getSensor(topicByteRateName))
                    topicByteRate.record(batch.estimatedSizeInBytes.toDouble())

                    // per-topic compression rate
                    val topicCompressionRateName = "topic.$topic.compression-rate"
                    val topicCompressionRate =
                        checkNotNull(metrics.getSensor(topicCompressionRateName))
                    topicCompressionRate.record(batch.compressionRatio)

                    // global metrics
                    batchSizeSensor.record(batch.estimatedSizeInBytes.toDouble(), now)
                    queueTimeSensor.record(batch.queueTimeMs().toDouble(), now)
                    compressionRateSensor.record(batch.compressionRatio)
                    maxRecordSizeSensor.record(batch.maxRecordSize.toDouble(), now)
                    records += batch.recordCount
                }
                recordsPerRequestSensor.record(records.toDouble(), now)
            }
        }

        fun recordRetries(topic: String, count: Int) {
            val now = time.milliseconds()
            retrySensor.record(count.toDouble(), now)
            val topicRetryName = "topic.$topic.record-retries"
            val topicRetrySensor = metrics.getSensor(topicRetryName)
            topicRetrySensor?.record(count.toDouble(), now)
        }

        fun recordErrors(topic: String, count: Int) {
            val now = time.milliseconds()
            errorSensor.record(count.toDouble(), now)
            val topicErrorName = "topic.$topic.record-errors"
            val topicErrorSensor = metrics.getSensor(topicErrorName)
            topicErrorSensor?.record(count.toDouble(), now)
        }

        fun recordLatency(node: String, latency: Long) {
            val now = time.milliseconds()
            requestTimeSensor.record(latency.toDouble(), now)
            if (node.isNotEmpty()) {
                val nodeTimeName = "node-$node.latency"
                val nodeRequestTime = metrics.getSensor(nodeTimeName)
                nodeRequestTime?.record(latency.toDouble(), now)
            }
        }

        fun recordBatchSplit() = batchSplitSensor.record()
    }

    companion object {

        fun throttleTimeSensor(metrics: SenderMetricsRegistry): Sensor {
            val produceThrottleTimeSensor = metrics.sensor("produce-throttle-time")
            produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, Avg())
            produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, Max())
            return produceThrottleTimeSensor
        }
    }
}
