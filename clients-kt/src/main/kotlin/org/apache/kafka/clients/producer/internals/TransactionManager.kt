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
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.InvalidPidMappingException
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.errors.UnknownProducerIdException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData
import org.apache.kafka.common.message.EndTxnRequestData
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse
import org.apache.kafka.common.requests.EndTxnRequest
import org.apache.kafka.common.requests.EndTxnResponse
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.requests.InitProducerIdResponse
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.requests.TxnOffsetCommitRequest
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset
import org.apache.kafka.common.requests.TxnOffsetCommitResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.slf4j.Logger
import java.util.*
import kotlin.math.max
import kotlin.math.min

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure
 * idempotent production.
 */
class TransactionManager(
    logContext: LogContext,
    val transactionalId: String?,
    private val transactionTimeoutMs: Int,
    // This is used by the TxnRequestHandlers to control how long to back off before a given request
    // is retried. For instance, this value is lowered by the AddPartitionsToTxnHandler when it
    // receives a CONCURRENT_TRANSACTIONS error for the first AddPartitionsRequest in a transaction.
    private val retryBackoffMs: Long,
    private val apiVersions: ApiVersions,
) {

    private val log: Logger = logContext.logger(TransactionManager::class.java)

    private val txnPartitionMap: TxnPartitionMap = TxnPartitionMap()

    private val pendingTxnOffsetCommits = mutableMapOf<TopicPartition, CommittedOffset>()

    // If a batch bound for a partition expired locally after being sent at least once, the
    // partition is considered to have an unresolved state. We keep track of such partitions here,
    // and cannot assign any more sequence numbers for this partition until the unresolved state
    // gets cleared. This may happen if other inflight batches returned successfully (indicating
    // that the expired batch actually made it to the broker). If we don't get any successful
    // responses for the partition once the inflight request count falls to zero, we reset the
    // producer id and consequently clear this data structure as well. The value of the map is the
    // sequence number of the batch following the expired one, computed by adding its record count
    // to its sequence number. This is used to tell if a subsequent batch is the one immediately
    // following the expired one.
    private val partitionsWithUnresolvedSequences = mutableMapOf<TopicPartition, Int>()

    // The partitions that have received an error that triggers an epoch bump. When the epoch is
    // bumped, these partitions will have the sequences of their in-flight batches rewritten
    private val partitionsToRewriteSequences = mutableSetOf<TopicPartition>()

    private val pendingRequests: PriorityQueue<TxnRequestHandler> = PriorityQueue(
        10,
        Comparator.comparingInt { it.priority().priority }
    )

    private val newPartitionsInTransaction = mutableSetOf<TopicPartition>()

    private val pendingPartitionsInTransaction = mutableSetOf<TopicPartition>()

    private val partitionsInTransaction = mutableSetOf<TopicPartition>()

    private var pendingTransition: PendingStateTransition? = null

    private var inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID

    private var transactionCoordinator: Node? = null

    private var consumerGroupCoordinator: Node? = null

    private var coordinatorSupportsBumpingEpoch = false

    @Volatile
    private var currentState = State.UNINITIALIZED

    @Volatile
    private var lastError: RuntimeException? = null

    @Volatile
    var producerIdAndEpoch: ProducerIdAndEpoch = ProducerIdAndEpoch.NONE
        private set(value) {
            log.info(
                "ProducerId set to {} with epoch {}",
                value.producerId,
                value.epoch,
            )
            field = value
        }

    @Volatile
    private var transactionStarted = false

    @Volatile
    private var epochBumpRequired = false

    @Synchronized
    fun initializeTransactions(): TransactionalRequestResult =
        initializeTransactions(ProducerIdAndEpoch.NONE)

    @Synchronized
    fun initializeTransactions(producerIdAndEpoch: ProducerIdAndEpoch): TransactionalRequestResult {
        maybeFailWithError()
        val isEpochBump = producerIdAndEpoch !== ProducerIdAndEpoch.NONE
        return handleCachedTransactionRequestResult(
            transactionalRequestResultSupplier = {

                // If this is an epoch bump, we will transition the state as part of handling the
                // EndTxnRequest
                if (!isEpochBump) {
                    transitionTo(State.INITIALIZING)
                    log.info(
                        "Invoking InitProducerId for the first time in order to acquire a " +
                                "producer ID"
                    )
                } else log.info(
                    "Invoking InitProducerId with current producer ID and epoch {} in order to " +
                            "bump the epoch",
                    producerIdAndEpoch,
                )
                val requestData: InitProducerIdRequestData = InitProducerIdRequestData()
                    .setTransactionalId(transactionalId)
                    .setTransactionTimeoutMs(transactionTimeoutMs)
                    .setProducerId(producerIdAndEpoch.producerId)
                    .setProducerEpoch(producerIdAndEpoch.epoch)
                val handler = InitProducerIdHandler(
                    builder = InitProducerIdRequest.Builder(requestData),
                    isEpochBump = isEpochBump,
                )
                enqueueRequest(handler)
                handler.result
            },
            nextState = State.INITIALIZING,
            operation = "initTransactions",
        )
    }

    @Synchronized
    fun beginTransaction() {
        ensureTransactional()
        throwIfPendingState("beginTransaction")
        maybeFailWithError()
        transitionTo(State.IN_TRANSACTION)
    }

    @Synchronized
    fun beginCommit(): TransactionalRequestResult = handleCachedTransactionRequestResult(
        transactionalRequestResultSupplier = {
            maybeFailWithError()
            transitionTo(State.COMMITTING_TRANSACTION)
            beginCompletingTransaction(TransactionResult.COMMIT)
        },
        nextState = State.COMMITTING_TRANSACTION,
        operation = "commitTransaction",
    )

    @Synchronized
    fun beginAbort(): TransactionalRequestResult = handleCachedTransactionRequestResult(
        transactionalRequestResultSupplier = {
            if (currentState != State.ABORTABLE_ERROR) maybeFailWithError()
            transitionTo(State.ABORTING_TRANSACTION)

            // We're aborting the transaction, so there should be no need to add new partitions
            newPartitionsInTransaction.clear()
            beginCompletingTransaction(TransactionResult.ABORT)
        },
        nextState = State.ABORTING_TRANSACTION,
        operation = "abortTransaction",
    )

    private fun beginCompletingTransaction(
        transactionResult: TransactionResult,
    ): TransactionalRequestResult {
        if (newPartitionsInTransaction.isNotEmpty())
            enqueueRequest(addPartitionsToTransactionHandler())

        // If the error is an INVALID_PRODUCER_ID_MAPPING error, the server will not accept an
        // EndTxnRequest, so skip directly to InitProducerId. Otherwise, we must first abort the
        // transaction, because the producer will be fenced if we directly call InitProducerId.
        if (lastError !is InvalidPidMappingException) {
            val builder = EndTxnRequest.Builder(
                EndTxnRequestData()
                    .setTransactionalId(transactionalId)
                    .setProducerId(producerIdAndEpoch.producerId)
                    .setProducerEpoch(producerIdAndEpoch.epoch)
                    .setCommitted(transactionResult.id)
            )
            val handler = EndTxnHandler(builder)
            enqueueRequest(handler)
            if (!epochBumpRequired) return handler.result
        }
        return initializeTransactions(producerIdAndEpoch)
    }

    @Synchronized
    fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    ): TransactionalRequestResult {
        ensureTransactional()
        throwIfPendingState("sendOffsetsToTransaction")
        maybeFailWithError()
        check(currentState == State.IN_TRANSACTION) {
            "Cannot send offsets if a transaction is not in progress (currentState= $currentState)"
        }

        log.debug(
            "Begin adding offsets {} for consumer group {} to transaction",
            offsets,
            groupMetadata,
        )
        val builder = AddOffsetsToTxnRequest.Builder(
            AddOffsetsToTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerIdAndEpoch.producerId)
                .setProducerEpoch(producerIdAndEpoch.epoch)
                .setGroupId(groupMetadata.groupId)
        )
        val handler = AddOffsetsToTxnHandler(builder, offsets, groupMetadata)
        enqueueRequest(handler)
        return handler.result
    }

    @Synchronized
    fun maybeAddPartition(topicPartition: TopicPartition) {
        maybeFailWithError()
        throwIfPendingState("send")
        if (!isTransactional) return

        check(hasProducerId) {
            "Cannot add partition $topicPartition to transaction before completing a call to " +
                    "initTransactions"
        }
        check(currentState == State.IN_TRANSACTION) {
            "Cannot add partition $topicPartition to transaction while in state $currentState"
        }

        if (isPartitionAdded(topicPartition) || isPartitionPendingAdd(topicPartition)) return

        log.debug("Begin adding new partition {} to transaction", topicPartition)
        txnPartitionMap.getOrCreate(topicPartition)
        newPartitionsInTransaction.add(topicPartition)
    }

    fun lastError(): RuntimeException? = lastError

    @Synchronized
    fun isSendToPartitionAllowed(tp: TopicPartition): Boolean {
        return if (hasFatalError) false
        else !isTransactional || partitionsInTransaction.contains(tp)
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionalId"),
    )
    fun transactionalId(): String? = transactionalId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasProducerId"),
    )
    fun hasProducerId(): Boolean = producerIdAndEpoch.isValid

    val hasProducerId: Boolean
        get() = producerIdAndEpoch.isValid

    val isTransactional: Boolean
        get() = transactionalId != null

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasPartitionsToAdd"),
    )
    @Synchronized
    fun hasPartitionsToAdd(): Boolean =
        newPartitionsInTransaction.isNotEmpty() || pendingPartitionsInTransaction.isNotEmpty()

    @get:Synchronized
    val hasPartitionsToAdd: Boolean
        get() = newPartitionsInTransaction.isNotEmpty()
                || pendingPartitionsInTransaction.isNotEmpty()

    @get:Synchronized
    val isCompleting: Boolean
        get() = currentState == State.COMMITTING_TRANSACTION
                || currentState == State.ABORTING_TRANSACTION

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasError"),
    )
    @Synchronized
    fun hasError(): Boolean {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR
    }

    @get:Synchronized
    val hasError: Boolean
        get() = currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR

    @get:Synchronized
    val isAborting: Boolean
        get() = currentState == State.ABORTING_TRANSACTION

    @Synchronized
    fun transitionToAbortableError(exception: RuntimeException) {
        if (currentState == State.ABORTING_TRANSACTION) {
            log.debug(
                "Skipping transition to abortable error state since the transaction is already " +
                        "being aborted. Underlying exception: ",
                exception,
            )
            return
        }
        log.info("Transiting to abortable error state due to {}", exception.toString())
        transitionTo(State.ABORTABLE_ERROR, exception)
    }

    @Synchronized
    fun transitionToFatalError(exception: RuntimeException?) {
        log.info("Transiting to fatal error state due to {}", exception.toString())
        transitionTo(State.FATAL_ERROR, exception)
        if (pendingTransition != null) pendingTransition!!.result.fail(exception)
    }

    // visible for testing
    @Synchronized
    fun isPartitionAdded(partition: TopicPartition): Boolean =
        partitionsInTransaction.contains(partition)

    // visible for testing
    @Synchronized
    fun isPartitionPendingAdd(partition: TopicPartition): Boolean =
        newPartitionsInTransaction.contains(partition)
                || pendingPartitionsInTransaction.contains(partition)

    /**
     * Get the current producer id and epoch without blocking. Callers must use
     * [ProducerIdAndEpoch.isValid] to verify that the result is valid.
     *
     * @return the current ProducerIdAndEpoch.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("producerIdAndEpoch"),
    )
    fun producerIdAndEpoch(): ProducerIdAndEpoch = producerIdAndEpoch

    @Synchronized
    fun maybeUpdateProducerIdAndEpoch(topicPartition: TopicPartition) {
        if (hasStaleProducerIdAndEpoch(topicPartition) && !hasInflightBatches(topicPartition)) {
            // If the batch was on a different ID and/or epoch (due to an epoch bump) and all its
            // in-flight batches have completed, reset the partition sequence so that the next batch
            // (with the new epoch) starts from 0
            txnPartitionMap.startSequencesAtBeginning(topicPartition, producerIdAndEpoch)
            log.debug(
                "ProducerId of partition {} set to {} with epoch {}. Reinitialize sequence at " +
                        "beginning.",
                topicPartition,
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
            )
        }
    }

    /**
     * Set the producer id and epoch atomically.
     */
    @Deprecated("User property instead")
    private fun setProducerIdAndEpoch(producerIdAndEpoch: ProducerIdAndEpoch) {
        log.info(
            "ProducerId set to {} with epoch {}",
            producerIdAndEpoch.producerId,
            producerIdAndEpoch.epoch,
        )
        this.producerIdAndEpoch = producerIdAndEpoch
    }

    /**
     * This method resets the producer ID and epoch and sets the state to UNINITIALIZED, which will
     * trigger a new InitProducerId request. This method is only called when the producer epoch is
     * exhausted; we will bump the epoch instead.
     */
    private fun resetIdempotentProducerId() {
        check(!isTransactional) {
            "Cannot reset producer state for a transactional producer. You must either abort the" +
                    " ongoing transaction or reinitialize the transactional producer instead"
        }
        log.debug(
            "Resetting idempotent producer ID. ID and epoch before reset are {}",
            producerIdAndEpoch
        )
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE
        transitionTo(State.UNINITIALIZED)
    }

    private fun resetSequenceForPartition(topicPartition: TopicPartition) {
        txnPartitionMap.topicPartitions.remove(topicPartition)
        partitionsWithUnresolvedSequences.remove(topicPartition)
    }

    private fun resetSequenceNumbers() {
        txnPartitionMap.reset()
        partitionsWithUnresolvedSequences.clear()
    }

    @Synchronized
    fun requestEpochBumpForPartition(tp: TopicPartition) {
        epochBumpRequired = true
        partitionsToRewriteSequences.add(tp)
    }

    private fun bumpIdempotentProducerEpoch() {
        if (producerIdAndEpoch.epoch == Short.MAX_VALUE) resetIdempotentProducerId()
        else {
            producerIdAndEpoch = ProducerIdAndEpoch(
                producerId = producerIdAndEpoch.producerId,
                epoch = (producerIdAndEpoch.epoch + 1).toShort(),
            )

            log.debug(
                "Incremented producer epoch, current producer ID and epoch are now {}",
                producerIdAndEpoch
            )
        }

        // When the epoch is bumped, rewrite all in-flight sequences for the partition(s) that
        // triggered the epoch bump
        for (topicPartition: TopicPartition in partitionsToRewriteSequences) {
            txnPartitionMap.startSequencesAtBeginning(topicPartition, producerIdAndEpoch)
            partitionsWithUnresolvedSequences.remove(topicPartition)
        }

        partitionsToRewriteSequences.clear()
        epochBumpRequired = false
    }

    @Synchronized
    fun bumpIdempotentEpochAndResetIdIfNeeded() {
        if (!isTransactional) {
            if (epochBumpRequired) bumpIdempotentProducerEpoch()

            if (currentState != State.INITIALIZING && !hasProducerId) {
                transitionTo(State.INITIALIZING)
                val requestData = InitProducerIdRequestData()
                    .setTransactionalId(null)
                    .setTransactionTimeoutMs(Int.MAX_VALUE)

                val handler = InitProducerIdHandler(
                    builder = InitProducerIdRequest.Builder(requestData),
                    isEpochBump = false,
                )
                enqueueRequest(handler)
            }
        }
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    @Synchronized
    fun sequenceNumber(topicPartition: TopicPartition): Int =
        txnPartitionMap.getOrCreate(topicPartition).nextSequence

    /**
     * Returns the current producer id/epoch of the given TopicPartition.
     */
    @Synchronized
    fun producerIdAndEpoch(topicPartition: TopicPartition): ProducerIdAndEpoch =
        txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch

    @Synchronized
    fun incrementSequenceNumber(topicPartition: TopicPartition, increment: Int) {
        var currentSequence: Int = sequenceNumber(topicPartition)
        currentSequence = DefaultRecordBatch.incrementSequence((currentSequence), increment)
        txnPartitionMap[topicPartition].nextSequence = currentSequence
    }

    @Synchronized
    fun addInFlightBatch(batch: ProducerBatch) {
        if (batch.hasSequence()) {
            "Can't track batch for partition ${batch.topicPartition} when sequence is not set."
        }
        txnPartitionMap[batch.topicPartition].inflightBatchesBySequence.add(batch)
    }

    /**
     * Returns the first inflight sequence for a given partition. This is the base sequence of an
     * inflight batch with the lowest sequence number.
     *
     * @return the lowest inflight sequence if the transaction manager is tracking inflight requests
     * for this partition. If there are no inflight requests being tracked for this partition, this
     * method will return [RecordBatch.NO_SEQUENCE].
     */
    @Synchronized
    fun firstInFlightSequence(topicPartition: TopicPartition): Int {
        if (!hasInflightBatches(topicPartition)) return RecordBatch.NO_SEQUENCE

        val inflightBatches = txnPartitionMap[topicPartition].inflightBatchesBySequence
        return if (inflightBatches.isEmpty()) RecordBatch.NO_SEQUENCE
        else inflightBatches.first().baseSequence
    }

    @Synchronized
    fun nextBatchBySequence(topicPartition: TopicPartition): ProducerBatch? {
        val queue = txnPartitionMap[topicPartition].inflightBatchesBySequence
        return if (queue.isEmpty()) null else queue.first()
    }

    @Synchronized
    fun removeInFlightBatch(batch: ProducerBatch) {
        if (hasInflightBatches(batch.topicPartition))
            txnPartitionMap[batch.topicPartition].inflightBatchesBySequence.remove(batch)
    }

    private fun maybeUpdateLastAckedSequence(topicPartition: TopicPartition, sequence: Int): Int {
        val lastAckedSequence = lastAckedSequence(topicPartition) ?: NO_LAST_ACKED_SEQUENCE_NUMBER

        if (sequence > lastAckedSequence) {
            txnPartitionMap[topicPartition].lastAckedSequence = sequence
            return sequence
        }
        return lastAckedSequence
    }

    @Synchronized
    fun lastAckedSequence(topicPartition: TopicPartition): Int? =
        txnPartitionMap.lastAckedSequence(topicPartition)

    @Synchronized
    fun lastAckedOffset(topicPartition: TopicPartition): Long? =
        txnPartitionMap.lastAckedOffset(topicPartition)

    private fun updateLastAckedOffset(
        response: ProduceResponse.PartitionResponse,
        batch: ProducerBatch,
    ) {
        if (response.baseOffset == ProduceResponse.INVALID_OFFSET) return
        val lastOffset = response.baseOffset + batch.recordCount - 1
        val lastAckedOffset = lastAckedOffset(batch.topicPartition)

        // It might happen that the TransactionManager has been reset while a request was reenqueued
        // and got a valid response for this. This can happen only if the producer is only
        // idempotent (not transactional) and in this case there will be no tracked bookkeeper entry
        // about it, so we have to insert one.
        if (lastAckedOffset == null && !isTransactional)
            txnPartitionMap.getOrCreate(batch.topicPartition)

        if (lastOffset > (lastAckedOffset ?: ProduceResponse.INVALID_OFFSET))
            txnPartitionMap[batch.topicPartition].lastAckedOffset = lastOffset
        else log.trace("Partition {} keeps lastOffset at {}", batch.topicPartition, lastOffset)
    }

    @Synchronized
    fun handleCompletedBatch(batch: ProducerBatch, response: ProduceResponse.PartitionResponse) {
        val lastAckedSequence = maybeUpdateLastAckedSequence(
            topicPartition = batch.topicPartition,
            sequence = batch.lastSequence
        )
        log.debug(
            "ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
            batch.producerId,
            batch.topicPartition,
            lastAckedSequence,
        )
        updateLastAckedOffset(response, batch)
        removeInFlightBatch(batch)
    }

    @Synchronized
    fun maybeTransitionToErrorState(exception: RuntimeException) {
        if ((exception is ClusterAuthorizationException
                    || exception is TransactionalIdAuthorizationException
                    || exception is ProducerFencedException
                    || exception is UnsupportedVersionException)
        ) transitionToFatalError(exception)
        else if (isTransactional) {
            if (canBumpEpoch && !isCompleting) epochBumpRequired = true
            transitionToAbortableError(exception)
        }
    }

    @Synchronized
    fun handleFailedBatch(
        batch: ProducerBatch,
        exception: RuntimeException,
        adjustSequenceNumbers: Boolean,
    ) {
        maybeTransitionToErrorState(exception)
        removeInFlightBatch(batch)

        if (hasFatalError) {
            log.debug(
                "Ignoring batch {} with producer id {}, epoch {}, and sequence number {} " +
                        "since the producer is already in fatal error state",
                batch,
                batch.producerId,
                batch.producerEpoch,
                batch.baseSequence,
                exception,
            )
            return
        }

        if (exception is OutOfOrderSequenceException && !isTransactional) {
            log.error(
                "The broker returned {} for topic-partition {} with producerId {}, epoch {}, and " +
                        "sequence number {}",
                exception,
                batch.topicPartition,
                batch.producerId,
                batch.producerEpoch,
                batch.baseSequence,
            )

            // If we fail with an OutOfOrderSequenceException, we have a gap in the log. Bump the
            // epoch for this partition, which will reset the sequence number to 0 and allow us to
            // continue
            requestEpochBumpForPartition(batch.topicPartition)
        } else if (exception is UnknownProducerIdException) {
            // If we get an UnknownProducerId for a partition, then the broker has no state for that
            // producer. It will therefore accept a write with sequence number 0. We reset the
            // sequence number for the partition here so that the producer can continue after
            // aborting the transaction. All inflight-requests to this partition will also fail with
            // an UnknownProducerId error, so the sequence will remain at 0. Note that if the broker
            // supports bumping the epoch, we will later reset all sequence numbers after calling
            // InitProducerId
            resetSequenceForPartition(batch.topicPartition)
        } else if (adjustSequenceNumbers) {
            if (!isTransactional) requestEpochBumpForPartition(batch.topicPartition)
            else adjustSequencesDueToFailedBatch(batch)
        }
    }

    // If a batch is failed fatally, the sequence numbers for future batches bound for the partition
    // must be adjusted so that they don't fail with the OutOfOrderSequenceException.
    //
    // This method must only be called when we know that the batch is question has been
    // unequivocally failed by the broker, ie. it has received a confirmed fatal status code like
    // 'Message Too Large' or something similar.
    private fun adjustSequencesDueToFailedBatch(batch: ProducerBatch) {

        // Sequence numbers are not being tracked for this partition. This could happen if the
        // producer id was just reset due to a previous OutOfOrderSequenceException.
        if (!txnPartitionMap.contains(batch.topicPartition)) return

        log.debug(
            "producerId: {}, send to partition {} failed fatally. Reducing future sequence " +
                    "numbers by {}",
            batch.producerId,
            batch.topicPartition,
            batch.recordCount,
        )

        var currentSequence = sequenceNumber(batch.topicPartition)
        currentSequence -= batch.recordCount

        check(currentSequence >= 0) {
            "Sequence number for partition ${batch.topicPartition} is going to become negative: " +
                    currentSequence
        }

        setNextSequence(batch.topicPartition, currentSequence)
        txnPartitionMap[batch.topicPartition].resetSequenceNumbers { inFlightBatch ->
            if (inFlightBatch.baseSequence < batch.baseSequence) return@resetSequenceNumbers

            val newSequence: Int = inFlightBatch.baseSequence - batch.recordCount

            check(newSequence >= 0) {
                "Sequence number for batch with sequence ${inFlightBatch.baseSequence} for " +
                        "partition ${batch.topicPartition} is going to become negative: " +
                        newSequence
            }

            inFlightBatch.resetProducerState(
                producerIdAndEpoch = ProducerIdAndEpoch(
                    producerId = inFlightBatch.producerId,
                    epoch = inFlightBatch.producerEpoch,
                ),
                baseSequence = newSequence,
                isTransactional = inFlightBatch.isTransactional,
            )
        }
    }

    @Synchronized
    fun hasInflightBatches(topicPartition: TopicPartition): Boolean =
        !txnPartitionMap.getOrCreate(topicPartition).inflightBatchesBySequence.isEmpty()

    @Synchronized
    fun hasStaleProducerIdAndEpoch(topicPartition: TopicPartition): Boolean =
        producerIdAndEpoch != txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch

    @Synchronized
    fun hasUnresolvedSequences(): Boolean = partitionsWithUnresolvedSequences.isNotEmpty()

    @Synchronized
    fun hasUnresolvedSequence(topicPartition: TopicPartition): Boolean =
        partitionsWithUnresolvedSequences.containsKey(topicPartition)

    @Synchronized
    fun markSequenceUnresolved(batch: ProducerBatch) {
        val nextSequence = batch.lastSequence + 1
        partitionsWithUnresolvedSequences.compute(batch.topicPartition) { _, v ->
            if (v == null) nextSequence else max(v, nextSequence)
        }

        log.debug(
            "Marking partition {} unresolved with next sequence number {}",
            batch.topicPartition,
            partitionsWithUnresolvedSequences[batch.topicPartition],
        )
    }

    // Attempts to resolve unresolved sequences. If all in-flight requests are complete and some
    // partitions are still unresolved, either bump the epoch if possible, or transition to a fatal
    // error
    @Synchronized
    fun maybeResolveSequences() {
        val iter = partitionsWithUnresolvedSequences.keys.iterator()
        while (iter.hasNext()) {
            val topicPartition = iter.next()
            if (hasInflightBatches(topicPartition)) continue

            // The partition has been fully drained. At this point, the last ack'd sequence
            // should be one less than next sequence destined for the partition. If so, the
            // partition is fully resolved. If not, we should reset the sequence number if
            // necessary.
            if (isNextSequence(topicPartition, sequenceNumber(topicPartition))) {
                // This would happen when a batch was expired, but subsequent batches succeeded.
                iter.remove()
            } else {
                // We would enter this branch if all in flight batches were ultimately expired in
                // the producer.
                if (isTransactional) {
                    // For the transactional producer, we bump the epoch if possible, otherwise we
                    // transition to a fatal error
                    val unackedMessagesErr = "The client hasn't received acknowledgment for some " +
                            "previously sent messages and can no longer retry them. "

                    if (canBumpEpoch) {
                        epochBumpRequired = true
                        val exception = KafkaException(
                            unackedMessagesErr + "It is safe to abort the transaction and continue."
                        )
                        transitionToAbortableError(exception)
                    } else {
                        val exception =
                            KafkaException(unackedMessagesErr + "It isn't safe to continue.")
                        transitionToFatalError(exception)
                    }
                } else {
                    // For the idempotent producer, bump the epoch
                    log.info(
                        "No inflight batches remaining for {}, last ack'd sequence for partition " +
                                "is {}, next sequence is {}. Going to bump epoch and reset " +
                                "sequence numbers.",
                        topicPartition,
                        lastAckedSequence(topicPartition) ?: NO_LAST_ACKED_SEQUENCE_NUMBER,
                        sequenceNumber(topicPartition),
                    )
                    requestEpochBumpForPartition(topicPartition)
                }
                iter.remove()
            }
        }
    }

    private fun isNextSequence(topicPartition: TopicPartition, sequence: Int): Boolean =
        sequence - (lastAckedSequence(topicPartition) ?: NO_LAST_ACKED_SEQUENCE_NUMBER) == 1

    private fun setNextSequence(topicPartition: TopicPartition, sequence: Int) {
        txnPartitionMap[topicPartition].nextSequence = sequence
    }

    private fun isNextSequenceForUnresolvedPartition(
        topicPartition: TopicPartition,
        sequence: Int,
    ): Boolean =
        hasUnresolvedSequence(topicPartition)
                && sequence == partitionsWithUnresolvedSequences[topicPartition]

    @Synchronized
    fun nextRequest(hasIncompleteBatches: Boolean): TxnRequestHandler? {
        if (newPartitionsInTransaction.isNotEmpty())
            enqueueRequest(addPartitionsToTransactionHandler())
        var nextRequestHandler: TxnRequestHandler? = pendingRequests.peek() ?: return null

        // Do not send the EndTxn until all batches have been flushed
        if (nextRequestHandler!!.isEndTxn && hasIncompleteBatches) return null

        pendingRequests.poll()
        if (maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace(
                "Not sending transactional request {} because we are in an error state",
                nextRequestHandler.requestBuilder(),
            )
            return null
        }

        if (nextRequestHandler.isEndTxn && !transactionStarted) {
            nextRequestHandler.result.done()
            if (currentState != State.FATAL_ERROR) {
                log.debug(
                    "Not sending EndTxn for completed transaction since no partitions " +
                            "or offsets were successfully added"
                )
                completeTransaction()
            }
            nextRequestHandler = pendingRequests.poll()
        }
        if (nextRequestHandler != null) log.trace(
            "Request {} dequeued for sending",
            nextRequestHandler.requestBuilder(),
        )
        return nextRequestHandler
    }

    @Synchronized
    fun retry(request: TxnRequestHandler) {
        request.setRetry()
        enqueueRequest(request)
    }

    @Synchronized
    fun authenticationFailed(e: AuthenticationException?) {
        for (request: TxnRequestHandler in pendingRequests) request.fatalError(e)
    }

    @Synchronized
    fun close() {
        val shutdownException = KafkaException("The producer closed forcefully")
        pendingRequests.forEach { handler -> handler.fatalError(shutdownException) }
        pendingTransition?.result?.fail(shutdownException)
    }

    fun coordinator(type: CoordinatorType): Node? = when (type) {
        CoordinatorType.GROUP -> consumerGroupCoordinator
        CoordinatorType.TRANSACTION -> transactionCoordinator
    }

    fun lookupCoordinator(request: TxnRequestHandler) =
        lookupCoordinator(request.coordinatorType(), request.coordinatorKey())

    fun setInFlightCorrelationId(correlationId: Int) {
        inFlightRequestCorrelationId = correlationId
    }

    private fun clearInFlightCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasInFlightRequest"),
    )
    fun hasInFlightRequest(): Boolean =
        inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID

    val hasInFlightRequest: Boolean
        get() = inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasFatalError"),
    )
    // visible for testing.
    fun hasFatalError(): Boolean = currentState == State.FATAL_ERROR

    // visible for testing.
    val hasFatalError: Boolean
        get() = currentState == State.FATAL_ERROR

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasAbortableError"),
    )
    // visible for testing.
    fun hasAbortableError(): Boolean {
        return currentState == State.ABORTABLE_ERROR
    }

    // visible for testing.
    val hasAbortableError: Boolean
        get() = currentState == State.ABORTABLE_ERROR

    // visible for testing
    @Synchronized
    fun transactionContainsPartition(topicPartition: TopicPartition): Boolean =
        partitionsInTransaction.contains(topicPartition)

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasPendingOffsetCommits"),
    )
    // visible for testing
    @Synchronized
    fun hasPendingOffsetCommits(): Boolean = pendingTxnOffsetCommits.isNotEmpty()

    @get:Synchronized
    // visible for testing
    val hasPendingOffsetCommits: Boolean
        get() = pendingTxnOffsetCommits.isNotEmpty()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasPendingRequests"),
    )
    @Synchronized
    fun hasPendingRequests(): Boolean = pendingRequests.isNotEmpty()

    @get:Synchronized
    val hasPendingRequests: Boolean
        get() = pendingRequests.isNotEmpty()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("hasOngoingTransaction"),
    )
    // visible for testing
    @Synchronized
    fun hasOngoingTransaction(): Boolean {
        // transactions are considered ongoing once started until completion or a fatal error
        return (currentState == State.IN_TRANSACTION) || isCompleting || hasAbortableError
    }

    @get:Synchronized
    // visible for testing
    val hasOngoingTransaction: Boolean
        // transactions are considered ongoing once started until completion or a fatal error
        get() = (currentState == State.IN_TRANSACTION) || isCompleting || hasAbortableError

    @Synchronized
    fun canRetry(response: ProduceResponse.PartitionResponse, batch: ProducerBatch): Boolean {
        val error = response.error

        // An UNKNOWN_PRODUCER_ID means that we have lost the producer state on the broker.
        // Depending on the log start offset, we may want to retry these, as described for each case
        // below. If none of those apply, then for the idempotent producer, we will locally bump the
        // epoch and reset the sequence numbers of in-flight batches from sequence 0, then retry the
        // failed batch, which should now succeed. For the transactional producer, allow the batch
        // to fail. When processing the failed batch, we will transition to an abortable error and
        // set a flag indicating that we need to bump the epoch (if supported by the broker).
        if (error === Errors.UNKNOWN_PRODUCER_ID) {
            if (response.logStartOffset == -1L) {
                // We don't know the log start offset with this response. We should just retry the
                // request until we get it. The UNKNOWN_PRODUCER_ID error code was added along with
                // the new ProduceResponse which includes the logStartOffset. So the '-1' sentinel
                // is not for backward compatibility. Instead, it is possible for a broker to not
                // know the logStartOffset at when it is returning the response because the
                // partition may have moved away from the broker from the time the error was
                // initially raised to the time the response was being constructed. In these cases,
                // we should just retry the request: we are guaranteed to eventually get a
                // logStartOffset once things settle down.
                return true
            }

            if (batch.sequenceHasBeenReset) {
                // When the first inflight batch fails due to the truncation case, then the
                // sequences of all the other in flight batches would have been restarted from the
                // beginning. However, when those responses come back from the broker, they would
                // also come with an UNKNOWN_PRODUCER_ID error. In this case, we should not reset
                // the sequence numbers to the beginning.
                return true
            } else if ((lastAckedOffset(batch.topicPartition)
                    ?: NO_LAST_ACKED_SEQUENCE_NUMBER.toLong()) < response.logStartOffset
            ) {
                // The head of the log has been removed, probably due to the retention time
                // elapsing. In this case, we expect to lose the producer state. For the
                // transactional producer, reset the sequences of all inflight batches to be from
                // the beginning and retry them, so that the transaction does not need to be
                // aborted. For the idempotent producer, bump the epoch to avoid reusing (sequence,
                // epoch) pairs
                if (isTransactional) txnPartitionMap.startSequencesAtBeginning(
                    topicPartition = batch.topicPartition,
                    newProducerIdAndEpoch = producerIdAndEpoch,
                ) else requestEpochBumpForPartition(batch.topicPartition)
                return true
            }
            if (!isTransactional) {
                // For the idempotent producer, always retry UNKNOWN_PRODUCER_ID errors. If the
                // batch has the current producer ID and epoch, request a bump of the epoch.
                // Otherwise just retry the produce.
                requestEpochBumpForPartition(batch.topicPartition)
                return true
            }
        } else if (error === Errors.OUT_OF_ORDER_SEQUENCE_NUMBER) {
            if (!hasUnresolvedSequence(batch.topicPartition)
                && (batch.sequenceHasBeenReset
                        || !isNextSequence(batch.topicPartition, batch.baseSequence))
            ) {
                // We should retry the OutOfOrderSequenceException if the batch is _not_ the next
                // batch, ie. its base sequence isn't the lastAckedSequence + 1.
                return true
            } else if (!isTransactional) {
                // For the idempotent producer, retry all OUT_OF_ORDER_SEQUENCE_NUMBER errors. If
                // there are no unresolved sequences, or this batch is the one immediately following
                // an unresolved sequence, we know there is actually a gap in the sequences, and we
                // bump the epoch. Otherwise, retry without bumping and wait to see if the sequence
                // resolves
                if (!hasUnresolvedSequence(batch.topicPartition)
                    || isNextSequenceForUnresolvedPartition(
                        topicPartition = batch.topicPartition,
                        sequence = batch.baseSequence,
                    )
                ) requestEpochBumpForPartition(batch.topicPartition)

                return true
            }
        }

        // If neither of the above cases are true, retry if the exception is retryable
        return error.exception is RetriableException
    }

    @get:Synchronized
    val isReady: Boolean
        // visible for testing
        get() = isTransactional && currentState == State.READY

    fun handleCoordinatorReady() {
        val nodeApiVersions =
            if (transactionCoordinator != null) apiVersions[transactionCoordinator!!.idString()]
            else null
        val initProducerIdVersion = nodeApiVersions?.apiVersion(ApiKeys.INIT_PRODUCER_ID)
        coordinatorSupportsBumpingEpoch =
            initProducerIdVersion != null && initProducerIdVersion.maxVersion() >= 3
    }

    private fun transitionTo(target: State, error: RuntimeException? = null) {
        check(currentState.isTransitionValid(currentState, target)) {
            val idString =
                if (transactionalId == null) ""
                else "TransactionalId $transactionalId: "

            idString + "Invalid transition attempted from state ${currentState.name} to " +
                    "state ${target.name}"
        }

        lastError = if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            requireNotNull(error) { "Cannot transition to $target with a null exception" }
            error
        } else null

        if (lastError != null) log.debug(
            "Transition from state {} to error state {}",
            currentState,
            target,
            lastError,
        ) else log.debug("Transition from state {} to {}", currentState, target)

        currentState = target
    }

    private fun ensureTransactional() =
        check(isTransactional) { "Transactional method invoked on a non-transactional producer." }

    private fun maybeFailWithError() {
        if (hasError) {
            // for ProducerFencedException, do not wrap it as a KafkaException but create a new
            // instance without the call trace since it was not thrown because of the current call
            when (lastError) {
                is ProducerFencedException -> throw ProducerFencedException(
                    "Producer with transactionalId '$transactionalId' and $producerIdAndEpoch " +
                            "has been fenced by another producer with the same transactionalId"
                )

                is InvalidProducerEpochException -> throw InvalidProducerEpochException(
                    "Producer with transactionalId '$transactionalId' and $producerIdAndEpoch " +
                            "attempted to produce with an old epoch"
                )

                else -> throw KafkaException(
                    "Cannot execute transactional method because we are in an error state",
                    lastError,
                )
            }
        }
    }

    private fun maybeTerminateRequestWithError(requestHandler: TxnRequestHandler): Boolean {
        if (hasError) {
            if (hasAbortableError && requestHandler is FindCoordinatorHandler)
            // No harm letting the FindCoordinator request go through if we're expecting to abort
                return false
            requestHandler.fail(lastError)
            return true
        }
        return false
    }

    private fun enqueueRequest(requestHandler: TxnRequestHandler) {
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder())
        pendingRequests.add(requestHandler)
    }

    private fun lookupCoordinator(type: CoordinatorType?, coordinatorKey: String?) {
        when (type) {
            CoordinatorType.GROUP -> consumerGroupCoordinator = null
            CoordinatorType.TRANSACTION -> transactionCoordinator = null
            else -> error("Invalid coordinator type: $type")
        }

        val data = FindCoordinatorRequestData()
            .setKeyType(type.id)
            .setKey(coordinatorKey)
        val builder = FindCoordinatorRequest.Builder(data)
        enqueueRequest(FindCoordinatorHandler(builder))
    }

    private fun addPartitionsToTransactionHandler(): TxnRequestHandler {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction)
        newPartitionsInTransaction.clear()
        val builder = AddPartitionsToTxnRequest.Builder(
            transactionalId,
            producerIdAndEpoch.producerId,
            producerIdAndEpoch.epoch,
            ArrayList(pendingPartitionsInTransaction)
        )
        return AddPartitionsToTxnHandler(builder)
    }

    private fun txnOffsetCommitHandler(
        result: TransactionalRequestResult,
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata
    ): TxnOffsetCommitHandler {
        for ((partition, offsetAndMetadata) in offsets) {
            val committedOffset = CommittedOffset(
                offsetAndMetadata.offset,
                offsetAndMetadata.metadata, offsetAndMetadata.leaderEpoch()
            )
            pendingTxnOffsetCommits[partition] = committedOffset
        }
        val builder = TxnOffsetCommitRequest.Builder(
            transactionalId = transactionalId,
            consumerGroupId = groupMetadata.groupId,
            producerId = producerIdAndEpoch.producerId,
            producerEpoch = producerIdAndEpoch.epoch,
            pendingTxnOffsetCommits = pendingTxnOffsetCommits,
            memberId = groupMetadata.memberId,
            generationId = groupMetadata.generationId,
            groupInstanceId = groupMetadata.groupInstanceId,
        )
        return TxnOffsetCommitHandler(result, builder)
    }

    private fun throwIfPendingState(operation: String) {
        pendingTransition?.let {
            if (it.result.isAcked) pendingTransition = null
            else error(
                "Cannot attempt operation `$operation` because the previous call to " +
                        "`${pendingTransition!!.operation}` timed out and must be retried"
            )
        }
    }

    private fun handleCachedTransactionRequestResult(
        transactionalRequestResultSupplier: () -> TransactionalRequestResult,
        nextState: State,
        operation: String
    ): TransactionalRequestResult {
        ensureTransactional()
        pendingTransition?.let {
            if (it.result.isAcked) pendingTransition = null
            else if (nextState != pendingTransition!!.state) error(
                "Cannot attempt operation `$operation` because the previous call to " +
                        "`${pendingTransition!!.operation}` timed out and must be retried"
            )
            else it.result
        }
        val result = transactionalRequestResultSupplier()
        pendingTransition = PendingStateTransition(result, nextState, operation)
        return result
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("canBumpEpoch"),
    )
    // package-private for testing
    fun canBumpEpoch(): Boolean {
        return if (!isTransactional) true
        else coordinatorSupportsBumpingEpoch
    }

    // package-private for testing
    val canBumpEpoch: Boolean
        get() = if (!isTransactional) true
        else coordinatorSupportsBumpingEpoch

    private fun completeTransaction() {
        if (epochBumpRequired) transitionTo(State.INITIALIZING)
        else transitionTo(State.READY)

        lastError = null
        epochBumpRequired = false
        transactionStarted = false
        newPartitionsInTransaction.clear()
        pendingPartitionsInTransaction.clear()
        partitionsInTransaction.clear()
    }

    abstract inner class TxnRequestHandler(
        val result: TransactionalRequestResult,
    ) : RequestCompletionHandler {

        var isRetry = false
            private set

        constructor(operation: String) : this(TransactionalRequestResult(operation))

        fun fatalError(e: RuntimeException?) {
            result.fail(e)
            transitionToFatalError(e)
        }

        fun abortableError(e: RuntimeException) {
            result.fail(e)
            transitionToAbortableError(e)
        }

        fun abortableErrorIfPossible(e: RuntimeException) {
            if (canBumpEpoch) {
                epochBumpRequired = true
                abortableError(e)
            } else fatalError(e)
        }

        fun fail(e: RuntimeException?) = result.fail(e)

        fun reenqueue() {
            synchronized(this@TransactionManager) {
                isRetry = true
                enqueueRequest(this)
            }
        }

        open fun retryBackoffMs(): Long = retryBackoffMs

        override fun onComplete(response: ClientResponse) {
            if (response.requestHeader.correlationId() != inFlightRequestCorrelationId)
                fatalError(
                    RuntimeException("Detected more than one in-flight transactional request.")
                )
            else {
                clearInFlightCorrelationId()
                if (response.disconnected) {
                    log.debug("Disconnected from {}. Will retry.", response.destination)
                    if (needsCoordinator) lookupCoordinator(coordinatorType(), coordinatorKey())
                    reenqueue()
                } else if (response.versionMismatch != null) fatalError(response.versionMismatch)
                else if (response.hasResponse) {
                    log.trace(
                        "Received transactional response {} for request {}",
                        response.responseBody,
                        requestBuilder()
                    )
                    synchronized(this@TransactionManager) {
                        handleResponse(response.responseBody)
                    }
                } else fatalError(
                    KafkaException("Could not execute transactional request for unknown reasons")
                )
            }
        }

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("needsCoordinator"),
        )
        fun needsCoordinator(): Boolean = coordinatorType() != null

        val needsCoordinator: Boolean
            get() = coordinatorType() != null

        open fun coordinatorType(): CoordinatorType? = CoordinatorType.TRANSACTION

        open fun coordinatorKey(): String? = transactionalId

        fun setRetry() {
            isRetry = true
        }

        open val isEndTxn: Boolean
            get() = false

        abstract fun requestBuilder(): AbstractRequest.Builder<*>

        abstract fun handleResponse(responseBody: AbstractResponse?)

        abstract fun priority(): Priority
    }

    private inner class InitProducerIdHandler(
        private val builder: InitProducerIdRequest.Builder,
        private val isEpochBump: Boolean
    ) : TxnRequestHandler("InitProducerId") {

        override fun requestBuilder(): InitProducerIdRequest.Builder = builder

        override fun priority(): Priority =
            if (isEpochBump) Priority.EPOCH_BUMP
            else Priority.INIT_PRODUCER_ID

        override fun coordinatorType(): CoordinatorType? {
            return if (isTransactional) CoordinatorType.TRANSACTION
            else null
        }

        override fun handleResponse(responseBody: AbstractResponse?) {
            val initProducerIdResponse = responseBody as InitProducerIdResponse
            val error = initProducerIdResponse.error()

            if (error === Errors.NONE) {
                val producerIdAndEpoch = ProducerIdAndEpoch(
                    initProducerIdResponse.data().producerId(),
                    initProducerIdResponse.data().producerEpoch()
                )
                this@TransactionManager.producerIdAndEpoch = producerIdAndEpoch
                transitionTo(State.READY)
                lastError = null
                if (isEpochBump) resetSequenceNumbers()

                result.done()
            } else if (
                error === Errors.NOT_COORDINATOR
                || error === Errors.COORDINATOR_NOT_AVAILABLE
            ) {
                lookupCoordinator(
                    CoordinatorType.TRANSACTION,
                    transactionalId
                )
                reenqueue()
            } else if (error.exception is RetriableException) reenqueue()
            else if (error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                || error === Errors.CLUSTER_AUTHORIZATION_FAILED
            ) fatalError(error.exception)
            else if (error === Errors.INVALID_PRODUCER_EPOCH || error === Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction
                // coordinator, just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception)
            } else fatalError(
                KafkaException("Unexpected error in InitProducerIdResponse; " + error.message)
            )
        }
    }

    private inner class AddPartitionsToTxnHandler(
        private val builder: AddPartitionsToTxnRequest.Builder,
    ) : TxnRequestHandler("AddPartitionsToTxn") {

        private var retryBackoffMs: Long = this@TransactionManager.retryBackoffMs

        override fun requestBuilder(): AddPartitionsToTxnRequest.Builder = builder

        override fun priority(): Priority {
            return Priority.ADD_PARTITIONS_OR_OFFSETS
        }

        override fun handleResponse(responseBody: AbstractResponse?) {
            val addPartitionsToTxnResponse = responseBody as AddPartitionsToTxnResponse
            val errors = addPartitionsToTxnResponse.errors()
            var hasPartitionErrors = false
            val unauthorizedTopics: MutableSet<String> = HashSet()
            retryBackoffMs = this@TransactionManager.retryBackoffMs

            for ((topicPartition, error) in errors) {
                if (error === Errors.NONE) continue
                else if (
                    error === Errors.COORDINATOR_NOT_AVAILABLE
                    || error === Errors.NOT_COORDINATOR
                ) {
                    lookupCoordinator(
                        CoordinatorType.TRANSACTION,
                        transactionalId
                    )
                    reenqueue()
                    return
                } else if (error === Errors.CONCURRENT_TRANSACTIONS) {
                    maybeOverrideRetryBackoffMs()
                    reenqueue()
                    return
                } else if (error.exception is RetriableException) {
                    reenqueue()
                    return
                } else if (
                    error === Errors.INVALID_PRODUCER_EPOCH
                    || error === Errors.PRODUCER_FENCED
                ) {
                    // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction
                    // coordinator, just treat it the same as PRODUCE_FENCED.
                    fatalError(Errors.PRODUCER_FENCED.exception)
                    return
                } else if (error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatalError(error.exception)
                    return
                } else if (error === Errors.INVALID_TXN_STATE) {
                    fatalError(KafkaException(cause = error.exception))
                    return
                } else if (error === Errors.TOPIC_AUTHORIZATION_FAILED)
                    unauthorizedTopics.add(topicPartition.topic)
                else if (error === Errors.OPERATION_NOT_ATTEMPTED) {
                    log.debug(
                        "Did not attempt to add partition {} to transaction because other " +
                                "partitions in the batch had errors.",
                        topicPartition,
                    )
                    hasPartitionErrors = true
                } else if (
                    error === Errors.UNKNOWN_PRODUCER_ID
                    || error === Errors.INVALID_PRODUCER_ID_MAPPING
                ) {
                    abortableErrorIfPossible(error.exception!!)
                    return
                } else {
                    log.error(
                        "Could not add partition {} due to unexpected error {}",
                        topicPartition,
                        error
                    )
                    hasPartitionErrors = true
                }
            }
            val partitions = errors.keys

            // Remove the partitions from the pending set regardless of the result. We use the
            // presence of partitions in the pending set to know when it is not safe to send
            // batches. However, if the partitions failed to be added and we enter an error state,
            // we expect the batches to be aborted anyway. In this case, we must be able to continue
            // sending the batches which are in retry for partitions that were successfully added.
            pendingPartitionsInTransaction.removeAll(partitions)
            if (unauthorizedTopics.isNotEmpty())
                abortableError(TopicAuthorizationException(unauthorizedTopics))
            else if (hasPartitionErrors) abortableError(
                KafkaException("Could not add partitions to transaction due to errors: $errors")
            ) else {
                log.debug("Successfully added partitions {} to transaction", partitions)
                partitionsInTransaction.addAll(partitions)
                transactionStarted = true
                result.done()
            }
        }

        override fun retryBackoffMs(): Long =
            min(this@TransactionManager.retryBackoffMs, this.retryBackoffMs)

        private fun maybeOverrideRetryBackoffMs() {
            // We only want to reduce the backoff when retrying the first AddPartition which errored
            // out due to a CONCURRENT_TRANSACTIONS error since this means that the previous
            // transaction is still completing and we don't want to wait too long before trying to
            // start the new one.
            //
            // This is only a temporary fix, the long term solution is being tracked in
            // https://issues.apache.org/jira/browse/KAFKA-5482
            if (partitionsInTransaction.isEmpty())
                this.retryBackoffMs = ADD_PARTITIONS_RETRY_BACKOFF_MS
        }
    }

    private inner class FindCoordinatorHandler(
        private val builder: FindCoordinatorRequest.Builder,
    ) : TxnRequestHandler("FindCoordinator") {

        override fun requestBuilder(): FindCoordinatorRequest.Builder = builder

        override fun priority(): Priority = Priority.FIND_COORDINATOR

        override fun coordinatorType(): CoordinatorType? = null

        override fun coordinatorKey(): String? = null

        override fun handleResponse(responseBody: AbstractResponse?) {
            val coordinatorType = CoordinatorType.forId(builder.data().keyType())
            val coordinators = (responseBody as FindCoordinatorResponse).coordinators()

            if (coordinators.size != 1) {
                log.error(
                    "Group coordinator lookup failed: Invalid response containing more than a " +
                            "single coordinator"
                )
                fatalError(
                    IllegalStateException(
                        "Group coordinator lookup failed: Invalid response containing more than " +
                                "a single coordinator"
                    )
                )
            }

            val coordinatorData = coordinators[0]
            // For older versions without batching, obtain key from request data since it is not
            // included in response
            val key =
                if (coordinatorData.key() == null) builder.data().key()
                else coordinatorData.key()

            val error = Errors.forCode(coordinatorData.errorCode())
            if (error === Errors.NONE) {
                val node = Node(
                    id = coordinatorData.nodeId(),
                    host = coordinatorData.host(),
                    port = coordinatorData.port(),
                )

                when (coordinatorType) {
                    CoordinatorType.GROUP -> consumerGroupCoordinator = node
                    CoordinatorType.TRANSACTION -> transactionCoordinator = node
                }
                result.done()
                log.info(
                    "Discovered {} coordinator {}",
                    coordinatorType.toString().lowercase(),
                    node,
                )
            } else if (error.exception is RetriableException) reenqueue()
            else if (error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                fatalError(error.exception)
            else if (error === Errors.GROUP_AUTHORIZATION_FAILED)
                abortableError(GroupAuthorizationException(groupId = key))
            else fatalError(
                KafkaException(
                    "Could not find a coordinator with type $coordinatorType with key $key due " +
                            "to unexpected error: ${coordinatorData.errorMessage()}",
                )
            )
        }
    }

    private inner class EndTxnHandler(
        private val builder: EndTxnRequest.Builder,
    ) : TxnRequestHandler("EndTxn(${builder.data.committed()})") {

        override fun requestBuilder(): EndTxnRequest.Builder = builder

        override fun priority(): Priority = Priority.END_TXN

        override val isEndTxn: Boolean = true

        override fun handleResponse(responseBody: AbstractResponse?) {
            val endTxnResponse = responseBody as EndTxnResponse
            val error = endTxnResponse.error()
            if (error === Errors.NONE) {
                completeTransaction()
                result.done()
            } else if (
                error === Errors.COORDINATOR_NOT_AVAILABLE
                || error === Errors.NOT_COORDINATOR
            ) {
                lookupCoordinator(
                    CoordinatorType.TRANSACTION,
                    transactionalId
                )
                reenqueue()
            } else if (error.exception is RetriableException) reenqueue()
            else if (
                error === Errors.INVALID_PRODUCER_EPOCH
                || error === Errors.PRODUCER_FENCED
            ) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction
                // coordinator, just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception)
            } else if (error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                fatalError(error.exception)
            else if (error === Errors.INVALID_TXN_STATE) fatalError(error.exception)
            else if (
                error === Errors.UNKNOWN_PRODUCER_ID
                || error === Errors.INVALID_PRODUCER_ID_MAPPING
            ) abortableErrorIfPossible(error.exception!!)
            else fatalError(KafkaException("Unhandled error in EndTxnResponse: ${error.message}"))
        }
    }

    private inner class AddOffsetsToTxnHandler(
        private val builder: AddOffsetsToTxnRequest.Builder,
        private val offsets: Map<TopicPartition, OffsetAndMetadata>,
        private val groupMetadata: ConsumerGroupMetadata
    ) : TxnRequestHandler("AddOffsetsToTxn") {

        override fun requestBuilder(): AddOffsetsToTxnRequest.Builder = builder

        override fun priority(): Priority = Priority.ADD_PARTITIONS_OR_OFFSETS

        override fun handleResponse(responseBody: AbstractResponse?) {
            val addOffsetsToTxnResponse = responseBody as AddOffsetsToTxnResponse
            val error = Errors.forCode(addOffsetsToTxnResponse.data().errorCode())

            if (error === Errors.NONE) {
                log.debug(
                    "Successfully added partition for consumer group {} to transaction",
                    builder.data.groupId()
                )

                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, groupMetadata))
                transactionStarted = true
            } else if (
                error === Errors.COORDINATOR_NOT_AVAILABLE
                || error === Errors.NOT_COORDINATOR
            ) {
                lookupCoordinator(
                    CoordinatorType.TRANSACTION,
                    transactionalId
                )
                reenqueue()
            } else if (error.exception is RetriableException) reenqueue()
            else if (
                error === Errors.UNKNOWN_PRODUCER_ID
                || error === Errors.INVALID_PRODUCER_ID_MAPPING
            ) abortableErrorIfPossible(error.exception!!)
            else if (error === Errors.INVALID_PRODUCER_EPOCH || error === Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception)
            } else if (error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                fatalError(error.exception)
            else if (error === Errors.GROUP_AUTHORIZATION_FAILED)
                abortableError(GroupAuthorizationException(groupId = builder.data.groupId()))
            else fatalError(
                KafkaException("Unexpected error in AddOffsetsToTxnResponse: ${error.message}")
            )
        }
    }

    private inner class TxnOffsetCommitHandler(
        result: TransactionalRequestResult,
        private val builder: TxnOffsetCommitRequest.Builder,
    ) : TxnRequestHandler(result) {

        override fun requestBuilder(): TxnOffsetCommitRequest.Builder = builder

        override fun priority(): Priority = Priority.ADD_PARTITIONS_OR_OFFSETS

        override fun coordinatorType(): CoordinatorType = CoordinatorType.GROUP

        override fun coordinatorKey(): String = builder.data.groupId()

        override fun handleResponse(responseBody: AbstractResponse?) {
            val txnOffsetCommitResponse = responseBody as TxnOffsetCommitResponse
            var coordinatorReloaded = false
            val errors = txnOffsetCommitResponse.errors()

            log.debug(
                "Received TxnOffsetCommit response for consumer group {}: {}",
                builder.data.groupId(),
                errors
            )

            for ((topicPartition, error) in errors) {
                if (error === Errors.NONE) pendingTxnOffsetCommits.remove(topicPartition)
                else if (
                    error === Errors.COORDINATOR_NOT_AVAILABLE
                    || error === Errors.NOT_COORDINATOR
                    || error === Errors.REQUEST_TIMED_OUT
                ) {
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true
                        lookupCoordinator(CoordinatorType.GROUP, builder.data.groupId())
                    }
                } else if (
                    error === Errors.UNKNOWN_TOPIC_OR_PARTITION
                    || error === Errors.COORDINATOR_LOAD_IN_PROGRESS
                ) continue // If the topic is unknown or the coordinator is loading, retry with the current coordinator
                else if (error === Errors.GROUP_AUTHORIZATION_FAILED) {
                    abortableError(GroupAuthorizationException(groupId = builder.data.groupId()))
                    break
                } else if (error === Errors.FENCED_INSTANCE_ID) {
                    abortableError(error.exception!!)
                    break
                } else if (
                    error === Errors.UNKNOWN_MEMBER_ID
                    || error === Errors.ILLEGAL_GENERATION
                ) {
                    abortableError(
                        CommitFailedException(
                            "Transaction offset Commit failed due to consumer group metadata " +
                                    "mismatch: ${error.exception!!.message}"
                        )
                    )
                    break
                } else if (isFatalException(error)) {
                    fatalError(error.exception)
                    break
                } else {
                    fatalError(
                        KafkaException("Unexpected error in TxnOffsetCommitResponse: ${error.message}")
                    )
                    break
                }
            }
            if (result.isCompleted) pendingTxnOffsetCommits.clear()
            else if (pendingTxnOffsetCommits.isEmpty()) result.done()
            else reenqueue() // Retry the commits which failed with a retryable error
        }
    }

    private fun isFatalException(error: Errors): Boolean =
        error === Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                || error === Errors.INVALID_PRODUCER_EPOCH
                || error === Errors.PRODUCER_FENCED
                || error === Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT

    private class PendingStateTransition(
        val result: TransactionalRequestResult,
        val state: State,
        val operation: String,
    )

    private enum class State {

        UNINITIALIZED,

        INITIALIZING,

        READY,

        IN_TRANSACTION,

        COMMITTING_TRANSACTION,

        ABORTING_TRANSACTION,

        ABORTABLE_ERROR,

        FATAL_ERROR;

        fun isTransitionValid(source: State, target: State): Boolean = when (target) {
            UNINITIALIZED -> source == READY
            INITIALIZING -> source == UNINITIALIZED || source == ABORTING_TRANSACTION
            READY -> source == INITIALIZING
                    || source == COMMITTING_TRANSACTION
                    || source == ABORTING_TRANSACTION

            IN_TRANSACTION -> source == READY
            COMMITTING_TRANSACTION -> source == IN_TRANSACTION
            ABORTING_TRANSACTION -> source == IN_TRANSACTION || source == ABORTABLE_ERROR
            ABORTABLE_ERROR -> source == IN_TRANSACTION
                    || source == COMMITTING_TRANSACTION
                    || source == ABORTABLE_ERROR

            FATAL_ERROR ->
                // We can transition to FATAL_ERROR unconditionally.
                // FATAL_ERROR is never a valid starting state for any transition. So the only
                // option is to close the producer or do purely non-transactional requests.
                true
        }
    }

    // We use the priority to determine the order in which requests need to be sent out. For
    // instance, if we have a pending FindCoordinator request, that must always go first. Next, If
    // we need a producer id, that must go second. The endTxn request must always go last, unless we
    // are bumping the epoch (a special case of InitProducerId) as part of ending the transaction.
    enum class Priority(val priority: Int) {

        FIND_COORDINATOR(0),

        INIT_PRODUCER_ID(1),

        ADD_PARTITIONS_OR_OFFSETS(2),

        END_TXN(3),

        EPOCH_BUMP(4)
    }

    companion object {

        private val NO_INFLIGHT_REQUEST_CORRELATION_ID = -1

        val NO_LAST_ACKED_SEQUENCE_NUMBER = -1

        // The retryBackoff is overridden to the following value if the first AddPartitions receives
        // a CONCURRENT_TRANSACTIONS error.
        private val ADD_PARTITIONS_RETRY_BACKOFF_MS = 20L
    }
}
