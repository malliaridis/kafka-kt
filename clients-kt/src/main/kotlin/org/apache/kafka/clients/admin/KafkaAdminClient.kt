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

package org.apache.kafka.clients.admin

import java.security.InvalidKeyException
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.util.LinkedList
import java.util.TreeMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.ClientUtils
import org.apache.kafka.clients.ClientUtils.createNetworkClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.DefaultHostResolver
import org.apache.kafka.clients.HostResolver
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.StaleMetadataException
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.OffsetSpec.EarliestSpec
import org.apache.kafka.clients.admin.OffsetSpec.MaxTimestampSpec
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec
import org.apache.kafka.clients.admin.internals.AbortTransactionHandler
import org.apache.kafka.clients.admin.internals.AdminApiDriver
import org.apache.kafka.clients.admin.internals.AdminApiDriver.RequestSpec
import org.apache.kafka.clients.admin.internals.AdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture
import org.apache.kafka.clients.admin.internals.AdminApiHandler
import org.apache.kafka.clients.admin.internals.AdminMetadataManager
import org.apache.kafka.clients.admin.internals.AlterConsumerGroupOffsetsHandler
import org.apache.kafka.clients.admin.internals.CoordinatorKey
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupOffsetsHandler
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupsHandler
import org.apache.kafka.clients.admin.internals.DeleteRecordsHandler
import org.apache.kafka.clients.admin.internals.DescribeConsumerGroupsHandler
import org.apache.kafka.clients.admin.internals.DescribeProducersHandler
import org.apache.kafka.clients.admin.internals.DescribeTransactionsHandler
import org.apache.kafka.clients.admin.internals.FenceProducersHandler
import org.apache.kafka.clients.admin.internals.ListConsumerGroupOffsetsHandler
import org.apache.kafka.clients.admin.internals.ListOffsetsHandler
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler
import org.apache.kafka.clients.admin.internals.RemoveMembersFromConsumerGroupHandler
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.TopicCollection.TopicIdCollection
import org.apache.kafka.common.TopicCollection.TopicNameCollection
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.UnacceptableCredentialException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion
import org.apache.kafka.common.message.CreateAclsRequestData
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.CreateDelegationTokenRequestData
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers
import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs
import org.apache.kafka.common.message.DeleteAclsRequestData
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.message.DescribeClusterRequestData
import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeConfigsResponseData
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResourceResult
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsSynonym
import org.apache.kafka.common.message.DescribeLogDirsRequestData
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic
import org.apache.kafka.common.message.DescribeQuorumResponseData
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListGroupsRequestData
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.RenewDelegationTokenRequestData
import org.apache.kafka.common.message.UnregisterBrokerRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKey
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKeyCollection
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult
import org.apache.kafka.common.metrics.KafkaMetricsContext
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.quota.ClientQuotaFilter
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.AlterClientQuotasRequest
import org.apache.kafka.common.requests.AlterClientQuotasResponse
import org.apache.kafka.common.requests.AlterConfigsRequest
import org.apache.kafka.common.requests.AlterConfigsResponse
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.requests.CreateAclsRequest
import org.apache.kafka.common.requests.CreateAclsResponse
import org.apache.kafka.common.requests.CreateDelegationTokenRequest
import org.apache.kafka.common.requests.CreateDelegationTokenResponse
import org.apache.kafka.common.requests.CreatePartitionsRequest
import org.apache.kafka.common.requests.CreatePartitionsResponse
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.requests.CreateTopicsResponse
import org.apache.kafka.common.requests.DeleteAclsRequest
import org.apache.kafka.common.requests.DeleteAclsResponse
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.requests.DeleteTopicsResponse
import org.apache.kafka.common.requests.DescribeAclsRequest
import org.apache.kafka.common.requests.DescribeAclsResponse
import org.apache.kafka.common.requests.DescribeClientQuotasRequest
import org.apache.kafka.common.requests.DescribeClientQuotasResponse
import org.apache.kafka.common.requests.DescribeClusterRequest
import org.apache.kafka.common.requests.DescribeClusterResponse
import org.apache.kafka.common.requests.DescribeConfigsRequest
import org.apache.kafka.common.requests.DescribeConfigsResponse
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse
import org.apache.kafka.common.requests.DescribeLogDirsRequest
import org.apache.kafka.common.requests.DescribeLogDirsResponse
import org.apache.kafka.common.requests.DescribeQuorumRequest
import org.apache.kafka.common.requests.DescribeQuorumResponse
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse
import org.apache.kafka.common.requests.ElectLeadersRequest
import org.apache.kafka.common.requests.ElectLeadersResponse
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.ListGroupsRequest
import org.apache.kafka.common.requests.ListGroupsResponse
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.RenewDelegationTokenRequest
import org.apache.kafka.common.requests.RenewDelegationTokenResponse
import org.apache.kafka.common.requests.UnregisterBrokerRequest
import org.apache.kafka.common.requests.UnregisterBrokerResponse
import org.apache.kafka.common.requests.UpdateFeaturesRequest
import org.apache.kafka.common.requests.UpdateFeaturesResponse
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramFormatter
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.slf4j.Logger
import kotlin.math.min

/**
 * The default implementation of [Admin]. An instance of this class is created by invoking one of the
 * `create()` methods in `AdminClient`. Users should not refer to this class directly.
 *
 * This class is thread-safe.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @property clientId The name of this AdminClient instance.
 * @property time Provides the time.
 * @property metadataManager The cluster metadata manager used by the KafkaClient.
 * @property metrics The metrics for this KafkaAdminClient.
 * @property client The network client to use.
 * @property timeoutProcessorFactory A factory which creates TimeoutProcessors for the RPC thread.
 */
@Evolving
class KafkaAdminClient internal constructor(
    config: AdminClientConfig,
    internal val clientId: String,
    private val time: Time,
    private val metadataManager: AdminMetadataManager,
    val metrics: Metrics,
    private val client: KafkaClient,
    private val timeoutProcessorFactory: TimeoutProcessorFactory = TimeoutProcessorFactory(),
    private val logContext: LogContext,
) : AdminClient() {

    private val log: Logger = logContext.logger(KafkaAdminClient::class.java)

    /**
     * The default timeout to use for an operation.
     */
    private val defaultApiTimeoutMs: Int = configureDefaultApiTimeoutMs(config)

    /**
     * The timeout to use for a single request.
     */
    private val requestTimeoutMs: Int = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)!!

    /**
     * The runnable used in the service thread for this admin client.
     */
    private val runnable: AdminClientRunnable = AdminClientRunnable()

    /**
     * The network service thread for this admin client.
     */
    private val thread: Thread = KafkaThread("$NETWORK_THREAD_PREFIX | $clientId", runnable, true)

    /**
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private val hardShutdownTimeMs = AtomicLong(INVALID_SHUTDOWN_TIME)

    private val maxRetries: Int = config.getInt(AdminClientConfig.RETRIES_CONFIG)!!

    private val retryBackoffMs: Long = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG)!!

    /**
     * Get the deadline for a particular call.
     *
     * @param now The current time in milliseconds.
     * @param optionTimeoutMs The timeout option given by the user.
     *
     * @return The deadline in milliseconds.
     */
    private fun calcDeadlineMs(now: Long, optionTimeoutMs: Int?): Long {
        return now + (optionTimeoutMs ?: defaultApiTimeoutMs).coerceAtLeast(0)
    }

    init {
        config.logUnused()
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds())
        log.debug("Kafka admin client initialized")
        thread.start()
    }

    /**
     * If a default.api.timeout.ms has been explicitly specified, raise an error if it conflicts
     * with request.timeout.ms. If no default.api.timeout.ms has been configured, then set its value
     * as the max of the default and request.timeout.ms. Also we should probably log a warning.
     * Otherwise, use the provided values for both configurations.
     *
     * @param config The configuration
     */
    private fun configureDefaultApiTimeoutMs(config: AdminClientConfig): Int {
        val requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)!!
        val defaultApiTimeoutMs = config.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)!!
        if (defaultApiTimeoutMs < requestTimeoutMs) {
            if (config.originals().containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
                throw ConfigException(
                    "The specified value of ${AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG}" +
                            " must be no smaller than the value of ${AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG}."
                )
            } else {
                log.warn(
                    "Overriding the default value for {} ({}) with the explicitly configured request timeout {}",
                    AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, this.defaultApiTimeoutMs,
                    requestTimeoutMs
                )
                return requestTimeoutMs
            }
        }
        return defaultApiTimeoutMs
    }

    override fun close(timeout: Duration) {
        var waitTimeMs = timeout.toMillis()
        if (waitTimeMs < 0) throw IllegalArgumentException("The timeout cannot be negative.")
        waitTimeMs =
            waitTimeMs.coerceAtMost(TimeUnit.DAYS.toMillis(365)) // Limit the timeout to a year.
        val now = time.milliseconds()
        var newHardShutdownTimeMs = now + waitTimeMs
        var prev = INVALID_SHUTDOWN_TIME
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) log.debug("Initiating close operation.")
                else log.debug("Moving hard shutdown time forward.")
                client.wakeup() // Wake the thread, if it is blocked inside poll().
                break
            }
            prev = hardShutdownTimeMs.get()
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.")
                newHardShutdownTimeMs = prev
                break
            }
        }
        if (log.isDebugEnabled) {
            val deltaMs = (newHardShutdownTimeMs - time.milliseconds()).coerceAtLeast(0)
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs)
        }
        try {
            // close() can be called by AdminClient thread when it invokes callback. That will
            // cause deadlock, so check for that condition.
            if (Thread.currentThread() !== thread) {
                // Wait for the thread to be joined.
                thread.join(waitTimeMs)
            }
            log.debug("Kafka admin client closed.")
        } catch (e: InterruptedException) {
            log.debug("Interrupted while joining I/O thread", e)
            Thread.currentThread().interrupt()
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    internal interface NodeProvider {
        fun provide(): Node?
    }

    private inner class MetadataUpdateNodeIdProvider : NodeProvider {
        override fun provide(): Node? {
            return client.leastLoadedNode(time.milliseconds())
        }
    }

    private inner class ConstantNodeIdProvider(private val nodeId: Int) : NodeProvider {
        override fun provide(): Node? {
            if (metadataManager.isReady && (metadataManager.nodeById(nodeId) != null)) {
                return metadataManager.nodeById(nodeId)
            }
            // If we can't find the node with the given constant ID, we schedule a
            // metadata update and hope it appears. This behavior is useful for avoiding
            // flaky behavior in tests when the cluster is starting up and not all nodes
            // have appeared.
            metadataManager.requestUpdate()
            return null
        }
    }

    /**
     * Provides the controller node.
     */
    private inner class ControllerNodeProvider : NodeProvider {
        override fun provide(): Node? {
            if (metadataManager.isReady && (metadataManager.controller() != null)) {
                return metadataManager.controller()
            }
            metadataManager.requestUpdate()
            return null
        }
    }

    /**
     * Provides the least loaded node.
     */
    private inner class LeastLoadedNodeProvider : NodeProvider {
        override fun provide(): Node? {
            if (metadataManager.isReady) {
                // This may return null if all nodes are busy.
                // In that case, we will postpone node assignment.
                return client.leastLoadedNode(time.milliseconds())
            }
            metadataManager.requestUpdate()
            return null
        }
    }

    internal abstract inner class Call(
        val callName: String,
        val internal: Boolean = false,
        var nextAllowedTryMs: Long = 0,
        var tries: Int = 0,
        val deadlineMs: Long,
        val nodeProvider: NodeProvider,
    ) {
        var curNode: Node? = null

        fun curNode(): Node? {
            return curNode
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now The current time in milliseconds.
         * @param throwable The failure exception.
         */
        fun fail(now: Long, throwable: Throwable) {
            if (curNode != null) {
                runnable.nodeReadyDeadlines.remove(curNode)
                curNode = null
            }
            // If the admin client is closing, we can't retry.
            if (runnable.closing) {
                handleFailure(throwable)
                return
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable is UnsupportedVersionException) && handleUnsupportedVersionException(
                    throwable
                )
            ) {
                log.debug("{} attempting protocol downgrade and then retry.", this)
                runnable.pendingCalls.add(this)
                return
            }
            tries++
            nextAllowedTryMs = now + retryBackoffMs

            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) <= 0) {
                handleTimeoutFailure(now, throwable)
                return
            }
            // If the exception is not retriable, fail.
            if (throwable !is RetriableException) {
                if (log.isDebugEnabled) {
                    log.debug(
                        "{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        Exception(prettyPrintException(throwable))
                    )
                }
                handleFailure(throwable)
                return
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                handleTimeoutFailure(now, throwable)
                return
            }
            if (log.isDebugEnabled) {
                log.debug(
                    "{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries
                )
            }
            maybeRetry(now, throwable)
        }

        open fun maybeRetry(now: Long, throwable: Throwable?) {
            runnable.pendingCalls.add(this)
        }

        fun handleTimeoutFailure(now: Long, cause: Throwable?) {
            if (log.isDebugEnabled) log.debug(
                "{} timed out at {} after {} attempt(s)",
                this,
                now,
                tries,
                Exception(prettyPrintException(cause))
            )
            if (cause is TimeoutException) handleFailure(cause)
            else handleFailure(
                TimeoutException("$this timed out at $now after $tries attempt(s)", cause)
            )
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         * @return The AbstractRequest builder.
         */
        abstract fun createRequest(timeoutMs: Int): AbstractRequest.Builder<*>

        /**
         * Process the call response.
         *
         * @param abstractResponse The AbstractResponse.
         */
        abstract fun handleResponse(abstractResponse: AbstractResponse)

        /**
         * Handle a failure. This will only be called if the failure exception was not
         * retriable, or if we hit a timeout.
         *
         * @param throwable The exception.
         */
        abstract fun handleFailure(throwable: Throwable)

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception The exception.
         *
         * @return True if the exception can be handled; false otherwise.
         */
        open fun handleUnsupportedVersionException(exception: UnsupportedVersionException): Boolean =
            false

        override fun toString(): String {
            return ("Call(callName=" + callName + ", deadlineMs=" + deadlineMs +
                    ", tries=" + tries + ", nextAllowedTryMs=" + nextAllowedTryMs + ")")
        }
    }

    open class TimeoutProcessorFactory {
        open fun create(now: Long): TimeoutProcessor {
            return TimeoutProcessor(now)
        }
    }

    /**
     * Create a new timeout processor.
     *
     * @property now The current time in milliseconds since the epoch.
     */
    open class TimeoutProcessor(
        private val now: Long,
    ) {
        /**
         * The number of milliseconds until the next timeout.
         */
        private var nextTimeoutMs: Int = Int.MAX_VALUE

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        internal fun handleTimeouts(calls: MutableCollection<Call>, msg: String): Int {
            var numTimedOut = 0
            val iter = calls.iterator()
            while (iter.hasNext()) {
                val call = iter.next()
                val remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs)
                if (remainingMs < 0) {
                    call.fail(now, TimeoutException(msg + " Call: " + call.callName))
                    iter.remove()
                    numTimedOut++
                } else {
                    nextTimeoutMs = nextTimeoutMs.coerceAtMost(remainingMs)
                }
            }
            return numTimedOut
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call The call.
         *
         * @return True if the call should be timed out.
         */
        internal open fun callHasExpired(call: Call): Boolean {
            val remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs)
            if (remainingMs < 0) return true
            nextTimeoutMs = nextTimeoutMs.coerceAtMost(remainingMs)
            return false
        }

        fun nextTimeoutMs(): Int {
            return nextTimeoutMs
        }
    }

    private inner class AdminClientRunnable : Runnable {

        /**
         * Calls which have not yet been assigned to a node.
         * Only accessed from this thread.
         */
        val pendingCalls = ArrayList<Call>()

        /**
         * Maps nodes to calls that we want to send.
         * Only accessed from this thread.
         */
        private val callsToSend: MutableMap<Node, MutableList<Call>> = HashMap()

        /**
         * Maps node ID strings to calls that have been sent.
         * Only accessed from this thread.
         */
        private val callsInFlight: MutableMap<String, Call> = HashMap()

        /**
         * Maps correlation IDs to calls that have been sent.
         * Only accessed from this thread.
         */
        private val correlationIdToCalls: MutableMap<Int, Call> = HashMap()

        /**
         * Pending calls. Protected by the object monitor.
         */
        private val newCalls: MutableList<Call> = LinkedList()

        /**
         * Maps node ID strings to their readiness deadlines. A node will appear in this
         * map if there are callsToSend which are waiting for it to be ready, and there
         * are no calls in flight using the node.
         */
        val nodeReadyDeadlines: MutableMap<Node, Long> = HashMap()

        /**
         * Whether the admin client is closing.
         */
        @Volatile
        var closing = false

        /**
         * Time out the elements in the pendingCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private fun timeoutPendingCalls(processor: TimeoutProcessor) {
            val numTimedOut =
                processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.")
            if (numTimedOut > 0) log.debug("Timed out {} pending calls.", numTimedOut)
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         */
        private fun timeoutCallsToSend(processor: TimeoutProcessor): Int {
            var numTimedOut = 0
            for (callList: MutableList<Call> in callsToSend.values) {
                numTimedOut += processor.handleTimeouts(callList, "Timed out waiting to send the call.")
            }
            if (numTimedOut > 0) log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut)
            return numTimedOut
        }

        /**
         * Drain all the calls from newCalls into pendingCalls.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         */
        @Synchronized
        private fun drainNewCalls() {
            transitionToPendingAndClearList(newCalls)
        }

        /**
         * Add some calls to pendingCalls, and then clear the input list.
         * Also clears Call#curNode.
         *
         * @param calls         The calls to add.
         */
        private fun transitionToPendingAndClearList(calls: MutableList<Call>) {
            for (call: Call in calls) {
                call.curNode = null
                pendingCalls.add(call)
            }
            calls.clear()
        }

        /**
         * Choose nodes for the calls in the pendingCalls list.
         *
         * @param now           The current time in milliseconds.
         * @return              The minimum time until a call is ready to be retried if any of the pending
         * calls are backing off after a failure
         */
        private fun maybeDrainPendingCalls(now: Long): Long {
            var pollTimeout = Long.MAX_VALUE
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now)
            val pendingIter = pendingCalls.iterator()
            while (pendingIter.hasNext()) {
                val call = pendingIter.next()
                // If the call is being retried, await the proper backoff before finding the node
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = min(pollTimeout, call.nextAllowedTryMs - now)
                } else if (maybeDrainPendingCall(call, now)) {
                    pendingIter.remove()
                }
            }
            return pollTimeout
        }

        /**
         * Check whether a pending call can be assigned a node. Return true if the pending call was either
         * transferred to the callsToSend collection or if the call was failed. Return false if it
         * should remain pending.
         */
        private fun maybeDrainPendingCall(call: Call, now: Long): Boolean {
            try {
                val node = call.nodeProvider.provide()
                return if (node != null) {
                    log.trace("Assigned {} to node {}", call, node)
                    call.curNode = node
                    callsToSend.computeIfAbsent(node) { LinkedList() }.add(call)
                    true
                } else {
                    log.trace("Unable to assign {} to a node.", call)
                    false
                }
            } catch (t: Throwable) {
                // Handle authentication errors while choosing nodes.
                log.debug("Unable to choose node for {}", call, t)
                call.fail(now, t)
                return true
            }
        }

        /**
         * Send the calls which are ready.
         *
         * @param now The current time in milliseconds.
         * @return The minimum timeout we need for poll().
         */
        private fun sendEligibleCalls(now: Long): Long {
            var pollTimeout = Long.MAX_VALUE
            val iter: MutableIterator<Map.Entry<Node, MutableList<Call>>> = callsToSend.iterator()
            while (iter.hasNext()) {
                val entry = iter.next()
                val calls = entry.value
                if (calls.isEmpty()) {
                    iter.remove()
                    continue
                }
                val node = entry.key
                if (callsInFlight.containsKey(node.idString())) {
                    log.trace("Still waiting for other calls to finish on node {}.", node)
                    nodeReadyDeadlines.remove(node)
                    continue
                }
                if (!client.ready(node, now)) {
                    val deadline = nodeReadyDeadlines[node]
                    if (deadline != null) {
                        if (now >= deadline) {
                            log.info(
                                "Disconnecting from {} and revoking {} node assignment(s) " +
                                        "because the node is taking too long to become ready.",
                                node.idString(),
                                calls.size,
                            )
                            transitionToPendingAndClearList(calls)
                            client.disconnect(node.idString())
                            nodeReadyDeadlines.remove(node)
                            iter.remove()
                            continue
                        }
                        pollTimeout = min(pollTimeout, deadline - now)
                    } else {
                        nodeReadyDeadlines[node] = now + requestTimeoutMs
                    }
                    val nodeTimeout = client.pollDelayMs(node, now)
                    pollTimeout = min(pollTimeout, nodeTimeout)
                    log.trace(
                        "Client is not ready to send to {}. Must delay {} ms",
                        node,
                        nodeTimeout
                    )
                    continue
                }
                // Subtract the time we spent waiting for the node to become ready from
                // the total request time.
                val deadlineMs = nodeReadyDeadlines.remove(node)
                val remainingRequestTime = if (deadlineMs == null) requestTimeoutMs
                else calcTimeoutMsRemainingAsInt(now, deadlineMs)

                while (calls.isNotEmpty()) {
                    val call = calls.removeAt(0)
                    val timeoutMs = min(
                        remainingRequestTime,
                        calcTimeoutMsRemainingAsInt(now, call.deadlineMs),
                    )
                    var requestBuilder: AbstractRequest.Builder<*>?
                    try {
                        requestBuilder = call.createRequest(timeoutMs)
                    } catch (t: Throwable) {
                        call.fail(
                            now,
                            KafkaException("Internal error sending ${call.callName} to $node.", t),
                        )
                        continue
                    }
                    val clientRequest = client.newClientRequest(
                        nodeId = node.idString(),
                        requestBuilder = requestBuilder,
                        createdTimeMs = now,
                        expectResponse = true,
                        requestTimeoutMs = timeoutMs,
                        callback = null,
                    )
                    log.debug(
                        "Sending {} to {}. correlationId={}, timeoutMs={}",
                        requestBuilder,
                        node,
                        clientRequest.correlationId,
                        timeoutMs,
                    )
                    client.send(clientRequest, now)
                    callsInFlight[node.idString()] = call
                    correlationIdToCalls[clientRequest.correlationId] = call
                    break
                }
            }
            return pollTimeout
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They
         * may even be in the process of being processed by the remote server. At the moment, our
         * only option to time them out is to close the entire connection.
         *
         * @param processor The timeout processor.
         */
        private fun timeoutCallsInFlight(processor: TimeoutProcessor) {
            var numTimedOut = 0
            for ((nodeId, call) in callsInFlight) {
                if (processor.callHasExpired(call)) {
                    log.info("Disconnecting from {} due to timeout while awaiting {}", nodeId, call)
                    client.disconnect(nodeId)
                    numTimedOut++
                    // We don't remove anything from the callsInFlight data structure. Because the
                    // connection has been closed, the calls should be returned by the next
                    // client#poll(), and handled at that point.
                }
            }
            if (numTimedOut > 0) log.debug("Timed out {} call(s) in flight.", numTimedOut)
        }

        /**
         * Handle responses from the server.
         *
         * @param now The current time in milliseconds.
         * @param responses The latest responses from KafkaClient.
         */
        private fun handleResponses(now: Long, responses: List<ClientResponse>) {
            for (response in responses) {
                val correlationId = response.requestHeader.correlationId
                val call = correlationIdToCalls[correlationId]
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error
                    // message.
                    log.error(
                        "Internal server error on {}: server returned information about unknown " +
                                "correlation ID {}, requestHeader = {}",
                        response.destination,
                        correlationId,
                        response.requestHeader,
                    )
                    client.disconnect(response.destination)
                    continue
                }

                // Stop tracking this call.
                correlationIdToCalls.remove(correlationId)
                if (!callsInFlight.remove(response.destination, call)) {
                    log.error(
                        "Internal server error on {}: ignoring call {} in correlationIdToCall " +
                                "that did not exist in callsInFlight", response.destination, call
                    )
                    continue
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
                // retriable exception.
                if (response.versionMismatch != null) call.fail(now, response.versionMismatch)
                else if (response.disconnected) {
                    val authException = client.authenticationException(call.curNode()!!)
                    if (authException != null) call.fail(now, authException)
                    else {
                        call.fail(
                            now = now,
                            throwable = DisconnectException(
                                "Cancelled ${call.callName} request with correlation id " +
                                        "$correlationId due to node ${response.destination} " +
                                        "being disconnected",
                            )
                        )
                    }
                } else {
                    try {
                        call.handleResponse(response.responseBody!!)
                        if (log.isTraceEnabled) log.trace(
                            "{} got response {}",
                            call,
                            response.responseBody,
                        )
                    } catch (t: Throwable) {
                        if (log.isTraceEnabled) log.trace(
                            "{} handleResponse failed with {}",
                            call,
                            prettyPrintException(t),
                        )
                        call.fail(now, t)
                    }
                }
            }
        }

        /**
         * Unassign calls that have not yet been sent based on some predicate. For example, this is
         * used to reassign the calls that have been assigned to a disconnected node.
         *
         * @param shouldUnassign Condition for reassignment. If the predicate is true, then the
         * calls will be put back in the pendingCalls collection and they will be reassigned
         */
        private fun unassignUnsentCalls(shouldUnassign: Predicate<Node>) {
            val iterator = callsToSend.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                val node = entry.key
                val awaitingCalls = entry.value

                if (awaitingCalls.isEmpty()) iterator.remove()
                else if (shouldUnassign.test(node)) {
                    nodeReadyDeadlines.remove(node)
                    transitionToPendingAndClearList(awaitingCalls)
                    iterator.remove()
                }
            }
        }

        private fun hasActiveExternalCalls(calls: Collection<Call>): Boolean =
            calls.any { !it.internal }

        /**
         * Return true if there are currently active external calls.
         */
        private fun hasActiveExternalCalls(): Boolean {
            if (hasActiveExternalCalls(pendingCalls)) return true

            return callsToSend.values.any { hasActiveExternalCalls(it) }
                    || hasActiveExternalCalls(correlationIdToCalls.values)
        }

        private fun threadShouldExit(now: Long, curHardShutdownTimeMs: Long): Boolean {
            if (!hasActiveExternalCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.")
                return true
            }
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.")
                return true
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now)
            return false
        }

        override fun run() {
            log.debug("Thread starting")
            try {
                processRequests()
            } finally {
                closing = true
                AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics)
                var numTimedOut = 0
                val timeoutProcessor = TimeoutProcessor(Long.MAX_VALUE)
                synchronized(this) {
                    numTimedOut += timeoutProcessor.handleTimeouts(
                        newCalls,
                        "The AdminClient thread has exited."
                    )
                }
                numTimedOut += timeoutProcessor.handleTimeouts(
                    pendingCalls,
                    "The AdminClient thread has exited."
                )
                numTimedOut += timeoutCallsToSend(timeoutProcessor)
                numTimedOut += timeoutProcessor.handleTimeouts(
                    correlationIdToCalls.values,
                    "The AdminClient thread has exited."
                )
                if (numTimedOut > 0) {
                    log.info("Timed out {} remaining operation(s) during close.", numTimedOut)
                }
                Utils.closeQuietly(client, "KafkaClient")
                Utils.closeQuietly(metrics, "Metrics")
                log.debug("Exiting AdminClientRunnable thread.")
            }
        }

        private fun processRequests() {
            var now = time.milliseconds()
            while (true) {
                // Copy newCalls into pendingCalls.
                drainNewCalls()

                // Check if the AdminClient thread should shut down.
                val curHardShutdownTimeMs = hardShutdownTimeMs.get()
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(
                        now,
                        curHardShutdownTimeMs
                    )
                ) break

                // Handle timeouts.
                val timeoutProcessor = timeoutProcessorFactory.create(now)
                timeoutPendingCalls(timeoutProcessor)
                timeoutCallsToSend(timeoutProcessor)
                timeoutCallsInFlight(timeoutProcessor)
                var pollTimeout = timeoutProcessor.nextTimeoutMs().coerceAtMost(1200000).toLong()
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = pollTimeout.coerceAtMost(curHardShutdownTimeMs - now)
                }

                // Choose nodes for our pending calls.
                pollTimeout = pollTimeout.coerceAtMost(maybeDrainPendingCalls(now))
                val metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now)
                if (metadataFetchDelayMs == 0L) {
                    metadataManager.transitionToUpdatePending(now)
                    val metadataCall = makeMetadataCall(now)
                    // Create a new metadata fetch call and add it to the end of pendingCalls.
                    // Assign a node for just the new call (we handled the other pending nodes above).
                    if (!maybeDrainPendingCall(metadataCall, now)) pendingCalls.add(metadataCall)
                }
                pollTimeout = pollTimeout.coerceAtMost(sendEligibleCalls(now))
                if (metadataFetchDelayMs > 0) {
                    pollTimeout = pollTimeout.coerceAtMost(metadataFetchDelayMs)
                }

                // Ensure that we use a small poll timeout if there are pending calls which need to be sent
                if (pendingCalls.isNotEmpty()) pollTimeout = pollTimeout.coerceAtMost(retryBackoffMs)

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout)
                val responses = client.poll(pollTimeout.coerceAtLeast(0L), now)
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size)

                // unassign calls to disconnected nodes
                unassignUnsentCalls(client::connectionFailed)

                // Update the current time and handle the latest responses.
                now = time.milliseconds()
                handleResponses(now, responses)
            }
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call The new call object.
         * @param now The current time in milliseconds.
         */
        fun enqueue(call: Call, now: Long) {
            if (call.tries > maxRetries) {
                log.debug("Max retries {} for {} reached", maxRetries, call)
                call.handleTimeoutFailure(
                    time.milliseconds(),
                    TimeoutException("Exceeded maxRetries after ${call.tries} tries."),
                )
                return
            }
            if (log.isDebugEnabled) log.debug(
                "Queueing {} with a timeout {} ms from now.",
                call,
                min(requestTimeoutMs.toLong(), call.deadlineMs - now),
            )

            var accepted = false
            synchronized(this) {
                if (!closing) {
                    newCalls.add(call)
                    accepted = true
                }
            }
            if (accepted) client.wakeup() // wake the thread if it is in poll()
            else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call)
                call.handleTimeoutFailure(
                    time.milliseconds(),
                    TimeoutException("The AdminClient thread has exited.")
                )
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call The new call object.
         * @param now The current time in milliseconds.
         */
        fun call(call: Call, now: Long) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("The AdminClient is not accepting new calls. Timing out {}.", call)
                call.handleTimeoutFailure(
                    time.milliseconds(),
                    TimeoutException("The AdminClient thread is not accepting new calls.")
                )
            } else {
                enqueue(call, now)
            }
        }

        /**
         * Create a new metadata call.
         */
        private fun makeMetadataCall(now: Long): Call {
            return object : Call(
                internal = true,
                callName = "fetchMetadata",
                deadlineMs = calcDeadlineMs(now, requestTimeoutMs),
                nodeProvider = MetadataUpdateNodeIdProvider(),
            ) {
                override fun createRequest(timeoutMs: Int): MetadataRequest.Builder {
                    // Since this only requests node information, it's safe to pass true
                    // for allowAutoTopicCreation (and it simplifies communication with
                    // older brokers)
                    return MetadataRequest.Builder(
                        MetadataRequestData()
                            .setTopics(emptyList())
                            .setAllowAutoTopicCreation(true)
                    )
                }

                override fun handleResponse(abstractResponse: AbstractResponse) {
                    val response = abstractResponse as MetadataResponse
                    metadataManager.update(response.buildCluster(), time.milliseconds())

                    // Unassign all unsent requests after a metadata refresh to allow for a new
                    // destination to be selected from the new metadata
                    unassignUnsentCalls { true }
                }

                override fun handleFailure(throwable: Throwable) {
                    metadataManager.updateFailed(throwable)
                }
            }
        }
    }

    // for testing
    fun numPendingCalls(): Int {
        return runnable.pendingCalls.size
    }

    override fun createTopics(
        newTopics: Collection<NewTopic>,
        options: CreateTopicsOptions,
    ): CreateTopicsResult {
        val topicFutures: MutableMap<String, KafkaFutureImpl<TopicMetadataAndConfig>> = HashMap(newTopics.size)
        val topics = CreatableTopicCollection()
        for (newTopic: NewTopic in newTopics) {
            if (topicNameIsUnrepresentable(newTopic.name)) {
                val future = KafkaFutureImpl<TopicMetadataAndConfig>()
                future.completeExceptionally(
                    InvalidTopicException(
                        "The given topic name '${newTopic.name}' cannot be represented in a " +
                                "request."
                    )
                )
                topicFutures[newTopic.name] = future
            } else if (!topicFutures.containsKey(newTopic.name)) {
                topicFutures[newTopic.name] = KafkaFutureImpl()
                topics.add(newTopic.convertToCreatableTopic())
            }
        }
        if (!topics.isEmpty()) {
            val now = time.milliseconds()
            val deadline = calcDeadlineMs(now, options.timeoutMs)
            val call = getCreateTopicsCall(
                options = options,
                futures = topicFutures,
                topics = topics,
                quotaExceededExceptions = emptyMap(),
                now = now,
                deadline = deadline,
            )
            runnable.call(call, now)
        }
        return CreateTopicsResult(topicFutures.toMap())
    }

    private fun getCreateTopicsCall(
        options: CreateTopicsOptions,
        futures: Map<String, KafkaFutureImpl<TopicMetadataAndConfig>>,
        topics: CreatableTopicCollection,
        quotaExceededExceptions: Map<String, ThrottlingQuotaExceededException>,
        now: Long,
        deadline: Long,
    ): Call {
        return object : Call(
            callName = "createTopics",
            deadlineMs = deadline,
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): CreateTopicsRequest.Builder {
                return CreateTopicsRequest.Builder(
                    CreateTopicsRequestData()
                        .setTopics(topics)
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.validateOnly)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse)
                // Handle server responses for particular topics.
                val response = abstractResponse as CreateTopicsResponse
                val retryTopics = CreatableTopicCollection()
                val retryTopicQuotaExceededExceptions =
                    mutableMapOf<String, ThrottlingQuotaExceededException>()

                for (result in response.data().topics) {
                    val future = futures[result.name]
                    if (future == null) log.warn(
                        "Server response mentioned unknown topic {}",
                        result.name,
                    ) else {
                        val error = ApiError(result.errorCode, result.errorMessage)
                        if (error.isFailure) {
                            if (error.`is`(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                val quotaExceededException = ThrottlingQuotaExceededException(
                                    throttleTimeMs = response.throttleTimeMs(),
                                    message = error.messageWithFallback()
                                )
                                if (options.retryOnQuotaViolation) {
                                    retryTopics.add(topics.find(result.name)!!.duplicate())
                                    retryTopicQuotaExceededExceptions[result.name] =
                                        quotaExceededException
                                } else future.completeExceptionally(quotaExceededException)
                            } else future.completeExceptionally(error.exception()!!)
                        } else {
                            var topicMetadataAndConfig: TopicMetadataAndConfig
                            if (result.topicConfigErrorCode != Errors.NONE.code) {
                                topicMetadataAndConfig = TopicMetadataAndConfig(
                                    Errors.forCode(result.topicConfigErrorCode).exception!!
                                )
                            } else if (result.numPartitions == CreateTopicsResult.UNKNOWN) {
                                topicMetadataAndConfig = TopicMetadataAndConfig(
                                    UnsupportedVersionException(
                                        "Topic metadata and configs in CreateTopics response not supported"
                                    )
                                )
                            } else {
                                val configs = result.configs ?: emptyList()
                                val topicConfig =
                                    Config(configs.map { config -> configEntry(config) })

                                topicMetadataAndConfig = TopicMetadataAndConfig(
                                    topicId = result.topicId,
                                    numPartitions = result.numPartitions,
                                    replicationFactor = result.replicationFactor.toInt(),
                                    config = topicConfig,
                                )
                            }
                            future.complete(topicMetadataAndConfig)
                        }
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures = futures,
                        messageFormatter = { topic: String ->
                            "The controller response did not contain a result for topic $topic"
                        })
                } else {
                    val currentTime = time.milliseconds()
                    val call = getCreateTopicsCall(
                        options = options,
                        futures = futures,
                        topics = retryTopics,
                        quotaExceededExceptions = retryTopicQuotaExceededExceptions,
                        now = currentTime,
                        deadline = deadline,
                    )
                    runnable.call(call, currentTime)
                }
            }

            private fun configEntry(config: CreatableTopicConfigs): ConfigEntry {
                return ConfigEntry(
                    name = config.name,
                    value = config.value,
                    source =
                    configSource(DescribeConfigsResponse.ConfigSource.forId(config.configSource)),
                    isSensitive = config.isSensitive,
                    isReadOnly = config.readOnly,
                    synonyms = emptyList(),
                )
            }

            override fun handleFailure(throwable: Throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(
                    shouldRetryOnQuotaViolation = options.retryOnQuotaViolation,
                    throwable = throwable,
                    futures = futures,
                    quotaExceededExceptions = quotaExceededExceptions,
                    throttleTimeDelta = (time.milliseconds() - now).toInt(),
                )
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values, throwable)
            }
        }
    }

    override fun deleteTopics(
        topics: TopicCollection,
        options: DeleteTopicsOptions,
    ): DeleteTopicsResult {
        return when (topics) {
            is TopicIdCollection -> DeleteTopicsResult.ofTopicIds(
                handleDeleteTopicsUsingIds(topics.topicIds, options)
            )

            is TopicNameCollection -> DeleteTopicsResult.ofTopicNames(
                handleDeleteTopicsUsingNames(topics.topicNames, options)
            )
        }
    }

    private fun handleDeleteTopicsUsingNames(
        topicNames: Collection<String>,
        options: DeleteTopicsOptions,
    ): Map<String, KafkaFuture<Unit>> {
        val topicFutures: MutableMap<String, KafkaFutureImpl<Unit>> = HashMap(topicNames.size)
        val validTopicNames: MutableList<String> = ArrayList(topicNames.size)
        for (topicName: String in topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                val future = KafkaFutureImpl<Unit>()
                future.completeExceptionally(
                    InvalidTopicException(
                        ("The given topic name '" +
                                topicName + "' cannot be represented in a request.")
                    )
                )
                topicFutures[topicName] = future
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures[topicName] = KafkaFutureImpl()
                validTopicNames.add(topicName)
            }
        }
        if (validTopicNames.isNotEmpty()) {
            val now = time.milliseconds()
            val deadline = calcDeadlineMs(now, options.timeoutMs)
            val call = getDeleteTopicsCall(
                options = options,
                futures = topicFutures,
                topics = validTopicNames,
                quotaExceededExceptions = emptyMap(),
                now = now,
                deadline = deadline,
            )
            runnable.call(call, now)
        }
        return topicFutures.toMap()
    }

    private fun handleDeleteTopicsUsingIds(
        topicIds: Collection<Uuid>,
        options: DeleteTopicsOptions,
    ): Map<Uuid, KafkaFuture<Unit>> {
        val topicFutures: MutableMap<Uuid, KafkaFutureImpl<Unit>> = HashMap(topicIds.size)
        val validTopicIds: MutableList<Uuid> = ArrayList(topicIds.size)
        for (topicId: Uuid in topicIds) {
            if ((topicId == Uuid.ZERO_UUID)) {
                val future = KafkaFutureImpl<Unit>()
                future.completeExceptionally(
                    InvalidTopicException(
                        "The given topic ID '$topicId' cannot be represented in a request."
                    )
                )
                topicFutures[topicId] = future
            } else if (!topicFutures.containsKey(topicId)) {
                topicFutures[topicId] = KafkaFutureImpl()
                validTopicIds.add(topicId)
            }
        }
        if (validTopicIds.isNotEmpty()) {
            val now = time.milliseconds()
            val deadline = calcDeadlineMs(now, options.timeoutMs)
            val call = getDeleteTopicsWithIdsCall(
                options = options,
                futures = topicFutures,
                topicIds = validTopicIds,
                quotaExceededExceptions = emptyMap(),
                now = now,
                deadline = deadline,
            )
            runnable.call(call, now)
        }
        return topicFutures.toMap()
    }

    private fun getDeleteTopicsCall(
        options: DeleteTopicsOptions,
        futures: Map<String, KafkaFutureImpl<Unit>>,
        topics: List<String>,
        quotaExceededExceptions: Map<String, ThrottlingQuotaExceededException>,
        now: Long,
        deadline: Long,
    ): Call {
        return object : Call(
            callName = "deleteTopics",
            deadlineMs = deadline,
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DeleteTopicsRequest.Builder {
                return DeleteTopicsRequest.Builder(
                    DeleteTopicsRequestData()
                        .setTopicNames(topics)
                        .setTimeoutMs(timeoutMs)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse)
                // Handle server responses for particular topics.
                val response = abstractResponse as DeleteTopicsResponse
                val retryTopics: MutableList<String> = ArrayList()
                val retryTopicQuotaExceededExceptions =
                    mutableMapOf<String, ThrottlingQuotaExceededException>()

                for (result in response.data().responses) {
                    val resultName = result.name
                    val future = futures[resultName]
                    if (future == null || resultName == null) log.warn(
                        "Server response mentioned unknown topic {}",
                        resultName,
                    ) else {
                        val error = ApiError(result.errorCode, result.errorMessage)
                        if (error.isFailure) {
                            if (error.`is`(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                val quotaExceededException = ThrottlingQuotaExceededException(
                                    throttleTimeMs = response.throttleTimeMs(),
                                    message = error.messageWithFallback()
                                )
                                if (options.retryOnQuotaViolation) {
                                    retryTopics.add(resultName)
                                    retryTopicQuotaExceededExceptions[resultName] =
                                        quotaExceededException
                                } else future.completeExceptionally(quotaExceededException)
                            } else future.completeExceptionally(error.exception()!!)
                        } else future.complete(Unit)
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures = futures,
                        messageFormatter = { topic: String ->
                            "The controller response did not contain a result for topic $topic"
                        })
                } else {
                    val currentTime = time.milliseconds()
                    val call = getDeleteTopicsCall(
                        options = options,
                        futures = futures,
                        topics = retryTopics,
                        quotaExceededExceptions = retryTopicQuotaExceededExceptions,
                        now = currentTime,
                        deadline = deadline,
                    )
                    runnable.call(call, currentTime)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(
                    options.retryOnQuotaViolation,
                    throwable, futures, quotaExceededExceptions, (time.milliseconds() - now).toInt()
                )
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values, throwable)
            }
        }
    }

    private fun getDeleteTopicsWithIdsCall(
        options: DeleteTopicsOptions,
        futures: Map<Uuid, KafkaFutureImpl<Unit>>,
        topicIds: List<Uuid>,
        quotaExceededExceptions: Map<Uuid, ThrottlingQuotaExceededException>,
        now: Long,
        deadline: Long,
    ): Call {
        return object : Call(
            callName = "deleteTopics",
            deadlineMs = deadline,
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DeleteTopicsRequest.Builder {
                return DeleteTopicsRequest.Builder(
                    DeleteTopicsRequestData()
                        .setTopics(
                            topicIds.map { topic -> DeleteTopicState().setTopicId(topic) }
                        )
                        .setTimeoutMs(timeoutMs)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse)
                // Handle server responses for particular topics.
                val response = abstractResponse as DeleteTopicsResponse
                val retryTopics: MutableList<Uuid> = ArrayList()
                val retryTopicQuotaExceededExceptions =
                    mutableMapOf<Uuid, ThrottlingQuotaExceededException>()

                for (result in response.data().responses) {
                    val future = futures[result.topicId]
                    if (future == null) log.warn(
                        "Server response mentioned unknown topic ID {}",
                        result.topicId,
                    ) else {
                        val error = ApiError(result.errorCode, result.errorMessage)
                        if (error.isFailure) {
                            if (error.`is`(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                val quotaExceededException = ThrottlingQuotaExceededException(
                                    throttleTimeMs = response.throttleTimeMs(),
                                    message = error.messageWithFallback(),
                                )
                                if (options.retryOnQuotaViolation) {
                                    retryTopics.add(result.topicId)
                                    retryTopicQuotaExceededExceptions[result.topicId] =
                                        quotaExceededException
                                } else future.completeExceptionally(quotaExceededException)
                            } else future.completeExceptionally(error.exception()!!)
                        } else future.complete(Unit)
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures = futures,
                        messageFormatter = { topic: Uuid ->
                            "The controller response did not contain a result for topic $topic"
                        })
                } else {
                    val currentTime = time.milliseconds()
                    val call = getDeleteTopicsWithIdsCall(
                        options = options,
                        futures = futures,
                        topicIds = retryTopics,
                        quotaExceededExceptions = retryTopicQuotaExceededExceptions,
                        now = currentTime,
                        deadline = deadline,
                    )
                    runnable.call(call, currentTime)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(
                    options.retryOnQuotaViolation,
                    throwable, futures, quotaExceededExceptions, (time.milliseconds() - now).toInt()
                )
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values, throwable)
            }
        }
    }

    override fun listTopics(options: ListTopicsOptions): ListTopicsResult {
        val topicListingFuture = KafkaFutureImpl<Map<String, TopicListing>>()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "listTopics",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): MetadataRequest.Builder {
                return MetadataRequest.Builder.allTopics()
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as MetadataResponse
                val topicListing: MutableMap<String, TopicListing> = HashMap()
                for (topicMetadata: MetadataResponse.TopicMetadata in response.topicMetadata()) {
                    val topicName = topicMetadata.topic
                    val isInternal = topicMetadata.isInternal
                    if (!topicMetadata.isInternal || options.shouldListInternal)
                        topicListing[topicName] = TopicListing(
                            name = topicName,
                            topicId = topicMetadata.topicId,
                            isInternal = isInternal,
                        )
                }
                topicListingFuture.complete(topicListing)
            }

            override fun handleFailure(throwable: Throwable) {
                topicListingFuture.completeExceptionally(throwable)
            }
        }, now)
        return ListTopicsResult(topicListingFuture)
    }

    override fun describeTopics(
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ): DescribeTopicsResult {
        return when (topics) {
            is TopicIdCollection -> DescribeTopicsResult.ofTopicIds(
                handleDescribeTopicsByIds(topics.topicIds, options)
            )

            is TopicNameCollection -> DescribeTopicsResult.ofTopicNames(
                handleDescribeTopicsByNames(topics.topicNames, options)
            )
        }
    }

    private fun handleDescribeTopicsByNames(
        topicNames: Collection<String>,
        options: DescribeTopicsOptions,
    ): Map<String, KafkaFuture<TopicDescription>> {
        val topicFutures: MutableMap<String, KafkaFutureImpl<TopicDescription>> = HashMap(topicNames.size)
        val topicNamesList = mutableListOf<String>()

        for (topicName in topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(
                    InvalidTopicException(
                        "The given topic name '$topicName' cannot be represented in a request."
                    )
                )
                topicFutures[topicName] = future
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures[topicName] = KafkaFutureImpl()
                topicNamesList.add(topicName)
            }
        }
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "describeTopics",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            private var supportsDisablingTopicCreation = true
            override fun createRequest(timeoutMs: Int): MetadataRequest.Builder {
                return if (supportsDisablingTopicCreation) MetadataRequest.Builder(
                    MetadataRequestData()
                        .setTopics(MetadataRequest.convertToMetadataRequestTopic(topicNamesList))
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations)
                ) else MetadataRequest.Builder.allTopics()
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as MetadataResponse
                // Handle server responses for particular topics.
                val cluster = response.buildCluster()
                val errors = response.errors()
                for ((topicName, future) in topicFutures) {
                    val topicError = errors[topicName]
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception!!)
                        continue
                    }
                    if (!cluster.topics.contains(topicName)) {
                        future.completeExceptionally(UnknownTopicOrPartitionException("Topic $topicName not found."))
                        continue
                    }
                    val topicId = cluster.topicId(topicName)
                    val authorizedOperations = response.topicAuthorizedOperations(topicName)
                    val topicDescription =
                        getTopicDescriptionFromCluster(
                            cluster,
                            topicName,
                            topicId,
                            authorizedOperations
                        )
                    future.complete(topicDescription)
                }
            }

            override fun handleUnsupportedVersionException(
                exception: UnsupportedVersionException,
            ): Boolean {
                if (supportsDisablingTopicCreation) {
                    supportsDisablingTopicCreation = false
                    return true
                }
                return false
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(topicFutures.values, throwable)
        }
        if (topicNamesList.isNotEmpty()) runnable.call(call, now)

        return topicFutures.toMap()
    }

    private fun handleDescribeTopicsByIds(
        topicIds: Collection<Uuid>,
        options: DescribeTopicsOptions,
    ): Map<Uuid, KafkaFuture<TopicDescription>> {
        val topicFutures: MutableMap<Uuid, KafkaFutureImpl<TopicDescription>> =
            HashMap(topicIds.size)
        val topicIdsList = mutableListOf<Uuid>()

        for (topicId in topicIds) {
            if (topicIdIsUnrepresentable(topicId)) {
                val future = KafkaFutureImpl<TopicDescription>()
                future.completeExceptionally(
                    InvalidTopicException(
                        "The given topic id '$topicId' cannot be represented in a request."
                    )
                )
                topicFutures[topicId] = future
            } else if (!topicFutures.containsKey(topicId)) {
                topicFutures[topicId] = KafkaFutureImpl()
                topicIdsList.add(topicId)
            }
        }
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "describeTopicsWithIds",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): MetadataRequest.Builder {
                return MetadataRequest.Builder(
                    MetadataRequestData()
                        .setTopics(
                            MetadataRequest.convertTopicIdsToMetadataRequestTopic(topicIdsList)
                        )
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as MetadataResponse
                // Handle server responses for particular topics.
                val cluster = response.buildCluster()
                val errors = response.errorsByTopicId()
                for ((topicId, future) in topicFutures) {
                    val topicName = cluster.topicName(topicId)
                    if (topicName == null) {
                        future.completeExceptionally(
                            InvalidTopicException("TopicId $topicId not found.")
                        )
                        continue
                    }
                    val topicError = errors[topicId]
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception!!)
                        continue
                    }
                    val authorizedOperations = response.topicAuthorizedOperations(topicName)
                    val topicDescription =
                        getTopicDescriptionFromCluster(
                            cluster,
                            topicName,
                            topicId,
                            authorizedOperations
                        )
                    future.complete(topicDescription)
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(topicFutures.values, throwable)
        }
        if (topicIdsList.isNotEmpty()) {
            runnable.call(call, now)
        }
        return topicFutures.toMap()
    }

    private fun getTopicDescriptionFromCluster(
        cluster: Cluster, topicName: String, topicId: Uuid,
        authorizedOperations: Int,
    ): TopicDescription {
        val isInternal = cluster.internalTopics.contains(topicName)
        val partitionInfos = cluster.partitionsForTopic(topicName)
        val partitions = partitionInfos.map { partitionInfo ->
            TopicPartitionInfo(
                partition = partitionInfo.partition,
                leader = leader(partitionInfo),
                replicas = partitionInfo.replicas,
                inSyncReplicas = partitionInfo.inSyncReplicas
            )
        }.sortedBy { it.partition }

        return TopicDescription(
            name = topicName,
            internal = isInternal,
            partitions = partitions,
            authorizedOperations = validAclOperations(authorizedOperations),
            topicId = topicId
        )
    }

    private fun leader(partitionInfo: PartitionInfo): Node? {
        return if (partitionInfo.leader == null || partitionInfo.leader.id == Node.noNode().id) null
        else partitionInfo.leader
    }

    override fun describeCluster(options: DescribeClusterOptions): DescribeClusterResult {
        val describeClusterFuture = KafkaFutureImpl<Collection<Node>>()
        val controllerFuture = KafkaFutureImpl<Node?>()
        val clusterIdFuture = KafkaFutureImpl<String>()
        val authorizedOperationsFuture = KafkaFutureImpl<Set<AclOperation>?>()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "listNodes",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            private var useMetadataRequest = false
            override fun createRequest(timeoutMs: Int): AbstractRequest.Builder<*> {
                return if (!useMetadataRequest) {
                    DescribeClusterRequest.Builder(
                        DescribeClusterRequestData()
                            .setIncludeClusterAuthorizedOperations(
                                options.includeAuthorizedOperations()
                            )
                    )
                } else {
                    // Since this only requests node information, it's safe to pass true for allowAutoTopicCreation (and it
                    // simplifies communication with older brokers)
                    MetadataRequest.Builder(
                        MetadataRequestData()
                            .setTopics(emptyList())
                            .setAllowAutoTopicCreation(true)
                            .setIncludeClusterAuthorizedOperations(
                                options.includeAuthorizedOperations()
                            )
                    )
                }
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                if (!useMetadataRequest) {
                    val response = abstractResponse as DescribeClusterResponse
                    val error = Errors.forCode(response.data().errorCode)
                    if (error != Errors.NONE) {
                        val apiError = ApiError(error, response.data().errorMessage)
                        handleFailure(apiError.exception()!!)
                        return
                    }
                    val nodes = response.nodes()
                    describeClusterFuture.complete(nodes.values)
                    // Controller is null if controller id is equal to NO_CONTROLLER_ID
                    controllerFuture.complete(nodes[response.data().controllerId])
                    clusterIdFuture.complete(response.data().clusterId)
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.data().clusterAuthorizedOperations)
                    )
                } else {
                    val response = abstractResponse as MetadataResponse
                    describeClusterFuture.complete(response.brokers())
                    controllerFuture.complete(controller(response))
                    clusterIdFuture.complete(response.clusterId!!)
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.clusterAuthorizedOperations())
                    )
                }
            }

            private fun controller(response: MetadataResponse): Node? {
                return if (response.controller == null
                    || response.controller.id == MetadataResponse.NO_CONTROLLER_ID
                ) null else response.controller
            }

            override fun handleFailure(throwable: Throwable) {
                describeClusterFuture.completeExceptionally(throwable)
                controllerFuture.completeExceptionally(throwable)
                clusterIdFuture.completeExceptionally(throwable)
                authorizedOperationsFuture.completeExceptionally(throwable)
            }

            override fun handleUnsupportedVersionException(
                exception: UnsupportedVersionException,
            ): Boolean {
                if (useMetadataRequest) return false
                useMetadataRequest = true
                return true
            }
        }, now)

        return DescribeClusterResult(
            describeClusterFuture, controllerFuture, clusterIdFuture,
            authorizedOperationsFuture
        )
    }

    override fun describeAcls(
        filter: AclBindingFilter,
        options: DescribeAclsOptions,
    ): DescribeAclsResult {
        if (filter.isUnknown) {
            val future = KafkaFutureImpl<Collection<AclBinding>>()
            future.completeExceptionally(
                InvalidRequestException(
                    "The AclBindingFilter " +
                            "must not contain UNKNOWN elements."
                )
            )
            return DescribeAclsResult(future)
        }
        val now = time.milliseconds()
        val future = KafkaFutureImpl<Collection<AclBinding>>()
        runnable.call(object : Call(
            callName = "describeAcls",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DescribeAclsRequest.Builder {
                return DescribeAclsRequest.Builder(filter)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as DescribeAclsResponse
                if (response.error.isFailure)
                    future.completeExceptionally(response.error.exception()!!)
                else future.complete(DescribeAclsResponse.aclBindings(response.acls))
            }

            override fun handleFailure(throwable: Throwable) {
                future.completeExceptionally(throwable)
            }
        }, now)

        return DescribeAclsResult(future)
    }

    override fun createAcls(
        acls: Collection<AclBinding>,
        options: CreateAclsOptions,
    ): CreateAclsResult {
        val now = time.milliseconds()
        val futures: MutableMap<AclBinding, KafkaFutureImpl<Unit>> = HashMap()
        val aclCreations: MutableList<AclCreation> = ArrayList()
        val aclBindingsSent: MutableList<AclBinding> = ArrayList()
        for (acl: AclBinding in acls) {
            if (futures[acl] == null) {
                val future = KafkaFutureImpl<Unit>()
                futures[acl] = future
                val indefinite = acl.toFilter().findIndefiniteField()
                if (indefinite == null) {
                    aclCreations.add(CreateAclsRequest.aclCreation(acl))
                    aclBindingsSent.add(acl)
                } else future.completeExceptionally(
                    InvalidRequestException("Invalid ACL creation: $indefinite")
                )
            }
        }
        val data = CreateAclsRequestData().setCreations(aclCreations)
        runnable.call(object : Call(
            callName = "createAcls",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): CreateAclsRequest.Builder {
                return CreateAclsRequest.Builder(data)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as CreateAclsResponse
                val responses = response.results()
                val iter: Iterator<AclCreationResult> = responses.iterator()

                aclBindingsSent.forEach { aclBinding ->
                    val future = futures[aclBinding]!!
                    if (!iter.hasNext()) {
                        future.completeExceptionally(
                            UnknownServerException(
                                "The broker reported no creation result for the given ACL: $aclBinding"
                            )
                        )
                    } else {
                        val creation = iter.next()
                        val error = Errors.forCode(creation.errorCode)
                        val apiError = ApiError(error, creation.errorMessage)
                        if (apiError.isFailure) future.completeExceptionally(apiError.exception()!!)
                        else future.complete(Unit)
                    }
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }, now)
        return CreateAclsResult(futures.toMap())
    }

    override fun deleteAcls(
        filters: Collection<AclBindingFilter>,
        options: DeleteAclsOptions,
    ): DeleteAclsResult {
        val now = time.milliseconds()
        val futures = mutableMapOf<AclBindingFilter, KafkaFutureImpl<FilterResults>>()
        val aclBindingFiltersSent = mutableListOf<AclBindingFilter>()
        val deleteAclsFilters = mutableListOf<DeleteAclsFilter>()


        for (filter in filters) {
            if (futures[filter] == null) {
                aclBindingFiltersSent.add(filter)
                deleteAclsFilters.add(DeleteAclsRequest.deleteAclsFilter(filter))
                futures[filter] = KafkaFutureImpl()
            }
        }

        val data = DeleteAclsRequestData().setFilters(deleteAclsFilters)
        runnable.call(object : Call(
            callName = "deleteAcls",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {

            override fun createRequest(timeoutMs: Int): DeleteAclsRequest.Builder =
                DeleteAclsRequest.Builder(data)

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as DeleteAclsResponse
                val results = response.filterResults()
                val iter: Iterator<DeleteAclsFilterResult> = results.iterator()
                for (bindingFilter in aclBindingFiltersSent) {
                    val future = futures[bindingFilter]!!
                    if (!iter.hasNext()) {
                        future.completeExceptionally(
                            UnknownServerException(
                                "The broker reported no deletion result for the given filter."
                            )
                        )
                    } else {
                        val filterResult = iter.next()
                        val error = ApiError(
                            Errors.forCode(filterResult.errorCode),
                            filterResult.errorMessage
                        )
                        if (error.isFailure) future.completeExceptionally(error.exception()!!)
                        else {
                            val filterResults = mutableListOf<DeleteAclsResult.FilterResult>()
                            for (matchingAcl in filterResult.matchingAcls) {
                                val aclError = ApiError(
                                    error = Errors.forCode(matchingAcl.errorCode),
                                    message = matchingAcl.errorMessage,
                                )
                                val aclBinding = DeleteAclsResponse.aclBinding(matchingAcl)
                                filterResults.add(
                                    DeleteAclsResult.FilterResult(
                                        binding = aclBinding,
                                        exception = aclError.exception(),
                                    )
                                )
                            }
                            future.complete(FilterResults(filterResults))
                        }
                    }
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }, now)
        return DeleteAclsResult(futures.toMap())
    }

    override fun describeConfigs(
        resources: Collection<ConfigResource>,
        options: DescribeConfigsOptions,
    ): DescribeConfigsResult {
        // Partition the requested config resources based on which broker they must be sent to with the
        // null broker being used for config resources which can be obtained from any broker
        val brokerFutures: MutableMap<Int?, MutableMap<ConfigResource, KafkaFutureImpl<Config>>> =
            HashMap(resources.size)
        for (resource: ConfigResource in resources) {
            val broker = nodeFor(resource)
            brokerFutures.compute(broker) { _, value ->
                val v = value ?: mutableMapOf()
                v[resource] = KafkaFutureImpl()
                v
            }
        }
        val now = time.milliseconds()
        for ((broker, unified) in brokerFutures) {
            runnable.call(
                object : Call(
                    callName = "describeConfigs",
                    deadlineMs = calcDeadlineMs(now, options.timeoutMs),
                    nodeProvider = broker?.let { ConstantNodeIdProvider(it) }
                        ?: LeastLoadedNodeProvider(),
                ) {
                    override fun createRequest(timeoutMs: Int): DescribeConfigsRequest.Builder {
                        return DescribeConfigsRequest.Builder(
                            DescribeConfigsRequestData()
                                .setResources(
                                    unified.keys.map { config: ConfigResource ->
                                        DescribeConfigsResource()
                                            .setResourceName(config.name)
                                            .setResourceType(config.type.id)
                                            .setConfigurationKeys(emptyList())
                                    }
                                )
                                .setIncludeSynonyms(options.includeSynonyms)
                                .setIncludeDocumentation(options.includeDocumentation)
                        )
                    }

                    override fun handleResponse(abstractResponse: AbstractResponse) {
                        val response = abstractResponse as DescribeConfigsResponse
                        for ((configResource, describeConfigsResult) in response.resultMap()) {
                            val future = unified[configResource]
                            if (future == null) {
                                if (broker != null) log.warn(
                                    "The config {} in the response from broker {} is not in the request",
                                    configResource,
                                    broker,
                                ) else log.warn(
                                    "The config {} in the response from the least loaded broker is not in the request",
                                    configResource
                                )
                            } else {
                                if (describeConfigsResult.errorCode != Errors.NONE.code) {
                                    future.completeExceptionally(
                                        Errors.forCode(describeConfigsResult.errorCode)
                                            .exception(describeConfigsResult.errorMessage)!!
                                    )
                                } else future.complete(describeConfigResult(describeConfigsResult))
                            }
                        }
                        completeUnrealizedFutures(
                            futures = unified,
                            messageFormatter = { configResource: ConfigResource ->
                                "The broker response did not contain a result for config resource $configResource"
                            })
                    }

                    override fun handleFailure(throwable: Throwable) =
                        completeAllExceptionally(unified.values, throwable)
                }, now
            )
        }

        return DescribeConfigsResult(
            brokerFutures
                .flatMap { it.value.entries }
                .associate { it.key to it.value }
                .toMap()
        )
    }

    private fun describeConfigResult(describeConfigsResult: DescribeConfigsResponseData.DescribeConfigsResult): Config {
        return Config(
            describeConfigsResult.configs.map { config: DescribeConfigsResourceResult ->
                ConfigEntry(
                    config.name,
                    config.value,
                    DescribeConfigsResponse.ConfigSource.forId(config.configSource).source,
                    config.isSensitive,
                    config.readOnly,
                    config.synonyms.map { synonym: DescribeConfigsSynonym ->
                        ConfigEntry.ConfigSynonym(
                            synonym.name,
                            synonym.value,
                            DescribeConfigsResponse.ConfigSource
                                .forId(synonym.source)
                                .source
                        )
                    },
                    DescribeConfigsResponse.ConfigType.forId(config.configType).type,
                    config.documentation
                )
            }
        )
    }

    private fun configSource(source: DescribeConfigsResponse.ConfigSource): ConfigEntry.ConfigSource {
        return when (source) {
            DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG -> ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG

            DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG -> ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG

            DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG ->
                ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG

            DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG -> ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG

            DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG ->
                ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG

            DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG -> ConfigEntry.ConfigSource.DEFAULT_CONFIG

            else -> throw IllegalArgumentException("Unexpected config source $source")
        }
    }

    @Deprecated("")
    override fun alterConfigs(
        configs: Map<ConfigResource, Config>,
        options: AlterConfigsOptions,
    ): AlterConfigsResult {
        val allFutures: MutableMap<ConfigResource, KafkaFutureImpl<Unit>> = HashMap()
        // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        // and send the request to that specific broker. Other resources are grouped together into
        // a single request that may be sent to any broker.
        val unifiedRequestResources: MutableCollection<ConfigResource> = ArrayList()
        for (resource: ConfigResource in configs.keys) {
            val node = nodeFor(resource)
            if (node != null) {
                val nodeProvider = ConstantNodeIdProvider(node)
                allFutures.putAll(
                    alterConfigs(
                        configs = configs,
                        options = options,
                        resources = setOf(resource),
                        nodeProvider = nodeProvider,
                    )
                )
            } else unifiedRequestResources.add(resource)
        }
        if (!unifiedRequestResources.isEmpty()) allFutures.putAll(
            alterConfigs(
                configs = configs,
                options = options,
                resources = unifiedRequestResources,
                nodeProvider = LeastLoadedNodeProvider()
            )
        )
        return AlterConfigsResult(allFutures.toMap())
    }

    private fun alterConfigs(
        configs: Map<ConfigResource, Config>,
        options: AlterConfigsOptions,
        resources: Collection<ConfigResource>,
        nodeProvider: NodeProvider,
    ): Map<ConfigResource, KafkaFutureImpl<Unit>> {
        val futures = mutableMapOf<ConfigResource, KafkaFutureImpl<Unit>>()
        val requestMap: MutableMap<ConfigResource, AlterConfigsRequest.Config> = HashMap(resources.size)
        for (resource in resources) {
            val configEntries = mutableListOf<AlterConfigsRequest.ConfigEntry>()
            for (configEntry in configs[resource]!!.entries()) configEntries.add(
                AlterConfigsRequest.ConfigEntry(
                    name = configEntry.name,
                    value = configEntry.value,
                )
            )
            requestMap[resource] = AlterConfigsRequest.Config(configEntries)
            futures[resource] = KafkaFutureImpl()
        }
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "alterConfigs",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = nodeProvider,
        ) {
            override fun createRequest(timeoutMs: Int): AlterConfigsRequest.Builder {
                return AlterConfigsRequest.Builder(requestMap, options.validateOnly)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as AlterConfigsResponse
                for ((key, future) in futures) {
                    val exception = response.errors()[key]?.exception()
                    if (exception != null) future.completeExceptionally(exception)
                    else future.complete(Unit)
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }, now)
        return futures
    }

    override fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ): AlterConfigsResult {
        val allFutures: MutableMap<ConfigResource, KafkaFutureImpl<Unit>> = HashMap()
        // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        // and send the request to that specific broker. Other resources are grouped together into
        // a single request that may be sent to any broker.
        val unifiedRequestResources: MutableCollection<ConfigResource> = ArrayList()
        for (resource in configs.keys) {
            val node = nodeFor(resource)
            if (node != null) {
                val nodeProvider: NodeProvider = ConstantNodeIdProvider(node)
                allFutures.putAll(
                    incrementalAlterConfigs(
                        configs = configs,
                        options = options,
                        resources = setOf(resource),
                        nodeProvider = nodeProvider,
                    )
                )
            } else unifiedRequestResources.add(resource)
        }
        if (!unifiedRequestResources.isEmpty()) allFutures.putAll(
            incrementalAlterConfigs(
                configs = configs,
                options = options,
                resources = unifiedRequestResources,
                nodeProvider = LeastLoadedNodeProvider()
            )
        )
        return AlterConfigsResult(allFutures.toMap())
    }

    private fun incrementalAlterConfigs(
        configs: Map<ConfigResource, Collection<AlterConfigOp>>,
        options: AlterConfigsOptions,
        resources: Collection<ConfigResource>,
        nodeProvider: NodeProvider,
    ): Map<ConfigResource, KafkaFutureImpl<Unit>> {
        val futures: MutableMap<ConfigResource, KafkaFutureImpl<Unit>> = HashMap()
        for (resource: ConfigResource in resources) futures[resource] = KafkaFutureImpl()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "incrementalAlterConfigs",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = nodeProvider,
        ) {
            override fun createRequest(timeoutMs: Int): IncrementalAlterConfigsRequest.Builder {
                return IncrementalAlterConfigsRequest.Builder(
                    resources = resources,
                    configs = configs,
                    validateOnly = options.validateOnly,
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as IncrementalAlterConfigsResponse
                val errors = IncrementalAlterConfigsResponse.fromResponseData(response.data())

                futures.forEach { (key, future) ->
                    val exception = errors[key]?.exception()

                    if (exception != null) future.completeExceptionally(exception)
                    else future.complete(Unit)
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }, now)
        return futures
    }

    override fun alterReplicaLogDirs(
        replicaAssignment: Map<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ): AlterReplicaLogDirsResult {
        val futures: MutableMap<TopicPartitionReplica, KafkaFutureImpl<Unit>> =
            HashMap(replicaAssignment.size)
        for (replica: TopicPartitionReplica in replicaAssignment.keys)
            futures[replica] = KafkaFutureImpl()
        val replicaAssignmentByBroker: MutableMap<Int, AlterReplicaLogDirsRequestData> = HashMap()
        for ((replica, logDir) in replicaAssignment) {
            val value = replicaAssignmentByBroker.computeIfAbsent(replica.brokerId) {
                AlterReplicaLogDirsRequestData()
            }
            var alterReplicaLogDir = value.dirs.find(logDir)
            if (alterReplicaLogDir == null) {
                alterReplicaLogDir = AlterReplicaLogDir()
                alterReplicaLogDir.setPath(logDir)
                value.dirs.add(alterReplicaLogDir)
            }
            var alterReplicaLogDirTopic = alterReplicaLogDir.topics.find(replica.topic)
            if (alterReplicaLogDirTopic == null) {
                alterReplicaLogDirTopic = AlterReplicaLogDirTopic().setName(replica.topic)
                alterReplicaLogDir.topics.add(alterReplicaLogDirTopic)
            }
            alterReplicaLogDirTopic.partitions += replica.partition
        }
        val now = time.milliseconds()

        for ((brokerId, assignment) in replicaAssignmentByBroker) {
            runnable.call(object : Call(
                callName = "alterReplicaLogDirs",
                deadlineMs = calcDeadlineMs(now, options.timeoutMs),
                nodeProvider = ConstantNodeIdProvider(brokerId),
            ) {
                override fun createRequest(timeoutMs: Int): AlterReplicaLogDirsRequest.Builder {
                    return AlterReplicaLogDirsRequest.Builder(assignment)
                }

                override fun handleResponse(abstractResponse: AbstractResponse) {
                    val response = abstractResponse as AlterReplicaLogDirsResponse

                    response.data().results.forEach { topicResult ->
                        topicResult.partitions.forEach { partitionResult ->
                            val replica = TopicPartitionReplica(
                                topic = topicResult.topicName,
                                partition = partitionResult.partitionIndex,
                                brokerId = brokerId
                            )
                            val future = futures[replica]
                            if (future == null) log.warn(
                                "The partition {} in the response from broker {} is not in the request",
                                TopicPartition(
                                    topic = topicResult.topicName,
                                    partition = partitionResult.partitionIndex
                                ),
                                brokerId
                            )
                            else if (partitionResult.errorCode == Errors.NONE.code)
                                future.complete(Unit)
                            else future.completeExceptionally(
                                Errors.forCode(partitionResult.errorCode).exception!!
                            )
                        }
                    }
                    // The server should send back a response for every replica. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures = futures.filterKeys { key -> key.brokerId == brokerId },
                        messageFormatter = { replica ->
                            "The response from broker $brokerId did not contain a result for replica $replica"
                        })
                }

                override fun handleFailure(throwable: Throwable) {
                    // Only completes the futures of brokerId
                    completeAllExceptionally(
                        futures = futures.filterKeys { key -> key.brokerId == brokerId },
                        exception = throwable,
                    )
                }
            }, now)
        }
        return AlterReplicaLogDirsResult(futures.toMap())
    }

    override fun describeLogDirs(
        brokers: Collection<Int>,
        options: DescribeLogDirsOptions,
    ): DescribeLogDirsResult {
        val futures: MutableMap<Int, KafkaFutureImpl<Map<String, LogDirDescription>>> =
            HashMap(brokers.size)
        val now = time.milliseconds()
        for (brokerId: Int in brokers) {
            val future = KafkaFutureImpl<Map<String, LogDirDescription>>()
            futures[brokerId] = future
            runnable.call(object : Call(
                callName = "describeLogDirs",
                deadlineMs = calcDeadlineMs(now, options.timeoutMs),
                nodeProvider = ConstantNodeIdProvider(brokerId),
            ) {
                override fun createRequest(timeoutMs: Int): DescribeLogDirsRequest.Builder {
                    // Query selected partitions in all log directories
                    return DescribeLogDirsRequest.Builder(DescribeLogDirsRequestData().setTopics(null))
                }

                override fun handleResponse(abstractResponse: AbstractResponse) {
                    val response = abstractResponse as DescribeLogDirsResponse
                    val descriptions = logDirDescriptions(response)
                    if (descriptions.isNotEmpty()) future.complete(descriptions)
                    else {
                        // Up to v3 DescribeLogDirsResponse did not have an error code field, hence it defaults to None
                        val error = if (response.data().errorCode == Errors.NONE.code)
                            Errors.CLUSTER_AUTHORIZATION_FAILED
                        else Errors.forCode(response.data().errorCode)
                        future.completeExceptionally(error.exception!!)
                    }
                }

                override fun handleFailure(throwable: Throwable) {
                    future.completeExceptionally(throwable)
                }
            }, now)
        }
        return DescribeLogDirsResult(futures.toMap())
    }

    override fun describeReplicaLogDirs(
        replicas: Collection<TopicPartitionReplica>,
        options: DescribeReplicaLogDirsOptions,
    ): DescribeReplicaLogDirsResult {
        val futures = replicas.associateBy(
            keySelector = { it },
            valueTransform = { KafkaFutureImpl<ReplicaLogDirInfo>() }
        )

        val partitionsByBroker: MutableMap<Int, DescribeLogDirsRequestData> = HashMap()

        replicas.forEach { replica ->
            val requestData = partitionsByBroker.computeIfAbsent(replica.brokerId) {
                DescribeLogDirsRequestData()
            }
            var describableLogDirTopic = requestData.topics?.find(replica.topic)
            if (describableLogDirTopic == null) {
                val partitions = mutableListOf<Int>()
                partitions.add(replica.partition)
                describableLogDirTopic = DescribableLogDirTopic()
                    .setTopic(replica.topic)
                    .setPartitions(partitions.toIntArray())
                requestData.topics!!.add(describableLogDirTopic)
            } else describableLogDirTopic.partitions += replica.partition
        }

        val now = time.milliseconds()
        partitionsByBroker.forEach { (brokerId, topicPartitions) ->
            val replicaDirInfoByPartition = mutableMapOf<TopicPartition, ReplicaLogDirInfo>()

            topicPartitions.topics!!.forEach { topicPartition ->
                topicPartition.partitions.forEach { partitionId ->
                    replicaDirInfoByPartition[
                        TopicPartition(
                            topic = topicPartition.topic,
                            partition = partitionId,
                        )
                    ] = ReplicaLogDirInfo()
                }
            }

            runnable.call(object : Call(
                callName = "describeReplicaLogDirs",
                deadlineMs = calcDeadlineMs(now, options.timeoutMs),
                nodeProvider = ConstantNodeIdProvider(brokerId),
            ) {
                override fun createRequest(timeoutMs: Int): DescribeLogDirsRequest.Builder {
                    // Query selected partitions in all log directories
                    return DescribeLogDirsRequest.Builder(topicPartitions)
                }

                override fun handleResponse(abstractResponse: AbstractResponse) {
                    val response = abstractResponse as DescribeLogDirsResponse

                    logDirDescriptions(response).forEach innerLoop@{ (logDir, logDirInfo) ->

                        val error = logDirInfo.error()
                        // No replica info will be provided if the log directory is offline
                        if (error is KafkaStorageException) return@innerLoop
                        if (error != null) handleFailure(
                            IllegalStateException(
                                "The error ${error.javaClass.name} for log directory $logDir in " +
                                        "the response from broker $brokerId is illegal"
                            )
                        )

                        logDirInfo.replicaInfos().forEach { (tp, replicaInfo) ->

                            val replicaLogDirInfo = replicaDirInfoByPartition[tp]
                            if (replicaLogDirInfo == null) {
                                log.warn(
                                    "Server response from broker {} mentioned unknown partition {}",
                                    brokerId,
                                    tp,
                                )
                            } else if (replicaInfo.isFuture) {
                                replicaDirInfoByPartition[tp] = ReplicaLogDirInfo(
                                    replicaLogDirInfo.currentReplicaLogDir,
                                    replicaLogDirInfo.currentReplicaOffsetLag,
                                    logDir,
                                    replicaInfo.offsetLag,
                                )
                            } else {
                                replicaDirInfoByPartition[tp] = ReplicaLogDirInfo(
                                    logDir,
                                    replicaInfo.offsetLag,
                                    replicaLogDirInfo.futureReplicaLogDir,
                                    replicaLogDirInfo.futureReplicaOffsetLag,
                                )
                            }
                        }
                    }
                    replicaDirInfoByPartition.forEach { (key, value) ->
                        futures[TopicPartitionReplica(
                            topic = key.topic,
                            partition = key.partition,
                            brokerId = brokerId,
                        )]?.complete(value)
                    }
                }

                override fun handleFailure(throwable: Throwable) =
                    completeAllExceptionally(futures.values, throwable)
            }, now)
        }
        return DescribeReplicaLogDirsResult(futures.toMap())
    }

    override fun createPartitions(
        newPartitions: Map<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ): CreatePartitionsResult {
        val futures: MutableMap<String, KafkaFutureImpl<Unit>> = HashMap(newPartitions.size)
        val topics = CreatePartitionsTopicCollection(newPartitions.size)

        newPartitions.forEach { (topic, newPartition) ->
            val assignments = newPartition.newAssignments.map { brokerIds ->
                CreatePartitionsAssignment().setBrokerIds(brokerIds.toIntArray())
            }
            topics.add(
                CreatePartitionsTopic()
                    .setName(topic)
                    .setCount(newPartition.totalCount)
                    .setAssignments(assignments)
            )
            futures[topic] = KafkaFutureImpl()
        }
        if (!topics.isEmpty()) {
            val now = time.milliseconds()
            val deadline = calcDeadlineMs(now, options.timeoutMs)
            val call = getCreatePartitionsCall(
                options = options,
                futures = futures,
                topics = topics,
                quotaExceededExceptions = emptyMap(),
                now = now,
                deadline = deadline,
            )
            runnable.call(call, now)
        }
        return CreatePartitionsResult(futures.toMap())
    }

    private fun getCreatePartitionsCall(
        options: CreatePartitionsOptions,
        futures: Map<String, KafkaFutureImpl<Unit>>,
        topics: CreatePartitionsTopicCollection,
        quotaExceededExceptions: Map<String, ThrottlingQuotaExceededException>,
        now: Long,
        deadline: Long,
    ): Call {
        return object : Call(
            callName = "createPartitions",
            deadlineMs = deadline,
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): CreatePartitionsRequest.Builder {
                return CreatePartitionsRequest.Builder(
                    CreatePartitionsRequestData()
                        .setTopics(topics)
                        .setValidateOnly(options.validateOnly)
                        .setTimeoutMs(timeoutMs)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse)
                // Handle server responses for particular topics.
                val response = abstractResponse as CreatePartitionsResponse
                val retryTopics = CreatePartitionsTopicCollection()
                val retryTopicQuotaExceededExceptions =
                    mutableMapOf<String, ThrottlingQuotaExceededException>()

                for (result in response.data().results) {
                    val future = futures[result.name]
                    if (future == null)
                        log.warn("Server response mentioned unknown topic {}", result.name)
                    else {
                        val error = ApiError(result.errorCode, result.errorMessage)
                        if (error.isFailure) {
                            if (error.`is`(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                val quotaExceededException = ThrottlingQuotaExceededException(
                                    throttleTimeMs = response.throttleTimeMs(),
                                    message = error.messageWithFallback()
                                )
                                if (options.retryOnQuotaViolation) {
                                    retryTopics.add(topics.find(result.name)!!.duplicate())
                                    retryTopicQuotaExceededExceptions[result.name] =
                                        quotaExceededException
                                } else future.completeExceptionally(quotaExceededException)
                            } else future.completeExceptionally(error.exception()!!)
                        } else future.complete(Unit)
                    }
                }

                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures = futures,
                        messageFormatter = { topic ->
                            "The controller response did not contain a result for topic $topic"
                        })
                } else {
                    val currentTime = time.milliseconds()
                    val call = getCreatePartitionsCall(
                        options = options,
                        futures = futures,
                        topics = retryTopics,
                        quotaExceededExceptions = retryTopicQuotaExceededExceptions,
                        now = currentTime,
                        deadline = deadline,
                    )
                    runnable.call(call, currentTime)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(
                    options.retryOnQuotaViolation,
                    throwable, futures, quotaExceededExceptions, (time.milliseconds() - now).toInt()
                )
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values, throwable)
            }
        }
    }

    override fun deleteRecords(
        recordsToDelete: Map<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ): DeleteRecordsResult {
        val future: SimpleAdminApiFuture<TopicPartition, DeletedRecords> =
            DeleteRecordsHandler.newFuture(recordsToDelete.keys)

        val timeoutMs = options.timeoutMs ?: defaultApiTimeoutMs

        val handler = DeleteRecordsHandler(recordsToDelete, logContext, timeoutMs)
        invokeDriver(handler, future, options.timeoutMs)

        return DeleteRecordsResult(future.all())
    }

    override fun createDelegationToken(options: CreateDelegationTokenOptions): CreateDelegationTokenResult {
        val delegationTokenFuture = KafkaFutureImpl<DelegationToken>()
        val now = time.milliseconds()
        val renewers: MutableList<CreatableRenewers> = ArrayList()
        for (principal: KafkaPrincipal in options.renewers) {
            renewers.add(
                CreatableRenewers()
                    .setPrincipalName(principal.name)
                    .setPrincipalType(principal.principalType)
            )
        }
        runnable.call(object : Call(
            callName = "createDelegationToken",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): CreateDelegationTokenRequest.Builder {
                val data = CreateDelegationTokenRequestData()
                    .setRenewers(renewers)
                    .setMaxLifetimeMs(options.maxLifeTimeMs)
                val owner = options.owner
                if (owner != null) {
                    data.setOwnerPrincipalName(owner.name)
                    data.setOwnerPrincipalType(owner.principalType)
                }
                return CreateDelegationTokenRequest.Builder(data)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as CreateDelegationTokenResponse
                if (response.hasError())
                    delegationTokenFuture.completeExceptionally(response.error().exception!!)
                else {
                    val data = response.data()
                    val tokenInfo = TokenInformation(
                        tokenId = data.tokenId,
                        owner = KafkaPrincipal(data.principalType, data.principalName),
                        tokenRequester = KafkaPrincipal(
                            principalType = data.tokenRequesterPrincipalType,
                            name = data.tokenRequesterPrincipalName
                        ),
                        renewers = options.renewers,
                        issueTimestamp = data.issueTimestampMs,
                        maxTimestamp = data.maxTimestampMs,
                        expiryTimestamp = data.expiryTimestampMs,
                    )
                    val token = DelegationToken(tokenInfo, data.hmac)
                    delegationTokenFuture.complete(token)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                delegationTokenFuture.completeExceptionally(throwable)
            }
        }, now)
        return CreateDelegationTokenResult(delegationTokenFuture)
    }

    override fun renewDelegationToken(
        hmac: ByteArray,
        options: RenewDelegationTokenOptions,
    ): RenewDelegationTokenResult {
        val expiryTimeFuture = KafkaFutureImpl<Long>()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "renewDelegationToken",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): RenewDelegationTokenRequest.Builder {
                return RenewDelegationTokenRequest.Builder(
                    RenewDelegationTokenRequestData()
                        .setHmac(hmac)
                        .setRenewPeriodMs(options.renewTimePeriodMs)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as RenewDelegationTokenResponse
                if (response.hasError())
                    expiryTimeFuture.completeExceptionally(response.error().exception!!)
                else expiryTimeFuture.complete(response.expiryTimestamp())
            }

            override fun handleFailure(throwable: Throwable) {
                expiryTimeFuture.completeExceptionally(throwable)
            }
        }, now)
        return RenewDelegationTokenResult(expiryTimeFuture)
    }

    override fun expireDelegationToken(
        hmac: ByteArray,
        options: ExpireDelegationTokenOptions,
    ): ExpireDelegationTokenResult {
        val expiryTimeFuture = KafkaFutureImpl<Long>()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "expireDelegationToken",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): ExpireDelegationTokenRequest.Builder {
                return ExpireDelegationTokenRequest.Builder(
                    ExpireDelegationTokenRequestData()
                        .setHmac(hmac)
                        .setExpiryTimePeriodMs(options.expiryTimePeriodMs)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as ExpireDelegationTokenResponse
                if (response.hasError())
                    expiryTimeFuture.completeExceptionally(response.error.exception!!)
                else expiryTimeFuture.complete(response.expiryTimestamp)
            }

            override fun handleFailure(throwable: Throwable) {
                expiryTimeFuture.completeExceptionally(throwable)
            }
        }, now)
        return ExpireDelegationTokenResult(expiryTimeFuture)
    }

    override fun describeDelegationToken(
        options: DescribeDelegationTokenOptions,
    ): DescribeDelegationTokenResult {
        val tokensFuture = KafkaFutureImpl<List<DelegationToken>>()
        val now = time.milliseconds()

        runnable.call(object : Call(
            callName = "describeDelegationToken",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DescribeDelegationTokenRequest.Builder {
                return DescribeDelegationTokenRequest.Builder(options.owners)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as DescribeDelegationTokenResponse
                if (response.hasError())
                    tokensFuture.completeExceptionally(response.error().exception!!)
                else tokensFuture.complete(response.tokens())
            }

            override fun handleFailure(throwable: Throwable) {
                tokensFuture.completeExceptionally(throwable)
            }
        }, now)
        return DescribeDelegationTokenResult(tokensFuture)
    }

    override fun describeConsumerGroups(
        groupIds: Collection<String>,
        options: DescribeConsumerGroupsOptions,
    ): DescribeConsumerGroupsResult {
        val future = DescribeConsumerGroupsHandler.newFuture(groupIds)
        val handler = DescribeConsumerGroupsHandler(
            includeAuthorizedOperations = options.includeAuthorizedOperations,
            logContext = logContext,
        )
        invokeDriver(handler, future, options.timeoutMs)
        return DescribeConsumerGroupsResult(
            future.all().entries.associate { it.key.idValue to it.value }
        )
    }

    private fun validAclOperations(authorizedOperations: Int): Set<AclOperation>? {
        return if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) null
        else Utils.from32BitField(authorizedOperations)
            .map { code -> AclOperation.fromCode(code) }
            .filter { operation: AclOperation ->
                (operation != AclOperation.UNKNOWN)
                        && (operation != AclOperation.ALL)
                        && (operation != AclOperation.ANY)
            }.toSet()
    }

    private class ListConsumerGroupsResults(
        leaders: Collection<Node>,
        private val future: KafkaFutureImpl<Collection<Any>>,
    ) {

        private val errors: MutableList<Throwable>

        private val listings: HashMap<String, ConsumerGroupListing>

        private val remaining: MutableSet<Node>

        init {
            errors = ArrayList()
            listings = HashMap()
            remaining = leaders.toMutableSet()
            tryComplete()
        }

        @Synchronized
        fun addError(throwable: Throwable, node: Node) {
            val error = ApiError.fromThrowable(throwable)
            if (error.message.isNullOrEmpty())
                errors.add(error.error.exception("Error listing groups on $node")!!)
            else errors.add(
                error.error.exception("Error listing groups on $node: ${error.message}")!!
            )
        }

        @Synchronized
        fun addListing(listing: ConsumerGroupListing) {
            listings[listing.groupId] = listing
        }

        @Synchronized
        fun tryComplete(leader: Node) {
            remaining.remove(leader)
            tryComplete()
        }

        @Synchronized
        private fun tryComplete() {
            if (remaining.isEmpty()) {
                val results = ArrayList<Any>(listings.values)
                results.addAll(errors)
                future.complete(results)
            }
        }
    }

    override fun listConsumerGroups(options: ListConsumerGroupsOptions): ListConsumerGroupsResult {
        val all = KafkaFutureImpl<Collection<Any>>()
        val nowMetadata = time.milliseconds()
        val deadline = calcDeadlineMs(nowMetadata, options.timeoutMs)
        runnable.call(object : Call(
            callName = "findAllBrokers",
            deadlineMs = deadline,
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): MetadataRequest.Builder {
                return MetadataRequest.Builder(
                    MetadataRequestData()
                        .setTopics(emptyList())
                        .setAllowAutoTopicCreation(true)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val metadataResponse = abstractResponse as MetadataResponse
                val nodes = metadataResponse.brokers()
                if (nodes.isEmpty())
                    throw StaleMetadataException("Metadata fetch failed due to missing broker list")

                val allNodes = nodes.toSet()
                val results = ListConsumerGroupsResults(allNodes, all)
                for (node: Node in allNodes) {
                    val nowList = time.milliseconds()
                    runnable.call(object : Call(
                        callName = "listConsumerGroups",
                        deadlineMs = deadline,
                        nodeProvider = ConstantNodeIdProvider(node.id),
                    ) {
                        override fun createRequest(timeoutMs: Int): ListGroupsRequest.Builder {
                            val states = options.states.map(ConsumerGroupState::toString)
                            return ListGroupsRequest.Builder(
                                ListGroupsRequestData().setStatesFilter(states)
                            )
                        }

                        private fun maybeAddConsumerGroup(group: ListedGroup) {
                            val protocolType = group.protocolType
                            if ((protocolType == ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                                val groupId = group.groupId
                                val state = if ((group.groupState == "")) null
                                else ConsumerGroupState.parse(group.groupState)

                                val groupListing = ConsumerGroupListing(
                                    groupId = groupId,
                                    isSimpleConsumerGroup = protocolType.isEmpty(),
                                    state = state,
                                )
                                results.addListing(groupListing)
                            }
                        }

                        override fun handleResponse(abstractResponse: AbstractResponse) {
                            val response = abstractResponse as ListGroupsResponse
                            synchronized(results) {
                                when (val error = Errors.forCode(response.data().errorCode)) {
                                    Errors.COORDINATOR_LOAD_IN_PROGRESS,
                                    Errors.COORDINATOR_NOT_AVAILABLE,
                                    -> throw error.exception!!

                                    Errors.NONE -> response.data().groups.forEach { group ->
                                        maybeAddConsumerGroup(group)
                                    }

                                    else -> results.addError(error.exception!!, node)
                                }
                                results.tryComplete(node)
                            }
                        }

                        override fun handleFailure(throwable: Throwable) {
                            synchronized(results) {
                                results.addError(throwable, node)
                                results.tryComplete(node)
                            }
                        }
                    }, nowList)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                val exception =
                    KafkaException("Failed to find brokers to send ListGroups", throwable)
                all.complete(listOf(exception))
            }
        }, nowMetadata)
        return ListConsumerGroupsResult(all)
    }

    override fun listConsumerGroupOffsets(
        groupSpecs: Map<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ): ListConsumerGroupOffsetsResult {
        val future = ListConsumerGroupOffsetsHandler.newFuture(groupSpecs.keys)

        val handler = ListConsumerGroupOffsetsHandler(
            groupSpecs = groupSpecs,
            requireStable = options.requireStable,
            logContext = logContext,
        )
        invokeDriver(handler, future, options.timeoutMs)

        return ListConsumerGroupOffsetsResult(future.all())
    }

    override fun deleteConsumerGroups(
        groupIds: Collection<String>,
        options: DeleteConsumerGroupsOptions,
    ): DeleteConsumerGroupsResult {
        val future = DeleteConsumerGroupsHandler.newFuture(groupIds)
        val handler = DeleteConsumerGroupsHandler(logContext)
        invokeDriver(handler, future, options.timeoutMs)
        return DeleteConsumerGroupsResult(future.all().entries.associate { it.key.idValue to it.value })
    }

    override fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: Set<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ): DeleteConsumerGroupOffsetsResult {
        val future = DeleteConsumerGroupOffsetsHandler.newFuture(groupId)
        val handler = DeleteConsumerGroupOffsetsHandler(
            groupId, partitions,
            logContext
        )
        invokeDriver(handler, future, options.timeoutMs)
        return DeleteConsumerGroupOffsetsResult(
            future[CoordinatorKey.byGroupId(groupId)],
            partitions
        )
    }

    override fun metrics(): Map<MetricName, Metric> = metrics.metrics.toMap()

    override fun electLeaders(
        electionType: ElectionType,
        partitions: Set<TopicPartition>?,
        options: ElectLeadersOptions,
    ): ElectLeadersResult {
        val electionFuture = KafkaFutureImpl<Map<TopicPartition, Throwable?>>()
        val now = time.milliseconds()

        runnable.call(object : Call(
            callName = "electLeaders",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = ControllerNodeProvider(),
        ) {

            override fun createRequest(timeoutMs: Int): ElectLeadersRequest.Builder =
                ElectLeadersRequest.Builder(electionType, partitions, timeoutMs)

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as ElectLeadersResponse
                val result = ElectLeadersResponse.electLeadersResult(response.data())

                // For version == 0 then errorCode would be 0 which maps to Errors.NONE
                val error = Errors.forCode(response.data().errorCode)
                if (error != Errors.NONE) {
                    electionFuture.completeExceptionally(error.exception!!)
                    return
                }
                electionFuture.complete(result)
            }

            override fun handleFailure(throwable: Throwable) {
                electionFuture.completeExceptionally(throwable)
            }
        }, now)
        return ElectLeadersResult(electionFuture)
    }

    override fun alterPartitionReassignments(
        reassignments: Map<TopicPartition, NewPartitionReassignment?>,
        options: AlterPartitionReassignmentsOptions,
    ): AlterPartitionReassignmentsResult {
        val futures = mutableMapOf<TopicPartition, KafkaFutureImpl<Unit>>()
        val topicsToReassignments = TreeMap<String, MutableMap<Int, NewPartitionReassignment?>>()
        for ((key, reassignment) in reassignments) {
            val topic = key.topic
            val partition = key.partition
            val topicPartition = TopicPartition(topic, partition)
            val future = KafkaFutureImpl<Unit>()
            futures[topicPartition] = future
            if (topicNameIsUnrepresentable(topic)) future.completeExceptionally(
                InvalidTopicException(
                    "The given topic name '$topic' cannot be represented in a request."
                )
            ) else if (topicPartition.partition < 0) future.completeExceptionally(
                InvalidTopicException(
                    "The given partition index ${topicPartition.partition} is not valid."
                )
            ) else {
                var partitionReassignments = topicsToReassignments[topicPartition.topic]
                if (partitionReassignments == null) {
                    partitionReassignments = TreeMap()
                    topicsToReassignments[topic] = partitionReassignments
                }
                partitionReassignments[partition] = reassignment
            }
        }
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "alterPartitionReassignments",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = ControllerNodeProvider(),
        ) {

            override fun createRequest(timeoutMs: Int): AlterPartitionReassignmentsRequest.Builder {
                val data = AlterPartitionReassignmentsRequestData()

                topicsToReassignments.forEach { (topicName, partitionsToReassignments) ->
                    val reassignablePartitions = mutableListOf<ReassignablePartition>()

                    partitionsToReassignments.forEach { (partitionIndex, reassignment) ->
                        val reassignablePartition = ReassignablePartition()
                            .setPartitionIndex(partitionIndex)
                            .setReplicas(reassignment?.targetReplicas?.toIntArray() ?: intArrayOf())
                        reassignablePartitions.add(reassignablePartition)
                    }

                    val reassignableTopic = ReassignableTopic()
                        .setName(topicName)
                        .setPartitions(reassignablePartitions)
                    data.topics += reassignableTopic
                }
                data.setTimeoutMs(timeoutMs)
                return AlterPartitionReassignmentsRequest.Builder(data)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as AlterPartitionReassignmentsResponse
                val errors = mutableMapOf<TopicPartition, ApiException?>()
                var receivedResponsesCount = 0
                when (val topLevelError = Errors.forCode(response.data().errorCode)) {
                    Errors.NONE -> receivedResponsesCount += validateTopicResponses(
                        topicResponses = response.data().responses,
                        errors = errors,
                    )

                    Errors.NOT_CONTROLLER -> handleNotControllerError(topLevelError)
                    else -> for (topicResponse in response.data().responses) {
                        val topicName = topicResponse.name
                        for (partition in topicResponse.partitions) {
                            errors[TopicPartition(topicName, partition.partitionIndex)] =
                                ApiError(
                                    error = topLevelError,
                                    message = response.data().errorMessage,
                                ).exception()
                            receivedResponsesCount += 1
                        }
                    }
                }
                assertResponseCountMatch(errors, receivedResponsesCount)
                errors.entries.forEach { (key, exception) ->
                    val future = futures[key]!!
                    if (exception == null) future.complete(Unit)
                    else future.completeExceptionally(exception)
                }
            }

            private fun assertResponseCountMatch(
                errors: Map<TopicPartition, ApiException?>,
                receivedResponsesCount: Int,
            ) {
                val expectedResponsesCount = topicsToReassignments.values.sumOf { it.size }

                if (
                    errors.values.none { it != null }
                    && receivedResponsesCount != expectedResponsesCount
                ) {
                    val quantifier =
                        if (receivedResponsesCount > expectedResponsesCount) "many"
                        else "less"
                    throw UnknownServerException(
                        "The server returned too $quantifier results." +
                                "Expected $expectedResponsesCount but received $receivedResponsesCount"
                    )
                }
            }

            private fun validateTopicResponses(
                topicResponses: List<ReassignableTopicResponse>,
                errors: MutableMap<TopicPartition, ApiException?>,
            ): Int {
                var receivedResponsesCount = 0
                for (topicResponse: ReassignableTopicResponse in topicResponses) {
                    val topicName = topicResponse.name
                    topicResponse.partitions.forEach { partResponse ->
                        val partitionError = Errors.forCode(partResponse.errorCode)
                        val tp = TopicPartition(topicName, partResponse.partitionIndex)
                        errors[tp] = if (partitionError == Errors.NONE) null
                        else ApiError(partitionError, partResponse.errorMessage).exception()
                        receivedResponsesCount += 1
                    }
                }
                return receivedResponsesCount
            }

            override fun handleFailure(throwable: Throwable) {
                futures.values.forEach { future -> future.completeExceptionally(throwable) }
            }
        }
        if (topicsToReassignments.isNotEmpty()) runnable.call(call, now)
        return AlterPartitionReassignmentsResult(futures.toMap())
    }

    override fun listPartitionReassignments(
        partitions: Set<TopicPartition>,
        options: ListPartitionReassignmentsOptions,
    ): ListPartitionReassignmentsResult {
        val partitionReassignmentsFuture =
            KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>>()

        partitions.forEach { tp ->
            val topic = tp.topic
            val partition = tp.partition
            if (topicNameIsUnrepresentable(topic)) partitionReassignmentsFuture.completeExceptionally(
                InvalidTopicException("The given topic name '$topic' cannot be represented in a request.")
            )
            else if (partition < 0) partitionReassignmentsFuture.completeExceptionally(
                InvalidTopicException("The given partition index $partition is not valid.")
            )

            if (partitionReassignmentsFuture.isCompletedExceptionally)
                return ListPartitionReassignmentsResult(partitionReassignmentsFuture)
        }
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "listPartitionReassignments",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): ListPartitionReassignmentsRequest.Builder {
                val listData = ListPartitionReassignmentsRequestData()
                listData.setTimeoutMs(timeoutMs)
                val reassignmentTopicByTopicName =
                    HashMap<String, ListPartitionReassignmentsTopics>()

                partitions.forEach { tp ->
                    val reassignmentTopic =
                        reassignmentTopicByTopicName.computeIfAbsent(tp.topic) {
                            ListPartitionReassignmentsTopics().setName(tp.topic)
                        }

                    reassignmentTopic.partitionIndexes += tp.partition
                }

                listData.setTopics(reassignmentTopicByTopicName.values.toList())
                return ListPartitionReassignmentsRequest.Builder(listData)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as ListPartitionReassignmentsResponse
                when (val error = Errors.forCode(response.data().errorCode)) {
                    Errors.NONE -> Unit
                    Errors.NOT_CONTROLLER -> handleNotControllerError(error)
                    else -> partitionReassignmentsFuture.completeExceptionally(
                        ApiError(error, response.data().errorMessage).exception()!!
                    )
                }
                val reassignmentMap = mutableMapOf<TopicPartition, PartitionReassignment>()
                for (topicReassignment in response.data().topics) {
                    val topicName = topicReassignment.name
                    for (partitionReassignment in topicReassignment.partitions) {
                        reassignmentMap[
                            TopicPartition(
                                topic = topicName,
                                partition = partitionReassignment.partitionIndex,
                            )
                        ] = PartitionReassignment(
                            replicas = partitionReassignment.replicas.toList(),
                            addingReplicas = partitionReassignment.addingReplicas.toList(),
                            removingReplicas = partitionReassignment.removingReplicas.toList(),
                        )
                    }
                }
                partitionReassignmentsFuture.complete(reassignmentMap)
            }

            override fun handleFailure(throwable: Throwable) {
                partitionReassignmentsFuture.completeExceptionally(throwable)
            }
        }, now)
        return ListPartitionReassignmentsResult(partitionReassignmentsFuture)
    }

    @Throws(ApiException::class)
    private fun handleNotControllerError(response: AbstractResponse) {
        if (response.errorCounts().containsKey(Errors.NOT_CONTROLLER))
            handleNotControllerError(Errors.NOT_CONTROLLER)
    }

    @Throws(ApiException::class)
    private fun handleNotControllerError(error: Errors) {
        metadataManager.clearController()
        metadataManager.requestUpdate()
        throw error.exception!!
    }

    /**
     * Returns the broker id pertaining to the given resource, or null if the resource is not
     * associated with a particular broker.
     */
    private fun nodeFor(resource: ConfigResource): Int? {
        return if (((resource.type == ConfigResource.Type.BROKER && !resource.isDefault)
                    || resource.type == ConfigResource.Type.BROKER_LOGGER)
        ) Integer.valueOf(resource.name)
        else null
    }

    private fun getMembersFromGroup(groupId: String, reason: String): List<MemberIdentity> {
        val members: Collection<MemberDescription>
        try {
            members = describeConsumerGroups(setOf(groupId))
                .describedGroups()[groupId]!!
                .get()
                .members

        } catch (ex: Exception) {
            throw KafkaException(
                "Encounter exception when trying to get members from group: $groupId",
                ex
            )
        }
        val membersToRemove: MutableList<MemberIdentity> = ArrayList()
        for (member: MemberDescription in members) {
            val memberIdentity = MemberIdentity().setReason(reason)

            if (member.groupInstanceId == null) memberIdentity.setMemberId(member.memberId)
            else memberIdentity.setGroupInstanceId(member.groupInstanceId)

            membersToRemove.add(memberIdentity)
        }
        return membersToRemove
    }

    override fun removeMembersFromConsumerGroup(
        groupId: String,
        options: RemoveMembersFromConsumerGroupOptions,
    ): RemoveMembersFromConsumerGroupResult {

        val optionsReason = options.reason
        val reason =
            if (optionsReason.isNullOrEmpty()) DEFAULT_LEAVE_GROUP_REASON
            else JoinGroupRequest.maybeTruncateReason(optionsReason)

        val members: List<MemberIdentity> =
            if (options.removeAll()) getMembersFromGroup(groupId, reason)
            else options.members
                .map { m: MemberToRemove -> m.toMemberIdentity().setReason(reason) }

        val future = RemoveMembersFromConsumerGroupHandler.newFuture(groupId)
        val handler = RemoveMembersFromConsumerGroupHandler(
            groupId = groupId,
            members = members,
            logContext = logContext,
        )
        invokeDriver(handler, future, options.timeoutMs)

        return RemoveMembersFromConsumerGroupResult(
            future = future[CoordinatorKey.byGroupId(groupId)],
            memberInfos = options.members,
        )
    }

    override fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ): AlterConsumerGroupOffsetsResult {

        val future = AlterConsumerGroupOffsetsHandler.newFuture(groupId)
        val handler = AlterConsumerGroupOffsetsHandler(
            groupId = groupId,
            offsets = offsets,
            logContext = logContext,
        )
        invokeDriver(handler, future, options.timeoutMs)

        return AlterConsumerGroupOffsetsResult(future[CoordinatorKey.byGroupId(groupId)])
    }

    override fun listOffsets(
        topicPartitionOffsets: Map<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ): ListOffsetsResult {
        val future: SimpleAdminApiFuture<TopicPartition, ListOffsetsResultInfo> =
            ListOffsetsHandler.newFuture(topicPartitionOffsets.keys)
        val offsetQueriesByPartition =
            topicPartitionOffsets.mapValues { (_, value) -> getOffsetFromSpec(value) }
        val handler = ListOffsetsHandler(offsetQueriesByPartition, options, logContext)
        invokeDriver(handler, future, options.timeoutMs)
        return ListOffsetsResult(future.all())
    }

    override fun describeClientQuotas(
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ): DescribeClientQuotasResult {
        val future = KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>>()
        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "describeClientQuotas",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DescribeClientQuotasRequest.Builder {
                return DescribeClientQuotasRequest.Builder(filter)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as DescribeClientQuotasResponse
                response.complete(future)
            }

            override fun handleFailure(throwable: Throwable) {
                future.completeExceptionally(throwable)
            }
        }, now)
        return DescribeClientQuotasResult(future)
    }

    override fun alterClientQuotas(
        entries: Collection<ClientQuotaAlteration>,
        options: AlterClientQuotasOptions,
    ): AlterClientQuotasResult {
        val futures = entries.associateBy(
            keySelector = { it.entity },
            valueTransform = { KafkaFutureImpl<Unit>() }
        ).toMutableMap()

        val now = time.milliseconds()
        runnable.call(object : Call(
            callName = "alterClientQuotas",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): AlterClientQuotasRequest.Builder =
                AlterClientQuotasRequest.Builder(entries, options.validateOnly)

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as AlterClientQuotasResponse
                response.complete(futures)
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }, now)

        return AlterClientQuotasResult(futures.toMap())
    }

    override fun describeUserScramCredentials(
        users: List<String?>?,
        options: DescribeUserScramCredentialsOptions,
    ): DescribeUserScramCredentialsResult {
        val dataFuture = KafkaFutureImpl<DescribeUserScramCredentialsResponseData>()
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "describeUserScramCredentials",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): DescribeUserScramCredentialsRequest.Builder {
                val requestData = DescribeUserScramCredentialsRequestData()
                if (!users.isNullOrEmpty()) requestData.setUsers(
                    users.filterNotNull().map { user -> UserName().setName(user) }
                )
                return DescribeUserScramCredentialsRequest.Builder(requestData)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as DescribeUserScramCredentialsResponse
                val data = response.data()
                val messageLevelErrorCode = data.errorCode
                if (messageLevelErrorCode != Errors.NONE.code) {
                    dataFuture.completeExceptionally(
                        Errors.forCode(messageLevelErrorCode).exception(data.errorMessage)!!
                    )
                } else {
                    dataFuture.complete(data)
                }
            }

            override fun handleFailure(throwable: Throwable) {
                dataFuture.completeExceptionally(throwable)
            }
        }
        runnable.call(call, now)
        return DescribeUserScramCredentialsResult(dataFuture)
    }

    override fun alterUserScramCredentials(
        alterations: List<UserScramCredentialAlteration>,
        options: AlterUserScramCredentialsOptions,
    ): AlterUserScramCredentialsResult {
        val now = time.milliseconds()

        val futures = alterations.associateBy(
            keySelector = { it.user },
            valueTransform = { KafkaFutureImpl<Unit>() }
        ).toMutableMap()

        val userIllegalAlterationExceptions: MutableMap<String, Exception> = HashMap()
        // We need to keep track of users with deletions of an unknown SCRAM mechanism
        val usernameMustNotBeEmptyMsg = "Username must not be empty"
        val passwordMustNotBeEmptyMsg = "Password must not be empty"
        val unknownScramMechanismMsg = "Unknown SCRAM mechanism"
        alterations
            .filterIsInstance<UserScramCredentialDeletion>()
            .forEach { alteration: UserScramCredentialAlteration ->
                val user = alteration.user
                if (user.isEmpty()) userIllegalAlterationExceptions[alteration.user] =
                    UnacceptableCredentialException(usernameMustNotBeEmptyMsg)
                else {
                    val deletion: UserScramCredentialDeletion =
                        alteration as UserScramCredentialDeletion
                    val mechanism = deletion.mechanism
                    if (mechanism == ScramMechanism.UNKNOWN)
                        userIllegalAlterationExceptions[user] =
                            UnsupportedSaslMechanismException(unknownScramMechanismMsg)
                }
            }
        // Creating an upsertion may throw InvalidKeyException or NoSuchAlgorithmException, so keep
        // track of which users are affected by such a failure so we can fail all their alterations
        // later
        val userInsertions =
            mutableMapOf<String, MutableMap<ScramMechanism, ScramCredentialUpsertion>>()

        alterations
            .filterIsInstance<UserScramCredentialUpsertion>()
            .filter { alteration: UserScramCredentialAlteration ->
                !userIllegalAlterationExceptions.containsKey(alteration.user)
            }
            .forEach { alteration: UserScramCredentialAlteration ->
                val user = alteration.user
                if (user.isEmpty()) userIllegalAlterationExceptions[alteration.user] =
                    UnacceptableCredentialException(usernameMustNotBeEmptyMsg)
                else {
                    val upsertion: UserScramCredentialUpsertion =
                        alteration as UserScramCredentialUpsertion
                    try {
                        val password = upsertion.password
                        if (password.isEmpty())
                            userIllegalAlterationExceptions[user] =
                                UnacceptableCredentialException(passwordMustNotBeEmptyMsg)
                        else {
                            val mechanism = upsertion.credentialInfo.mechanism
                            if (mechanism == ScramMechanism.UNKNOWN)
                                userIllegalAlterationExceptions[user] =
                                    UnsupportedSaslMechanismException(unknownScramMechanismMsg)
                            else {
                                userInsertions.putIfAbsent(user, mutableMapOf())
                                userInsertions[user]!![mechanism] =
                                    getScramCredentialUpsertion(upsertion)
                            }
                        }
                    } catch (e: NoSuchAlgorithmException) {
                        // we might overwrite an exception from a previous alteration, but we don't
                        // really care since we just need to mark this user as having at least one
                        // illegal alteration and make an exception instance available for
                        // completing the corresponding future exceptionally
                        userIllegalAlterationExceptions[user] =
                            UnsupportedSaslMechanismException(unknownScramMechanismMsg)
                    } catch (e: InvalidKeyException) {
                        // generally shouldn't happen since we deal with the empty password case above,
                        // but we still need to catch/handle it
                        userIllegalAlterationExceptions[user] =
                            UnacceptableCredentialException(e.message, e)
                    }
                }
            }

        // submit alterations only for users that do not have an illegal alteration as identified above
        val call: Call = object : Call(
            callName = "alterUserScramCredentials",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): AlterUserScramCredentialsRequest.Builder {
                return AlterUserScramCredentialsRequest.Builder(
                    AlterUserScramCredentialsRequestData().setUpsertions(
                        alterations
                            .filterIsInstance<UserScramCredentialUpsertion>()
                            .filter { !userIllegalAlterationExceptions.containsKey(it.user) }
                            .map { userInsertions[it.user]!![it.credentialInfo.mechanism]!! }
                    )
                        .setDeletions(
                            alterations
                                .filterIsInstance<UserScramCredentialDeletion>()
                                .filter { !userIllegalAlterationExceptions.containsKey(it.user) }
                                .map { getScramCredentialDeletion(it) }
                        )
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as AlterUserScramCredentialsResponse
                // Check for controller change
                for (error: Errors in response.errorCounts().keys) {
                    if (error == Errors.NOT_CONTROLLER) {
                        handleNotControllerError(error)
                    }
                }
                /* Now that we have the results for the ones we sent, fail any users that have an
                 * illegal alteration as identified above. Be sure to do this after the
                 *  NOT_CONTROLLER error check above so that all errors are consistent in that case.
                 */
                userIllegalAlterationExceptions.forEach { entry ->
                    futures[entry.key]!!.completeExceptionally(entry.value)
                }
                response.data().results.forEach { result ->
                    val future: KafkaFutureImpl<Unit>? = futures[result.user]
                    if (future == null) log.warn(
                        "Server response mentioned unknown user {}",
                        result.user
                    )
                    else {
                        val error: Errors = Errors.forCode(result.errorCode)
                        if (error != Errors.NONE)
                            future.completeExceptionally(error.exception(result.errorMessage)!!)
                        else future.complete(Unit)
                    }
                }
                completeUnrealizedFutures(
                    futures = futures,
                    messageFormatter = { user -> "The broker response did not contain a result for user $user" }
                )
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(futures.values, throwable)
        }
        runnable.call(call, now)
        return AlterUserScramCredentialsResult(futures.toMap())
    }

    override fun describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult {
        val future = KafkaFutureImpl<FeatureMetadata>()
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "describeFeatures",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {
            private fun createFeatureMetadata(response: ApiVersionsResponse): FeatureMetadata {
                val finalizedFeatures: MutableMap<String, FinalizedVersionRange> = HashMap()
                for (key in response.data().finalizedFeatures.valuesSet()) {
                    finalizedFeatures[key.name] =
                        FinalizedVersionRange(key.minVersionLevel, key.maxVersionLevel)
                }
                val finalizedFeaturesEpoch: Long? =
                    if (response.data().finalizedFeaturesEpoch >= 0L)
                        response.data().finalizedFeaturesEpoch
                    else null
                val supportedFeatures: MutableMap<String, SupportedVersionRange> = HashMap()
                for (key in response.data().supportedFeatures.valuesSet()) {
                    supportedFeatures[key.name] =
                        SupportedVersionRange(key.minVersion, key.maxVersion)
                }
                return FeatureMetadata(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures)
            }

            override fun createRequest(timeoutMs: Int): ApiVersionsRequest.Builder {
                return ApiVersionsRequest.Builder()
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val apiVersionsResponse = abstractResponse as ApiVersionsResponse
                if (apiVersionsResponse.data().errorCode == Errors.NONE.code)
                    future.complete(createFeatureMetadata(apiVersionsResponse))
                else future.completeExceptionally(
                    Errors.forCode(apiVersionsResponse.data().errorCode).exception!!
                )
            }

            override fun handleFailure(throwable: Throwable) {
                completeAllExceptionally(listOf(future), throwable)
            }
        }
        runnable.call(call, now)
        return DescribeFeaturesResult(future)
    }

    override fun updateFeatures(
        featureUpdates: Map<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ): UpdateFeaturesResult {
        require(featureUpdates.isNotEmpty()) { "Feature updates can not be null or empty." }
        val updateFutures = mutableMapOf<String, KafkaFutureImpl<Unit>>()
        for ((feature, _) in featureUpdates) {
            require(feature.isNotBlank()) { "Provided feature can not be empty." }
            updateFutures[feature] = KafkaFutureImpl()
        }
        val now = time.milliseconds()
        val call: Call = object : Call(
            callName = "updateFeatures",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = ControllerNodeProvider(),
        ) {
            override fun createRequest(timeoutMs: Int): UpdateFeaturesRequest.Builder {
                val featureUpdatesRequestData = FeatureUpdateKeyCollection()
                for ((feature, update) in featureUpdates) {
                    val requestItem = FeatureUpdateKey()
                    requestItem.setFeature(feature)
                    requestItem.setMaxVersionLevel(update.maxVersionLevel)
                    requestItem.setUpgradeType(update.upgradeType.code)
                    featureUpdatesRequestData.add(requestItem)
                }
                return UpdateFeaturesRequest.Builder(
                    UpdateFeaturesRequestData()
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.validateOnly())
                        .setFeatureUpdates(featureUpdatesRequestData)
                )
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as UpdateFeaturesResponse
                val topLevelError = response.topLevelError()
                when (topLevelError.error) {
                    Errors.NONE -> {
                        for (result: UpdatableFeatureResult in response.data().results) {
                            val future = updateFutures[result.feature]
                            if (future == null) log.warn(
                                "Server response mentioned unknown feature {}",
                                result.feature
                            )
                            else {
                                val error = Errors.forCode(result.errorCode)
                                if (error == Errors.NONE) future.complete(Unit)
                                else future.completeExceptionally(error.exception(result.errorMessage)!!)
                            }
                        }
                        // The server should send back a response for every feature, but we do a
                        // sanity check anyway.
                        completeUnrealizedFutures(
                            futures = updateFutures,
                            messageFormatter = { feature ->
                                "The controller response did not contain a result for feature " +
                                        feature
                            }
                        )
                    }

                    Errors.NOT_CONTROLLER -> handleNotControllerError(topLevelError.error)
                    else -> for ((_, value) in updateFutures) {
                        value.completeExceptionally(topLevelError.exception()!!)
                    }
                }
            }

            override fun handleFailure(throwable: Throwable) =
                completeAllExceptionally(updateFutures.values, throwable)
        }
        runnable.call(call, now)
        return UpdateFeaturesResult(updateFutures.toMap())
    }

    override fun describeMetadataQuorum(
        options: DescribeMetadataQuorumOptions,
    ): DescribeMetadataQuorumResult {
        val provider: NodeProvider = LeastLoadedNodeProvider()
        val future = KafkaFutureImpl<QuorumInfo>()
        val now = time.milliseconds()

        val call: Call = object : Call(
            callName = "describeMetadataQuorum",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = provider,
        ) {
            private fun translateReplicaState(
                replica: DescribeQuorumResponseData.ReplicaState,
            ): QuorumInfo.ReplicaState = QuorumInfo.ReplicaState(
                replicaId = replica.replicaId,
                logEndOffset = replica.logEndOffset,
                lastFetchTimestamp = if (replica.lastFetchTimestamp == -1L) null
                else replica.lastFetchTimestamp,
                lastCaughtUpTimestamp = if (replica.lastCaughtUpTimestamp == -1L) null
                else replica.lastCaughtUpTimestamp
            )

            private fun createQuorumResult(
                partition: DescribeQuorumResponseData.PartitionData,
            ): QuorumInfo {
                val voters = partition.currentVoters.map { replica ->
                    translateReplicaState(replica)
                }
                val observers = partition.observers.map { replica ->
                    translateReplicaState(replica)
                }

                return QuorumInfo(
                    leaderId = partition.leaderId,
                    leaderEpoch = partition.leaderEpoch.toLong(),
                    highWatermark = partition.highWatermark,
                    voters = voters,
                    observers = observers,
                )
            }

            override fun createRequest(timeoutMs: Int): DescribeQuorumRequest.Builder =
                DescribeQuorumRequest.Builder(
                    DescribeQuorumRequest.singletonRequest(
                        TopicPartition(
                            topic = Topic.CLUSTER_METADATA_TOPIC_NAME,
                            partition = Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition,
                        )
                    )
                )

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val quorumResponse = abstractResponse as DescribeQuorumResponse
                if (quorumResponse.data().errorCode != Errors.NONE.code)
                    throw Errors.forCode(quorumResponse.data().errorCode).exception!!
                if (quorumResponse.data().topics.size != 1) {
                    val msg = "DescribeMetadataQuorum received " +
                            "${quorumResponse.data().topics.size} topics when 1 was expected"
                    log.debug(msg)
                    throw UnknownServerException(msg)
                }
                val topic = quorumResponse.data().topics[0]
                if (topic.topicName != Topic.CLUSTER_METADATA_TOPIC_NAME) {
                    val msg =
                        "DescribeMetadataQuorum received a topic with name ${topic.topicName} " +
                                "when ${Topic.CLUSTER_METADATA_TOPIC_NAME} was expected"
                    log.debug(msg)
                    throw UnknownServerException(msg)
                }
                if (topic.partitions.size != 1) {
                    val msg =
                        "DescribeMetadataQuorum received a topic ${topic.topicName} with " +
                                "${topic.partitions.size} partitions when 1 was expected"
                    log.debug(msg)
                    throw UnknownServerException(msg)
                }
                val partition = topic.partitions[0]
                if (partition.partitionIndex != Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition) {
                    val msg =
                        "DescribeMetadataQuorum received a single partition with index " +
                                "${partition.partitionIndex} when " +
                                "${Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition} was expected"
                    log.debug(msg)
                    throw UnknownServerException(msg)
                }
                if (partition.errorCode != Errors.NONE.code)
                    throw Errors.forCode(partition.errorCode).exception!!

                future.complete(createQuorumResult(partition))
            }

            override fun handleFailure(throwable: Throwable) {
                future.completeExceptionally(throwable)
            }
        }
        runnable.call(call, now)
        return DescribeMetadataQuorumResult(future)
    }

    override fun unregisterBroker(
        brokerId: Int,
        options: UnregisterBrokerOptions,
    ): UnregisterBrokerResult {
        val future = KafkaFutureImpl<Unit>()
        val now = time.milliseconds()

        val call: Call = object : Call(
            callName = "unregisterBroker",
            deadlineMs = calcDeadlineMs(now, options.timeoutMs),
            nodeProvider = LeastLoadedNodeProvider(),
        ) {

            override fun createRequest(timeoutMs: Int): UnregisterBrokerRequest.Builder {
                val data = UnregisterBrokerRequestData().setBrokerId(brokerId)
                return UnregisterBrokerRequest.Builder(data)
            }

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val response = abstractResponse as UnregisterBrokerResponse
                when (val error = Errors.forCode(response.data().errorCode)) {
                    Errors.NONE -> future.complete(Unit)
                    Errors.REQUEST_TIMED_OUT -> throw error.exception!!
                    else -> {
                        log.error(
                            "Unregister broker request for broker ID {} failed: {}",
                            brokerId,
                            error.message
                        )
                        future.completeExceptionally(error.exception!!)
                    }
                }
            }

            override fun handleFailure(throwable: Throwable) {
                future.completeExceptionally(throwable)
            }
        }
        runnable.call(call, now)
        return UnregisterBrokerResult(future)
    }

    override fun describeProducers(
        partitions: Collection<TopicPartition>,
        options: DescribeProducersOptions,
    ): DescribeProducersResult {
        val future = DescribeProducersHandler.newFuture(partitions)
        val handler = DescribeProducersHandler(options, logContext)
        invokeDriver(handler, future, options.timeoutMs)
        return DescribeProducersResult(future.all())
    }

    override fun describeTransactions(
        transactionalIds: Collection<String>,
        options: DescribeTransactionsOptions,
    ): DescribeTransactionsResult {
        val future = DescribeTransactionsHandler.newFuture(transactionalIds)
        val handler = DescribeTransactionsHandler(logContext)
        invokeDriver(handler, future, options.timeoutMs)

        return DescribeTransactionsResult(future.all())
    }

    override fun abortTransaction(
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ): AbortTransactionResult {
        val future = AbortTransactionHandler.newFuture(setOf(spec.topicPartition))
        val handler = AbortTransactionHandler(spec, logContext)
        invokeDriver(handler, future, options.timeoutMs)

        return AbortTransactionResult(future.all())
    }

    override fun listTransactions(options: ListTransactionsOptions): ListTransactionsResult {
        val future = ListTransactionsHandler.newFuture()
        val handler = ListTransactionsHandler(options, logContext)
        invokeDriver(handler, future, options.timeoutMs)

        return ListTransactionsResult(future.all())
    }

    override fun fenceProducers(
        transactionalIds: Collection<String>,
        options: FenceProducersOptions,
    ): FenceProducersResult {
        val future = FenceProducersHandler.newFuture(transactionalIds)
        val handler = FenceProducersHandler(logContext)
        invokeDriver(handler, future, options.timeoutMs)

        return FenceProducersResult(future.all())
    }

    private fun <K, V> invokeDriver(
        handler: AdminApiHandler<K, V>,
        future: AdminApiFuture<K, V>,
        timeoutMs: Int?,
    ) {
        val currentTimeMs = time.milliseconds()
        val deadlineMs = calcDeadlineMs(currentTimeMs, timeoutMs)
        val driver = AdminApiDriver(
            handler = handler,
            future = future,
            deadlineMs = deadlineMs,
            retryBackoffMs = retryBackoffMs,
            logContext = logContext
        )
        maybeSendRequests(driver, currentTimeMs)
    }

    private fun <K, V> maybeSendRequests(driver: AdminApiDriver<K, V>, currentTimeMs: Long) {
        for (spec in driver.poll()) runnable.call(newCall(driver, spec), currentTimeMs)
    }

    private fun <K, V> newCall(driver: AdminApiDriver<K, V>, spec: RequestSpec<K>): Call {
        val nodeProvider = spec.scope.destinationBrokerId()?.let { ConstantNodeIdProvider(it) }
            ?: LeastLoadedNodeProvider()
        return object : Call(
            callName = spec.name,
            nextAllowedTryMs = spec.nextAllowedTryMs,
            tries = spec.tries,
            deadlineMs = spec.deadlineMs,
            nodeProvider = nodeProvider,
        ) {
            override fun createRequest(timeoutMs: Int): AbstractRequest.Builder<*> = spec.request

            override fun handleResponse(abstractResponse: AbstractResponse) {
                val currentTimeMs = time.milliseconds()
                driver.onResponse(currentTimeMs, spec, abstractResponse, curNode())
                maybeSendRequests(driver, currentTimeMs)
            }

            override fun handleFailure(throwable: Throwable) {
                val currentTimeMs = time.milliseconds()
                driver.onFailure(currentTimeMs, spec, throwable)
                maybeSendRequests(driver, currentTimeMs)
            }

            override fun maybeRetry(now: Long, throwable: Throwable?) {
                if (throwable is DisconnectException) {
                    // Disconnects are a special case. We want to give the driver a chance
                    // to retry lookup rather than getting stuck on a node which is down.
                    // For example, if a partition leader shuts down after our metadata query,
                    // then we might get a disconnect. We want to try to find the new partition
                    // leader rather than retrying on the same node.
                    driver.onFailure(now, spec, throwable)
                    maybeSendRequests(driver, now)
                } else super.maybeRetry(now, throwable)
            }
        }
    }

    @Suppress("TooManyFunctions")
    companion object {

        /**
         * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
         */
        private val ADMIN_CLIENT_ID_SEQUENCE = AtomicInteger(1)

        /**
         * The prefix to use for the JMX metrics for this class
         */
        private val JMX_PREFIX = "kafka.admin.client"

        /**
         * An invalid shutdown time which indicates that a shutdown has not yet been performed.
         */
        private val INVALID_SHUTDOWN_TIME: Long = -1

        /**
         * The default reason for a LeaveGroupRequest.
         */
        val DEFAULT_LEAVE_GROUP_REASON = "member was removed by an admin"

        /**
         * Thread name prefix for admin client network thread
         */
        val NETWORK_THREAD_PREFIX = "kafka-admin-client-thread"

        /**
         * Get or create a list value from a map.
         *
         * @param map The map to get or create the element from.
         * @param key The key.
         * @param K The key type.
         * @param V The value type.
         * @return The list value.
         */
        @Deprecated("Use computeIfAbsent instead")
        fun <K, V> getOrCreateListValue(
            map: MutableMap<K, MutableList<V>>,
            key: K,
        ): MutableList<V> {
            return map.computeIfAbsent(key) { LinkedList() }
        }

        /**
         * Send an exception to every element in a collection of KafkaFutureImpls.
         *
         * @param futures The collection of KafkaFutureImpl objects.
         * @param exc The exception
         * @param T The KafkaFutureImpl result type.
         */
        private fun <T> completeAllExceptionally(
            futures: Collection<KafkaFutureImpl<T>?>,
            exc: Throwable,
        ) {
            futures.forEach { future -> future?.completeExceptionally(exc) }
        }

        /**
         * Send an exception to all futures in the provided stream
         *
         * @param futures The stream of KafkaFutureImpl objects.
         * @param exception The exception
         * @param T The KafkaFutureImpl result type.
         */
        private fun <T> completeAllExceptionally(
            futures: List<KafkaFutureImpl<T>>,
            exception: Throwable,
        ) = futures.forEach { future -> future.completeExceptionally(exception) }

        /**
         * Send an exception to all futures in the provided stream
         *
         * @param futures The stream of KafkaFutureImpl objects.
         * @param exception The exception
         * @param K The key that is associated with the Kafka future.
         * @param V The KafkaFutureImpl result type.
         */
        private fun <K, V> completeAllExceptionally(
            futures: Map<K, KafkaFutureImpl<V>>,
            exception: Throwable,
        ) = futures.values.forEach { future -> future.completeExceptionally(exception) }

        /**
         * Get the current time remaining before a deadline as an integer.
         *
         * @param now The current time in milliseconds.
         * @param deadlineMs The deadline time in milliseconds.
         * @return The time delta in milliseconds.
         */
        fun calcTimeoutMsRemainingAsInt(now: Long, deadlineMs: Long): Int {
            var deltaMs = deadlineMs - now
            if (deltaMs > Int.MAX_VALUE) deltaMs = Int.MAX_VALUE.toLong()
            else if (deltaMs < Int.MIN_VALUE) deltaMs = Int.MIN_VALUE.toLong()
            return deltaMs.toInt()
        }

        /**
         * Generate the client id based on the configuration.
         *
         * @param config The configuration
         * @return The client id
         */
        fun generateClientId(config: AdminClientConfig): String {
            val clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG)
            return if (!clientId.isNullOrEmpty()) clientId
            else "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement()
        }

        /**
         * Pretty-print an exception.
         *
         * @param throwable The exception.
         * @return A compact human-readable string.
         */
        fun prettyPrintException(throwable: Throwable?): String {
            if (throwable == null) return "Null exception."
            return if (throwable.message != null) {
                throwable.javaClass.simpleName + ": " + throwable.message
            } else throwable.javaClass.simpleName
        }

        internal fun createInternal(
            config: AdminClientConfig,
            timeoutProcessorFactory: TimeoutProcessorFactory = TimeoutProcessorFactory(),
            hostResolver: HostResolver? = null,
        ): KafkaAdminClient {
            var metrics: Metrics? = null
            var networkClient: NetworkClient? = null
            val time = Time.SYSTEM
            val clientId = generateClientId(config)
            val apiVersions = ApiVersions()
            val logContext = createLogContext(clientId)
            try {
                // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
                // simplifies communication with older brokers)
                val metadataManager = AdminMetadataManager(
                    logContext = logContext,
                    refreshBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG)!!,
                    metadataExpireMs = config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG)!!,
                )
                val addresses = ClientUtils.parseAndValidateAddresses(config)
                metadataManager.update(Cluster.bootstrap(addresses), time.milliseconds())
                val reporters = CommonClientConfigs.metricsReporters(clientId, config)
                val metricTags = mapOf("client-id" to clientId)
                val metricConfig =
                    MetricConfig().apply {
                        samples = config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG)!!
                        timeWindowMs =
                            config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)!!
                        recordingLevel = Sensor.RecordingLevel.forName(
                            config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)!!
                        )
                        tags = metricTags
                    }
                val metricsContext = KafkaMetricsContext(
                    namespace = JMX_PREFIX,
                    contextLabels =
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX),
                )
                metrics = Metrics(
                    config = metricConfig,
                    reporters = reporters.toMutableList(),
                    time = time,
                    metricsContext = metricsContext,
                )
                networkClient = createNetworkClient(
                    config = config,
                    clientId = clientId,
                    metrics = metrics,
                    metricsGroupPrefix = "admin-client",
                    logContext = logContext,
                    apiVersions = apiVersions,
                    time = time,
                    maxInFlightRequestsPerConnection = 1,
                    requestTimeoutMs = TimeUnit.HOURS.toMillis(1).toInt(),
                    metadataUpdater = metadataManager.updater(),
                    hostResolver = hostResolver ?: DefaultHostResolver(),
                )

                return KafkaAdminClient(
                    config = config,
                    clientId = clientId,
                    time = time,
                    metadataManager = metadataManager,
                    metrics = metrics,
                    client = networkClient,
                    timeoutProcessorFactory = timeoutProcessorFactory,
                    logContext = logContext
                )
            } catch (exc: Throwable) {
                Utils.closeQuietly(metrics, "Metrics")
                Utils.closeQuietly(networkClient, "NetworkClient")
                throw KafkaException("Failed to create new KafkaAdminClient", exc)
            }
        }

        fun createInternal(
            config: AdminClientConfig,
            metadataManager: AdminMetadataManager,
            client: KafkaClient,
            time: Time,
        ): KafkaAdminClient {
            var metrics: Metrics? = null
            val clientId = generateClientId(config)
            try {
                metrics = Metrics(
                    config = MetricConfig(),
                    reporters = LinkedList(),
                    time = time,
                )
                val logContext = createLogContext(clientId)
                return KafkaAdminClient(
                    config = config,
                    clientId = clientId,
                    time = time,
                    metadataManager = metadataManager,
                    metrics = metrics,
                    client = client,
                    logContext = logContext,
                )
            } catch (exc: Throwable) {
                Utils.closeQuietly(metrics, "Metrics")
                throw KafkaException("Failed to create new KafkaAdminClient", exc)
            }
        }

        fun createLogContext(clientId: String): LogContext =
            LogContext("[AdminClient clientId=$clientId] ")

        /**
         * Returns true if a topic name cannot be represented in an RPC. This function does NOT
         * check whether the name is too long, contains invalid characters, etc. It is better to
         * enforce those policies on the server, so that they can be changed in the future if
         * needed.
         */
        private fun topicNameIsUnrepresentable(topicName: String?): Boolean =
            topicName.isNullOrEmpty()

        private fun topicIdIsUnrepresentable(topicId: Uuid?): Boolean =
            topicId == null || topicId === Uuid.ZERO_UUID

        /**
         * Fail futures in the given stream which are not done. Used when a response handler
         * expected a result for some entity but no result was present.
         */
        private fun <K, V> completeUnrealizedFutures(
            futures: Map<K, KafkaFutureImpl<V>?>,
            messageFormatter: (K) -> String,
        ) = futures.filterValues { value -> value?.isDone == false }.forEach { (key, value) ->
            value!!.completeExceptionally(ApiException(messageFormatter(key)))
        }

        /**
         * Fail futures in the given Map which were retried due to exceeding quota. We propagate
         * the initial error back to the caller if the request timed out.
         */
        private fun <K, V> maybeCompleteQuotaExceededException(
            shouldRetryOnQuotaViolation: Boolean,
            throwable: Throwable,
            futures: Map<K, KafkaFutureImpl<V>>,
            quotaExceededExceptions: Map<K, ThrottlingQuotaExceededException>,
            throttleTimeDelta: Int,
        ) {
            if (shouldRetryOnQuotaViolation && throwable is TimeoutException) {
                quotaExceededExceptions.forEach { (key, value) ->
                    futures[key]!!.completeExceptionally(
                        ThrottlingQuotaExceededException(
                            throttleTimeMs = (value.throttleTimeMs - throttleTimeDelta)
                                .coerceAtLeast(0),
                            message = value.message,
                        )
                    )
                }
            }
        }

        private fun logDirDescriptions(
            response: DescribeLogDirsResponse,
        ): Map<String, LogDirDescription> {
            val result: MutableMap<String, LogDirDescription> =
                HashMap(response.data().results.size)

            for (logDirResult in response.data().results) {
                val replicaInfoMap = mutableMapOf<TopicPartition, ReplicaInfo>()

                for (t in logDirResult.topics)
                    for (p in t.partitions)
                        replicaInfoMap[TopicPartition(t.name, p.partitionIndex)] =
                            ReplicaInfo(p.partitionSize, p.offsetLag, p.isFutureKey)

                result[logDirResult.logDir] = LogDirDescription(
                    Errors.forCode(logDirResult.errorCode).exception,
                    replicaInfoMap,
                    logDirResult.totalBytes,
                    logDirResult.usableBytes
                )
            }
            return result
        }

        @Throws(InvalidKeyException::class, NoSuchAlgorithmException::class)
        private fun getScramCredentialUpsertion(
            upsertion: UserScramCredentialUpsertion,
        ): ScramCredentialUpsertion {
            val retval = ScramCredentialUpsertion()
            return retval.setName(upsertion.user)
                .setMechanism(upsertion.credentialInfo.mechanism.type)
                .setIterations(upsertion.credentialInfo.iterations)
                .setSalt(upsertion.salt)
                .setSaltedPassword(
                    getSaltedPassword(
                        upsertion.credentialInfo.mechanism,
                        upsertion.password,
                        upsertion.salt,
                        upsertion.credentialInfo.iterations
                    )
                )
        }

        private fun getScramCredentialDeletion(
            deletion: UserScramCredentialDeletion,
        ): ScramCredentialDeletion = ScramCredentialDeletion()
            .setName(deletion.user)
            .setMechanism(deletion.mechanism.type)

        @Throws(NoSuchAlgorithmException::class, InvalidKeyException::class)
        private fun getSaltedPassword(
            publicScramMechanism: ScramMechanism,
            password: ByteArray,
            salt: ByteArray,
            iterations: Int,
        ): ByteArray = ScramFormatter(
            org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(
                publicScramMechanism.mechanismName,
            )!!
        ).hi(password, salt, iterations)

        /**
         * Get a sub-level error when the request is in batch. If given key was not found, return an
         * [IllegalArgumentException].
         */
        fun <K> getSubLevelError(
            subLevelErrors: Map<K, Errors>,
            subKey: K,
            keyNotFoundMsg: String,
        ): Throwable? = subLevelErrors.getOrElse(subKey) {
            return IllegalArgumentException(keyNotFoundMsg)
        }.exception

        private fun getOffsetFromSpec(offsetSpec: OffsetSpec): Long {
            return when (offsetSpec) {
                is TimestampSpec -> offsetSpec.timestamp
                is EarliestSpec -> ListOffsetsRequest.EARLIEST_TIMESTAMP
                is MaxTimestampSpec -> ListOffsetsRequest.MAX_TIMESTAMP
                else -> ListOffsetsRequest.LATEST_TIMESTAMP
            }
        }
    }
}
