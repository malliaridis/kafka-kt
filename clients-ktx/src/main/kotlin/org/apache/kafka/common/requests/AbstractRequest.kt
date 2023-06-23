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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.protocol.SendBuilder

abstract class AbstractRequest(
    val apiKey: ApiKeys,
    val version: Short,
) : AbstractRequestResponse {

    /**
     * @constructor Construct a new builder which allows any supported version
     */
    abstract class Builder<T : AbstractRequest>(
        val apiKey: ApiKeys,
        open val oldestAllowedVersion: Short = apiKey.oldestVersion(),
        open val latestAllowedVersion: Short = apiKey.latestVersion(),
    ) {

        /**
         * Construct a new builder which allows only a specific version
         */
        constructor(apiKey: ApiKeys, allowedVersion: Short) : this(
            apiKey = apiKey,
            oldestAllowedVersion = allowedVersion,
            latestAllowedVersion = allowedVersion
        )

        /**
         * Construct a new builder which allows an inclusive range of versions
         */
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("apiKey")
        )
        fun apiKey(): ApiKeys = apiKey

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("oldestAllowedVersion")
        )
        open fun oldestAllowedVersion(): Short = oldestAllowedVersion

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("latestAllowedVersion")
        )
        open fun latestAllowedVersion(): Short = latestAllowedVersion

        fun build(): T = build(latestAllowedVersion)

        abstract fun build(version: Short): T
    }

    init {
        if (!apiKey.isVersionSupported(version)) throw UnsupportedVersionException(
            "The $apiKey protocol does not support version $version"
        )
    }

    /**
     * Get the version of this AbstractRequest object.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("version")
    )
    fun version(): Short = version

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("apiKey")
    )
    fun apiKey(): ApiKeys = apiKey

    fun toSend(header: RequestHeader): Send {
        return SendBuilder.buildRequestSend(header, data())
    }

    /**
     * Serializes header and body without prefixing with size (unlike `toSend`, which does include a
     * size prefix).
     */
    fun serializeWithHeader(header: RequestHeader): ByteBuffer {
        require(header.apiKey() === apiKey) {
            "Could not build request $apiKey with header api key ${header.apiKey()}"
        }
        require(header.apiVersion() == version) {
            "Could not build request version $version with header version ${header.apiVersion()}"
        }

        return RequestUtils.serialize(header.data(), header.headerVersion(), data(), version)
    }

    // Visible for testing
    fun serialize(): ByteBuffer = toByteBuffer(data(), version)

    // Visible for testing
    fun sizeInBytes(): Int = data().size(ObjectSerializationCache(), version)

    open fun toString(verbose: Boolean): String = data().toString()

    override fun toString(): String = toString(true)

    /**
     * Get an error response for a request
     */
    fun getErrorResponse(e: Throwable): AbstractResponse? {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e)
    }

    /**
     * Get an error response for a request with specified throttle time in the response if
     * applicable.
     */
    abstract fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse?

    /**
     * Get the error counts corresponding to an error response. This is overridden for requests
     * where response may be `null` (e.g produce with acks=0).
     */
    open fun errorCounts(e: Throwable): Map<Errors, Int> {
        return getErrorResponse(0, e)?.errorCounts()
            ?: error("Error counts could not be obtained for request $this")
    }

    companion object {

        /**
         * Factory method for getting a request object based on ApiKey ID and a version
         */
        fun parseRequest(apiKey: ApiKeys, apiVersion: Short, buffer: ByteBuffer): RequestAndSize {
            val bufferSize = buffer.remaining()
            return RequestAndSize(doParseRequest(apiKey, apiVersion, buffer), bufferSize)
        }

        private fun doParseRequest(
            apiKey: ApiKeys,
            apiVersion: Short,
            buffer: ByteBuffer,
        ): AbstractRequest = when (apiKey) {
            ApiKeys.PRODUCE -> ProduceRequest.parse(buffer, apiVersion)
            ApiKeys.FETCH -> FetchRequest.parse(buffer, apiVersion)
            ApiKeys.LIST_OFFSETS -> ListOffsetsRequest.parse(buffer, apiVersion)
            ApiKeys.METADATA -> MetadataRequest.parse(buffer, apiVersion)
            ApiKeys.OFFSET_COMMIT -> OffsetCommitRequest.parse(buffer, apiVersion)
            ApiKeys.OFFSET_FETCH -> OffsetFetchRequest.parse(buffer, apiVersion)
            ApiKeys.FIND_COORDINATOR -> FindCoordinatorRequest.parse(buffer, apiVersion)
            ApiKeys.JOIN_GROUP -> JoinGroupRequest.parse(buffer, apiVersion)
            ApiKeys.HEARTBEAT -> HeartbeatRequest.parse(buffer, apiVersion)
            ApiKeys.LEAVE_GROUP -> LeaveGroupRequest.parse(buffer, apiVersion)
            ApiKeys.SYNC_GROUP -> SyncGroupRequest.parse(buffer, apiVersion)
            ApiKeys.STOP_REPLICA -> StopReplicaRequest.parse(buffer, apiVersion)
            ApiKeys.CONTROLLED_SHUTDOWN -> ControlledShutdownRequest.parse(buffer, apiVersion)
            ApiKeys.UPDATE_METADATA -> UpdateMetadataRequest.parse(buffer, apiVersion)
            ApiKeys.LEADER_AND_ISR -> LeaderAndIsrRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_GROUPS -> DescribeGroupsRequest.parse(buffer, apiVersion)
            ApiKeys.LIST_GROUPS -> ListGroupsRequest.parse(buffer, apiVersion)
            ApiKeys.SASL_HANDSHAKE -> SaslHandshakeRequest.parse(buffer, apiVersion)
            ApiKeys.API_VERSIONS -> ApiVersionsRequest.parse(buffer, apiVersion)
            ApiKeys.CREATE_TOPICS -> CreateTopicsRequest.parse(buffer, apiVersion)
            ApiKeys.DELETE_TOPICS -> DeleteTopicsRequest.parse(buffer, apiVersion)
            ApiKeys.DELETE_RECORDS -> DeleteRecordsRequest.parse(buffer, apiVersion)
            ApiKeys.INIT_PRODUCER_ID -> InitProducerIdRequest.parse(buffer, apiVersion)
            ApiKeys.OFFSET_FOR_LEADER_EPOCH -> OffsetsForLeaderEpochRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.ADD_PARTITIONS_TO_TXN -> AddPartitionsToTxnRequest.parse(buffer, apiVersion)
            ApiKeys.ADD_OFFSETS_TO_TXN -> AddOffsetsToTxnRequest.parse(buffer, apiVersion)
            ApiKeys.END_TXN -> EndTxnRequest.parse(buffer, apiVersion)
            ApiKeys.WRITE_TXN_MARKERS -> WriteTxnMarkersRequest.parse(buffer, apiVersion)
            ApiKeys.TXN_OFFSET_COMMIT -> TxnOffsetCommitRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_ACLS -> DescribeAclsRequest.parse(buffer, apiVersion)
            ApiKeys.CREATE_ACLS -> CreateAclsRequest.parse(buffer, apiVersion)
            ApiKeys.DELETE_ACLS -> DeleteAclsRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_CONFIGS -> DescribeConfigsRequest.parse(buffer, apiVersion)
            ApiKeys.ALTER_CONFIGS -> AlterConfigsRequest.parse(buffer, apiVersion)
            ApiKeys.ALTER_REPLICA_LOG_DIRS -> AlterReplicaLogDirsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.DESCRIBE_LOG_DIRS -> DescribeLogDirsRequest.parse(buffer, apiVersion)
            ApiKeys.SASL_AUTHENTICATE -> SaslAuthenticateRequest.parse(buffer, apiVersion)
            ApiKeys.CREATE_PARTITIONS -> CreatePartitionsRequest.parse(buffer, apiVersion)
            ApiKeys.CREATE_DELEGATION_TOKEN -> CreateDelegationTokenRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.RENEW_DELEGATION_TOKEN -> RenewDelegationTokenRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.EXPIRE_DELEGATION_TOKEN -> ExpireDelegationTokenRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.DESCRIBE_DELEGATION_TOKEN -> DescribeDelegationTokenRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.DELETE_GROUPS -> DeleteGroupsRequest.parse(buffer, apiVersion)
            ApiKeys.ELECT_LEADERS -> ElectLeadersRequest.parse(buffer, apiVersion)
            ApiKeys.INCREMENTAL_ALTER_CONFIGS -> IncrementalAlterConfigsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.ALTER_PARTITION_REASSIGNMENTS -> AlterPartitionReassignmentsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.LIST_PARTITION_REASSIGNMENTS -> ListPartitionReassignmentsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.OFFSET_DELETE -> OffsetDeleteRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_CLIENT_QUOTAS -> DescribeClientQuotasRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.ALTER_CLIENT_QUOTAS -> AlterClientQuotasRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS -> DescribeUserScramCredentialsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS -> AlterUserScramCredentialsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.VOTE -> VoteRequest.parse(buffer, apiVersion)
            ApiKeys.BEGIN_QUORUM_EPOCH -> BeginQuorumEpochRequest.parse(buffer, apiVersion)
            ApiKeys.END_QUORUM_EPOCH -> EndQuorumEpochRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_QUORUM -> DescribeQuorumRequest.parse(buffer, apiVersion)
            ApiKeys.ALTER_PARTITION -> AlterPartitionRequest.parse(buffer, apiVersion)
            ApiKeys.UPDATE_FEATURES -> UpdateFeaturesRequest.parse(buffer, apiVersion)
            ApiKeys.ENVELOPE -> EnvelopeRequest.parse(buffer, apiVersion)
            ApiKeys.FETCH_SNAPSHOT -> FetchSnapshotRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_CLUSTER -> DescribeClusterRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_PRODUCERS -> DescribeProducersRequest.parse(buffer, apiVersion)
            ApiKeys.BROKER_REGISTRATION -> BrokerRegistrationRequest.parse(buffer, apiVersion)
            ApiKeys.BROKER_HEARTBEAT -> BrokerHeartbeatRequest.parse(buffer, apiVersion)
            ApiKeys.UNREGISTER_BROKER -> UnregisterBrokerRequest.parse(buffer, apiVersion)
            ApiKeys.DESCRIBE_TRANSACTIONS -> DescribeTransactionsRequest.parse(
                buffer,
                apiVersion
            )

            ApiKeys.LIST_TRANSACTIONS -> ListTransactionsRequest.parse(buffer, apiVersion)
            ApiKeys.ALLOCATE_PRODUCER_IDS -> AllocateProducerIdsRequest.parse(
                buffer,
                apiVersion
            )
        }
    }
}
