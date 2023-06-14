package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.protocol.SendBuilder

abstract class AbstractResponse protected constructor(
    val apiKey: ApiKeys,
) : AbstractRequestResponse {

    fun toSend(header: ResponseHeader?, version: Short): Send {
        return SendBuilder.buildResponseSend(header, data(), version)
    }

    /**
     * Serializes header and body without prefixing with size (unlike `toSend`, which does include a size prefix).
     */
    fun serializeWithHeader(header: ResponseHeader, version: Short): ByteBuffer {
        return RequestUtils.serialize(header.data(), header.headerVersion(), data(), version)
    }

    // Visible for testing
    fun serialize(version: Short): ByteBuffer {
        return MessageUtil.toByteBuffer(data(), version)
    }

    /**
     * The number of each type of error in the response, including [Errors.NONE] and top-level errors as well as
     * more specifically scoped errors (such as topic or partition-level errors).
     * @return A count of errors.
     */
    abstract fun errorCounts(): Map<Errors, Int>

    protected fun errorCounts(error: Errors): Map<Errors, Int> {
        return Collections.singletonMap(error, 1)
    }

    protected fun errorCounts(errors: Stream<Errors>): Map<Errors, Int> {
        return errors.collect(
            Collectors.groupingBy({ e: Errors -> e }, Collectors.summingInt { 1 })
        )
    }

    protected fun errorCounts(errors: Collection<Errors>): Map<Errors, Int> {
        val errorCounts: MutableMap<Errors, Int> = EnumMap(Errors::class.java)
        for (error in errors) updateErrorCounts(errorCounts, error)
        return errorCounts
    }

    protected fun apiErrorCounts(errors: Map<*, ApiError>): Map<Errors, Int> {
        val errorCounts: MutableMap<Errors, Int> = EnumMap(Errors::class.java)
        for (apiError in errors.values) updateErrorCounts(errorCounts, apiError.error())
        return errorCounts
    }

    protected fun updateErrorCounts(errorCounts: MutableMap<Errors, Int>, error: Errors) {
        val count = errorCounts.getOrDefault(error, 0)
        errorCounts[error] = count + 1
    }

    /**
     * Returns whether or not client should throttle upon receiving a response of the specified version with a non-zero
     * throttle time. Client-side throttling is needed when communicating with a newer version of broker which, on
     * quota violation, sends out responses before throttling.
     */
    open fun shouldClientThrottle(version: Short): Boolean = false

    /**
     * Get the throttle time in milliseconds. If the response schema does not
     * support this field, then 0 will be returned.
     */
    abstract fun throttleTimeMs(): Int

    /**
     * Set the throttle time in the response if the schema supports it. Otherwise,
     * this is a no-op.
     *
     * @param throttleTimeMs The throttle time in milliseconds
     */
    abstract fun maybeSetThrottleTimeMs(throttleTimeMs: Int)

    override fun toString(): String = data().toString()

    companion object {
        const val DEFAULT_THROTTLE_TIME = 0

        /**
         * Parse a response from the provided buffer. The buffer is expected to hold both
         * the [ResponseHeader] as well as the response payload.
         */
        fun parseResponse(buffer: ByteBuffer, requestHeader: RequestHeader): AbstractResponse {
            val apiKey = requestHeader.apiKey()
            val apiVersion = requestHeader.apiVersion()
            val responseHeader = ResponseHeader.parse(buffer, apiKey.responseHeaderVersion(apiVersion))
            if (requestHeader.correlationId() != responseHeader.correlationId()) {
                throw CorrelationIdMismatchException(
                    "Correlation id for response (${responseHeader.correlationId()}) does not match request (" +
                            "${requestHeader.correlationId()}), request header: $requestHeader",
                    requestHeader.correlationId(), responseHeader.correlationId()
                )
            }
            return parseResponse(apiKey, buffer, apiVersion)
        }

        fun parseResponse(
            apiKey: ApiKeys,
            responseBuffer: ByteBuffer,
            version: Short,
        ): AbstractResponse {
            return when (apiKey) {
                ApiKeys.PRODUCE -> ProduceResponse.parse(responseBuffer, version)
                ApiKeys.FETCH -> FetchResponse.parse(responseBuffer, version)
                ApiKeys.LIST_OFFSETS -> ListOffsetsResponse.parse(responseBuffer, version)
                ApiKeys.METADATA -> MetadataResponse.parse(responseBuffer, version)
                ApiKeys.OFFSET_COMMIT -> OffsetCommitResponse.parse(responseBuffer, version)
                ApiKeys.OFFSET_FETCH -> OffsetFetchResponse.parse(responseBuffer, version)
                ApiKeys.FIND_COORDINATOR -> FindCoordinatorResponse.parse(responseBuffer, version)
                ApiKeys.JOIN_GROUP -> JoinGroupResponse.parse(responseBuffer, version)
                ApiKeys.HEARTBEAT -> HeartbeatResponse.parse(responseBuffer, version)
                ApiKeys.LEAVE_GROUP -> LeaveGroupResponse.parse(responseBuffer, version)
                ApiKeys.SYNC_GROUP -> SyncGroupResponse.parse(responseBuffer, version)
                ApiKeys.STOP_REPLICA -> StopReplicaResponse.parse(responseBuffer, version)
                ApiKeys.CONTROLLED_SHUTDOWN -> ControlledShutdownResponse.parse(responseBuffer, version)
                ApiKeys.UPDATE_METADATA -> UpdateMetadataResponse.parse(responseBuffer, version)
                ApiKeys.LEADER_AND_ISR -> LeaderAndIsrResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_GROUPS -> DescribeGroupsResponse.parse(responseBuffer, version)
                ApiKeys.LIST_GROUPS -> ListGroupsResponse.parse(responseBuffer, version)
                ApiKeys.SASL_HANDSHAKE -> SaslHandshakeResponse.parse(responseBuffer, version)
                ApiKeys.API_VERSIONS -> ApiVersionsResponse.parse(responseBuffer, version)
                ApiKeys.CREATE_TOPICS -> CreateTopicsResponse.parse(responseBuffer, version)
                ApiKeys.DELETE_TOPICS -> DeleteTopicsResponse.parse(responseBuffer, version)
                ApiKeys.DELETE_RECORDS -> DeleteRecordsResponse.parse(responseBuffer, version)
                ApiKeys.INIT_PRODUCER_ID -> InitProducerIdResponse.parse(responseBuffer, version)
                ApiKeys.OFFSET_FOR_LEADER_EPOCH -> OffsetsForLeaderEpochResponse.parse(responseBuffer, version)
                ApiKeys.ADD_PARTITIONS_TO_TXN -> AddPartitionsToTxnResponse.parse(responseBuffer, version)
                ApiKeys.ADD_OFFSETS_TO_TXN -> AddOffsetsToTxnResponse.parse(responseBuffer, version)
                ApiKeys.END_TXN -> EndTxnResponse.parse(responseBuffer, version)
                ApiKeys.WRITE_TXN_MARKERS -> WriteTxnMarkersResponse.parse(responseBuffer, version)
                ApiKeys.TXN_OFFSET_COMMIT -> TxnOffsetCommitResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_ACLS -> DescribeAclsResponse.parse(responseBuffer, version)
                ApiKeys.CREATE_ACLS -> CreateAclsResponse.parse(responseBuffer, version)
                ApiKeys.DELETE_ACLS -> DeleteAclsResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_CONFIGS -> DescribeConfigsResponse.parse(responseBuffer, version)
                ApiKeys.ALTER_CONFIGS -> AlterConfigsResponse.parse(responseBuffer, version)
                ApiKeys.ALTER_REPLICA_LOG_DIRS -> AlterReplicaLogDirsResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_LOG_DIRS -> DescribeLogDirsResponse.parse(responseBuffer, version)
                ApiKeys.SASL_AUTHENTICATE -> SaslAuthenticateResponse.parse(responseBuffer, version)
                ApiKeys.CREATE_PARTITIONS -> CreatePartitionsResponse.parse(responseBuffer, version)
                ApiKeys.CREATE_DELEGATION_TOKEN -> CreateDelegationTokenResponse.parse(responseBuffer, version)
                ApiKeys.RENEW_DELEGATION_TOKEN -> RenewDelegationTokenResponse.parse(responseBuffer, version)
                ApiKeys.EXPIRE_DELEGATION_TOKEN -> ExpireDelegationTokenResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_DELEGATION_TOKEN -> DescribeDelegationTokenResponse.parse(responseBuffer, version)
                ApiKeys.DELETE_GROUPS -> DeleteGroupsResponse.parse(responseBuffer, version)
                ApiKeys.ELECT_LEADERS -> ElectLeadersResponse.parse(responseBuffer, version)
                ApiKeys.INCREMENTAL_ALTER_CONFIGS -> IncrementalAlterConfigsResponse.parse(responseBuffer, version)
                ApiKeys.ALTER_PARTITION_REASSIGNMENTS ->
                    AlterPartitionReassignmentsResponse.parse(responseBuffer, version)

                ApiKeys.LIST_PARTITION_REASSIGNMENTS ->
                    ListPartitionReassignmentsResponse.parse(responseBuffer, version)

                ApiKeys.OFFSET_DELETE -> OffsetDeleteResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_CLIENT_QUOTAS -> DescribeClientQuotasResponse.parse(responseBuffer, version)
                ApiKeys.ALTER_CLIENT_QUOTAS -> AlterClientQuotasResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS -> DescribeUserScramCredentialsResponse.parse(
                    responseBuffer,
                    version
                )

                ApiKeys.ALTER_USER_SCRAM_CREDENTIALS -> AlterUserScramCredentialsResponse.parse(responseBuffer, version)
                ApiKeys.VOTE -> VoteResponse.parse(responseBuffer, version)
                ApiKeys.BEGIN_QUORUM_EPOCH -> BeginQuorumEpochResponse.parse(responseBuffer, version)
                ApiKeys.END_QUORUM_EPOCH -> EndQuorumEpochResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_QUORUM -> DescribeQuorumResponse.parse(responseBuffer, version)
                ApiKeys.ALTER_PARTITION -> AlterPartitionResponse.parse(responseBuffer, version)
                ApiKeys.UPDATE_FEATURES -> UpdateFeaturesResponse.parse(responseBuffer, version)
                ApiKeys.ENVELOPE -> EnvelopeResponse.parse(responseBuffer, version)
                ApiKeys.FETCH_SNAPSHOT -> FetchSnapshotResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_CLUSTER -> DescribeClusterResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_PRODUCERS -> DescribeProducersResponse.parse(responseBuffer, version)
                ApiKeys.BROKER_REGISTRATION -> BrokerRegistrationResponse.parse(responseBuffer, version)
                ApiKeys.BROKER_HEARTBEAT -> BrokerHeartbeatResponse.parse(responseBuffer, version)
                ApiKeys.UNREGISTER_BROKER -> UnregisterBrokerResponse.parse(responseBuffer, version)
                ApiKeys.DESCRIBE_TRANSACTIONS -> DescribeTransactionsResponse.parse(responseBuffer, version)
                ApiKeys.LIST_TRANSACTIONS -> ListTransactionsResponse.parse(responseBuffer, version)
                ApiKeys.ALLOCATE_PRODUCER_IDS -> AllocateProducerIdsResponse.parse(responseBuffer, version)
                else -> throw AssertionError(
                    String.format(
                        "ApiKey %s is not currently handled in `parseResponse`, the code should be updated to do so.",
                        apiKey,
                    )
                )
            }
        }
    }
}
