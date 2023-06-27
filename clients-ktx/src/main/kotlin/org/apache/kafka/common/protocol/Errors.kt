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

package org.apache.kafka.common.protocol

import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.ConcurrentTransactionsException
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException
import org.apache.kafka.common.errors.CoordinatorNotAvailableException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.errors.DelegationTokenAuthorizationException
import org.apache.kafka.common.errors.DelegationTokenDisabledException
import org.apache.kafka.common.errors.DelegationTokenExpiredException
import org.apache.kafka.common.errors.DelegationTokenNotFoundException
import org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException
import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException
import org.apache.kafka.common.errors.DuplicateResourceException
import org.apache.kafka.common.errors.DuplicateSequenceException
import org.apache.kafka.common.errors.ElectionNotNeededException
import org.apache.kafka.common.errors.EligibleLeadersNotAvailableException
import org.apache.kafka.common.errors.FeatureUpdateFailedException
import org.apache.kafka.common.errors.FencedInstanceIdException
import org.apache.kafka.common.errors.FencedLeaderEpochException
import org.apache.kafka.common.errors.FetchSessionIdNotFoundException
import org.apache.kafka.common.errors.FetchSessionTopicIdException
import org.apache.kafka.common.errors.GroupAuthorizationException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.errors.GroupSubscribedToTopicException
import org.apache.kafka.common.errors.IllegalGenerationException
import org.apache.kafka.common.errors.IllegalSaslStateException
import org.apache.kafka.common.errors.InconsistentClusterIdException
import org.apache.kafka.common.errors.InconsistentGroupProtocolException
import org.apache.kafka.common.errors.InconsistentTopicIdException
import org.apache.kafka.common.errors.InconsistentVoterSetException
import org.apache.kafka.common.errors.IneligibleReplicaException
import org.apache.kafka.common.errors.InvalidCommitOffsetSizeException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.InvalidFetchSessionEpochException
import org.apache.kafka.common.errors.InvalidFetchSizeException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.errors.InvalidPartitionsException
import org.apache.kafka.common.errors.InvalidPidMappingException
import org.apache.kafka.common.errors.InvalidPrincipalTypeException
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.errors.InvalidRequiredAcksException
import org.apache.kafka.common.errors.InvalidSessionTimeoutException
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.InvalidTxnStateException
import org.apache.kafka.common.errors.InvalidTxnTimeoutException
import org.apache.kafka.common.errors.InvalidUpdateVersionException
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.errors.LeaderNotAvailableException
import org.apache.kafka.common.errors.ListenerNotFoundException
import org.apache.kafka.common.errors.LogDirNotFoundException
import org.apache.kafka.common.errors.MemberIdRequiredException
import org.apache.kafka.common.errors.NetworkException
import org.apache.kafka.common.errors.NewLeaderElectedException
import org.apache.kafka.common.errors.NoReassignmentInProgressException
import org.apache.kafka.common.errors.NotControllerException
import org.apache.kafka.common.errors.NotCoordinatorException
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.errors.OffsetMetadataTooLarge
import org.apache.kafka.common.errors.OffsetNotAvailableException
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.errors.OperationNotAttemptedException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.errors.PositionOutOfRangeException
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException
import org.apache.kafka.common.errors.PrincipalDeserializationException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.ReassignmentInProgressException
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.errors.ResourceNotFoundException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.SecurityDisabledException
import org.apache.kafka.common.errors.SnapshotNotFoundException
import org.apache.kafka.common.errors.StaleBrokerEpochException
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TopicDeletionDisabledException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException
import org.apache.kafka.common.errors.TransactionalIdNotFoundException
import org.apache.kafka.common.errors.UnacceptableCredentialException
import org.apache.kafka.common.errors.UnknownLeaderEpochException
import org.apache.kafka.common.errors.UnknownMemberIdException
import org.apache.kafka.common.errors.UnknownProducerIdException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.UnknownTopicIdException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.errors.UnstableOffsetCommitException
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.slf4j.LoggerFactory

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Note that client library will convert an unknown error code to the non-retriable
 * UnknownServerException if the client library version is old and does not recognize the
 * newly-added error code. Therefore, when a new server-side error is added, we may need extra logic
 * to convert the new error code to another existing error code before sending the response back to
 * the client if the request version suggests that the client may not recognize the new error code.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 *
 * @see org.apache.kafka.common.network.SslTransportLayer
 */
enum class Errors(
    code: Int,
    defaultExceptionString: String?,
    private val builder: (String?) -> ApiException?
) {

    UNKNOWN_SERVER_ERROR(
        code = -1,
        defaultExceptionString = "The server experienced an unexpected error when processing the request.",
        builder = { message -> UnknownServerException(message = message) },
    ),
    NONE(
        code = 0,
        defaultExceptionString = null,
        builder = { null },
    ),
    OFFSET_OUT_OF_RANGE(
        code = 1,
        defaultExceptionString = "The requested offset is not within the range of offsets maintained by the server.",
        builder = { message -> OffsetOutOfRangeException(message = message) },
    ),
    CORRUPT_MESSAGE(
        code = 2,
        defaultExceptionString = "This message has failed its CRC checksum, exceeds the valid size, " +
                "has a null key for a compacted topic, or is otherwise corrupt.",
        builder = { message -> CorruptRecordException(message = message) },
    ),
    UNKNOWN_TOPIC_OR_PARTITION(
        code = 3,
        defaultExceptionString = "This server does not host this topic-partition.",
        builder = { message -> UnknownTopicOrPartitionException(message = message) },
    ),
    INVALID_FETCH_SIZE(
        code = 4,
        defaultExceptionString = "The requested fetch size is invalid.",
        builder = { message -> InvalidFetchSizeException(message = message) },
    ),
    LEADER_NOT_AVAILABLE(
        code = 5,
        defaultExceptionString = "There is no leader for this topic-partition as we are in the " +
                "middle of a leadership election.",
        builder = { message -> LeaderNotAvailableException(message = message) },
    ),
    NOT_LEADER_OR_FOLLOWER(
        code = 6,
        defaultExceptionString = "For requests intended only for the leader, this error indicates " +
                "that the broker is not the current leader. For requests intended for any replica, " +
                "this error indicates that the broker is not a replica of the topic partition.",
        builder = { message -> NotLeaderOrFollowerException(message = message) },
    ),
    REQUEST_TIMED_OUT(
        code = 7, "The request timed out.",
        builder = { message -> TimeoutException(message = message) },
    ),
    BROKER_NOT_AVAILABLE(
        code = 8, "The broker is not available.",
        builder = { message -> BrokerNotAvailableException(message = message) },
    ),
    REPLICA_NOT_AVAILABLE(
        code = 9,
        defaultExceptionString = "The replica is not available for the requested topic-partition. " +
                "Produce/Fetch requests and other requests intended only for the leader or follower " +
                "return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
        builder = { message -> ReplicaNotAvailableException(message = message) },
    ),
    MESSAGE_TOO_LARGE(
        code = 10,
        defaultExceptionString = "The request included a message larger than the max message size " +
                "the server will accept.",
        builder = { message -> RecordTooLargeException(message = message) },
    ),
    STALE_CONTROLLER_EPOCH(
        code = 11,
        defaultExceptionString = "The controller moved to another broker.",
        builder = { message -> ControllerMovedException(message = message) },
    ),
    OFFSET_METADATA_TOO_LARGE(
        code = 12,
        defaultExceptionString = "The metadata field of the offset request was too large.",
        builder = { message -> OffsetMetadataTooLarge(message = message) },
    ),
    NETWORK_EXCEPTION(
        code = 13,
        defaultExceptionString = "The server disconnected before a response was received.",
        builder = { message -> NetworkException(message = message) },
    ),
    COORDINATOR_LOAD_IN_PROGRESS(
        code = 14,
        defaultExceptionString = "The coordinator is loading and hence can't process requests.",
        builder = { message -> CoordinatorLoadInProgressException(message = message) },
    ),
    COORDINATOR_NOT_AVAILABLE(
        code = 15,
        defaultExceptionString = "The coordinator is not available.",
        builder = { message -> CoordinatorNotAvailableException(message = message) },
    ),
    NOT_COORDINATOR(
        code = 16,
        defaultExceptionString = "This is not the correct coordinator.",
        builder = { message -> NotCoordinatorException(message = message) },
    ),
    INVALID_TOPIC_EXCEPTION(
        code = 17,
        defaultExceptionString = "The request attempted to perform an operation on an invalid topic.",
        builder = { message -> InvalidTopicException(message = message) },
    ),
    RECORD_LIST_TOO_LARGE(
        code = 18,
        defaultExceptionString = "The request included message batch larger than the configured " +
                "segment size on the server.",
        builder = { message -> RecordBatchTooLargeException(message = message) },
    ),
    NOT_ENOUGH_REPLICAS(
        code = 19,
        defaultExceptionString = "Messages are rejected since there are fewer in-sync replicas than required.",
        builder = { message -> NotEnoughReplicasException(message = message) },
    ),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(
        code = 20,
        defaultExceptionString = "Messages are written to the log, but to fewer in-sync replicas than required.",
        builder = { message -> NotEnoughReplicasAfterAppendException(message = message) },
    ),
    INVALID_REQUIRED_ACKS(
        code = 21,
        defaultExceptionString = "Produce request specified an invalid value for required acks.",
        builder = { message -> InvalidRequiredAcksException(message = message) },
    ),
    ILLEGAL_GENERATION(
        code = 22,
        defaultExceptionString = "Specified group generation id is not valid.",
        builder = { message -> IllegalGenerationException(message = message) },
    ),
    INCONSISTENT_GROUP_PROTOCOL(
        code = 23,
        defaultExceptionString = "The group member's supported protocols are incompatible with " +
                "those of existing members or first group member tried to join with empty protocol " +
                "type or empty protocol list.",
        builder = { message -> InconsistentGroupProtocolException(message = message) },
    ),
    INVALID_GROUP_ID(
        code = 24,
        defaultExceptionString = "The configured groupId is invalid.",
        builder = { message -> InvalidGroupIdException(message = message) },
    ),
    UNKNOWN_MEMBER_ID(
        code = 25,
        defaultExceptionString = "The coordinator is not aware of this member.",
        builder = { message -> UnknownMemberIdException(message = message) },
    ),
    INVALID_SESSION_TIMEOUT(
        code = 26,
        defaultExceptionString = "The session timeout is not within the range allowed by the broker " +
                "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
        builder = { message -> InvalidSessionTimeoutException(message = message) },
    ),
    REBALANCE_IN_PROGRESS(
        code = 27,
        defaultExceptionString = "The group is rebalancing, so a rejoin is needed.",
        builder = { message -> RebalanceInProgressException(message = message) },
    ),
    INVALID_COMMIT_OFFSET_SIZE(
        code = 28,
        defaultExceptionString = "The committing offset data size is not valid.",
        builder = { message -> InvalidCommitOffsetSizeException(message = message) },
    ),
    TOPIC_AUTHORIZATION_FAILED(
        code = 29,
        defaultExceptionString = "Topic authorization failed.",
        builder = { message -> TopicAuthorizationException(message = message) },
    ),
    GROUP_AUTHORIZATION_FAILED(
        code = 30,
        defaultExceptionString = "Group authorization failed.",
        builder = { message -> GroupAuthorizationException(message = message) },
    ),
    CLUSTER_AUTHORIZATION_FAILED(
        code = 31,
        defaultExceptionString = "Cluster authorization failed.",
        builder = { message -> ClusterAuthorizationException(message = message) },
    ),
    INVALID_TIMESTAMP(
        code = 32,
        defaultExceptionString = "The timestamp of the message is out of acceptable range.",
        builder = { message -> InvalidTimestampException(message = message) },
    ),
    UNSUPPORTED_SASL_MECHANISM(
        code = 33,
        defaultExceptionString = "The broker does not support the requested SASL mechanism.",
        builder = { message -> UnsupportedSaslMechanismException(message = message) },
    ),
    ILLEGAL_SASL_STATE(
        code = 34,
        defaultExceptionString = "Request is not valid given the current SASL state.",
        builder = { message -> IllegalSaslStateException(message = message) },
    ),
    UNSUPPORTED_VERSION(
        code = 35,
        defaultExceptionString = "The version of API is not supported.",
        builder = { message -> UnsupportedVersionException(message = message) },
    ),
    TOPIC_ALREADY_EXISTS(
        code = 36,
        defaultExceptionString = "Topic with this name already exists.",
        builder = { message -> TopicExistsException(message = message) },
    ),
    INVALID_PARTITIONS(
        code = 37,
        defaultExceptionString = "Number of partitions is below 1.",
        builder = { message -> InvalidPartitionsException(message = message) },
    ),
    INVALID_REPLICATION_FACTOR(
        code = 38,
        defaultExceptionString = "Replication factor is below 1 or larger than the number of available brokers.",
        builder = { message -> InvalidReplicationFactorException(message = message) },
    ),
    INVALID_REPLICA_ASSIGNMENT(
        code = 39,
        defaultExceptionString = "Replica assignment is invalid.",
        builder = { message -> InvalidReplicaAssignmentException(message = message) },
    ),
    INVALID_CONFIG(
        code = 40,
        defaultExceptionString = "Configuration is invalid.",
        builder = { message -> InvalidConfigurationException(message = message) },
    ),
    NOT_CONTROLLER(
        code = 41,
        defaultExceptionString = "This is not the correct controller for this cluster.",
        builder = { message -> NotControllerException(message = message) },
    ),
    INVALID_REQUEST(
        code = 42,
        defaultExceptionString = "This most likely occurs because of a request being malformed by the " +
            "client library or the message was sent to an incompatible broker. See the broker logs " +
            "for more details.",
        builder = { message -> InvalidRequestException(message = message) },
    ),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(
        code = 43,
        defaultExceptionString = "The message format version on the broker does not support the request.",
        builder = { message -> UnsupportedForMessageFormatException(message = message) },
    ),
    POLICY_VIOLATION(
        code = 44,
        defaultExceptionString = "Request parameters do not satisfy the configured policy.",
        builder = { message -> PolicyViolationException(message = message) },
    ),
    OUT_OF_ORDER_SEQUENCE_NUMBER(
        code = 45,
        defaultExceptionString = "The broker received an out of order sequence number.",
        builder = { message -> OutOfOrderSequenceException(message = message) },
    ),
    DUPLICATE_SEQUENCE_NUMBER(
        code = 46,
        defaultExceptionString = "The broker received a duplicate sequence number.",
        builder = { message -> DuplicateSequenceException(message = message) },
    ),
    INVALID_PRODUCER_EPOCH(
        code = 47,
        defaultExceptionString = "Producer attempted to produce with an old epoch.",
        builder = { message -> InvalidProducerEpochException(message = message) },
    ),
    INVALID_TXN_STATE(
        code = 48,
        defaultExceptionString = "The producer attempted a transactional operation in an invalid state.",
        builder = { message -> InvalidTxnStateException(message = message) },
    ),
    INVALID_PRODUCER_ID_MAPPING(
        code = 49,
        defaultExceptionString = "The producer attempted to use a producer id which is not currently assigned to " +
                "its transactional id.",
        builder = { message -> InvalidPidMappingException(message = message) },
    ),
    INVALID_TRANSACTION_TIMEOUT(
        code = 50,
        defaultExceptionString = "The transaction timeout is larger than the maximum value allowed by " +
                "the broker (as configured by transaction.max.timeout.ms).",
        builder = { message -> InvalidTxnTimeoutException(message = message) },
    ),
    CONCURRENT_TRANSACTIONS(
        code = 51,
        defaultExceptionString = "The producer attempted to update a transaction " +
            "while another concurrent operation on the same transaction was ongoing.",
        builder = { message -> ConcurrentTransactionsException(message = message) },
    ),
    TRANSACTION_COORDINATOR_FENCED(
        code = 52,
        defaultExceptionString = "Indicates that the transaction coordinator sending a WriteTxnMarker " +
                "is no longer the current coordinator for a given producer.",
        builder = { message -> TransactionCoordinatorFencedException(message = message) },
    ),
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED(
        code = 53,
        defaultExceptionString = "Transactional Id authorization failed.",
        builder = { message -> TransactionalIdAuthorizationException(message = message) },
    ),
    SECURITY_DISABLED(
        code = 54,
        defaultExceptionString = "Security features are disabled.",
        builder = { message -> SecurityDisabledException(message = message) },
    ),
    OPERATION_NOT_ATTEMPTED(
        code = 55,
        defaultExceptionString = "The broker did not attempt to execute this operation. This may happen for " +
                "batched RPCs where some operations in the batch failed, causing the broker to respond without " +
                "trying the rest.",
        builder = { message -> OperationNotAttemptedException(message = message) },
    ),
    KAFKA_STORAGE_ERROR(
        code = 56,
        defaultExceptionString = "Disk error when trying to access log file on the disk.",
        builder = { message -> KafkaStorageException(message = message) },
    ),
    LOG_DIR_NOT_FOUND(
        code = 57,
        defaultExceptionString = "The user-specified log directory is not found in the broker config.",
        builder = { message -> LogDirNotFoundException(message = message) },
    ),
    SASL_AUTHENTICATION_FAILED(
        code = 58,
        defaultExceptionString = "SASL Authentication failed.",
        builder = { message -> SaslAuthenticationException(message = message) },
    ),
    UNKNOWN_PRODUCER_ID(
        code = 59,
        defaultExceptionString = "This exception is raised by the broker if it could not locate " +
                "the producer metadata associated with the producerId in question. This could " +
                "happen if, for instance, the producer's records were deleted because their " +
                "retention time had elapsed. Once the last records of the producerId are removed, " +
                "the producer's metadata is removed from the broker, and future appends by the " +
                "producer will return this exception.",
        builder = { message -> UnknownProducerIdException(message = message) },
    ),
    REASSIGNMENT_IN_PROGRESS(
        code = 60,
        defaultExceptionString = "A partition reassignment is in progress.",
        builder = { message -> ReassignmentInProgressException(message = message) },
    ),
    DELEGATION_TOKEN_AUTH_DISABLED(
        code = 61, 
        defaultExceptionString = "Delegation Token feature is not enabled.",
        builder = { message -> DelegationTokenDisabledException(message = message) },
    ),
    DELEGATION_TOKEN_NOT_FOUND(
        code = 62, 
        defaultExceptionString = "Delegation Token is not found on server.",
        builder = { message -> DelegationTokenNotFoundException(message = message) },
    ),
    DELEGATION_TOKEN_OWNER_MISMATCH(
        code = 63, 
        defaultExceptionString = "Specified Principal is not valid Owner/Renewer.",
        builder = { message -> DelegationTokenOwnerMismatchException(message = message) },
    ),
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(
        code = 64,
        defaultExceptionString = "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL " +
                "channels and on delegation token authenticated channels.",
        builder = { message -> UnsupportedByAuthenticationException(message = message) },
    ),
    DELEGATION_TOKEN_AUTHORIZATION_FAILED(
        code = 65, 
        defaultExceptionString = "Delegation Token authorization failed.",
        builder = { message -> DelegationTokenAuthorizationException(message = message) },
    ),
    DELEGATION_TOKEN_EXPIRED(
        code = 66, 
        defaultExceptionString = "Delegation Token is expired.",
        builder = { message -> DelegationTokenExpiredException(message = message) },
    ),
    INVALID_PRINCIPAL_TYPE(
        code = 67, 
        defaultExceptionString = "Supplied principalType is not supported.",
        builder = { message -> InvalidPrincipalTypeException(message = message) },
    ),
    NON_EMPTY_GROUP(
        code = 68, 
        defaultExceptionString = "The group is not empty.",
        builder = { message -> GroupNotEmptyException(message = message) },
    ),
    GROUP_ID_NOT_FOUND(
        code = 69, 
        defaultExceptionString = "The group id does not exist.",
        builder = { message -> GroupIdNotFoundException(message = message) },
    ),
    FETCH_SESSION_ID_NOT_FOUND(
        code = 70, 
        defaultExceptionString = "The fetch session ID was not found.",
        builder = { message -> FetchSessionIdNotFoundException(message = message) },
    ),
    INVALID_FETCH_SESSION_EPOCH(
        code = 71, 
        defaultExceptionString = "The fetch session epoch is invalid.",
        builder = { message -> InvalidFetchSessionEpochException(message = message) },
    ),
    LISTENER_NOT_FOUND(
        code = 72,
        defaultExceptionString = "There is no listener on the leader broker that matches the listener on which " +
                "metadata request was processed.",
        builder = { message -> ListenerNotFoundException(message = message) },
    ),
    TOPIC_DELETION_DISABLED(
        code = 73, 
        defaultExceptionString = "Topic deletion is disabled.",
        builder = { message -> TopicDeletionDisabledException(message = message) },
    ),
    FENCED_LEADER_EPOCH(
        code = 74,
        defaultExceptionString = "The leader epoch in the request is older than the epoch on the broker.",
        builder = { message -> FencedLeaderEpochException(message = message) },
    ),
    UNKNOWN_LEADER_EPOCH(
        code = 75,
        defaultExceptionString = "The leader epoch in the request is newer than the epoch on the broker.",
        builder = { message -> UnknownLeaderEpochException(message = message) },
    ),
    UNSUPPORTED_COMPRESSION_TYPE(
        code = 76,
        defaultExceptionString = "The requesting client does not support the compression type of given partition.",
        builder = { message -> UnsupportedCompressionTypeException(message = message) },
    ),
    STALE_BROKER_EPOCH(
        code = 77, 
        defaultExceptionString = "Broker epoch has changed.",
        builder = { message -> StaleBrokerEpochException(message = message) },
    ),
    OFFSET_NOT_AVAILABLE(
        code = 78, 
        defaultExceptionString = "The leader high watermark has not caught up from a recent leader " +
            "election so the offsets cannot be guaranteed to be monotonically increasing.",
        builder = { message -> OffsetNotAvailableException(message = message) },
    ),
    MEMBER_ID_REQUIRED(
        code = 79,
        defaultExceptionString = "The group member needs to have a valid member id before actually " +
                "entering a consumer group.",
        builder = { message -> MemberIdRequiredException(message = message) },
    ),
    PREFERRED_LEADER_NOT_AVAILABLE(
        code = 80, 
        defaultExceptionString = "The preferred leader was not available.",
        builder = { message -> PreferredLeaderNotAvailableException(message = message) },
    ),
    GROUP_MAX_SIZE_REACHED(
        code = 81, 
        defaultExceptionString = "The consumer group has reached its max size.",
        builder = { message -> GroupMaxSizeReachedException(message = message) },
    ),
    FENCED_INSTANCE_ID(
        code = 82, 
        defaultExceptionString = "The broker rejected this static consumer since " +
            "another consumer with the same group.instance.id has registered with a different member.id.",
        builder = { message -> FencedInstanceIdException(message = message) },
    ),
    ELIGIBLE_LEADERS_NOT_AVAILABLE(
        code = 83, 
        defaultExceptionString = "Eligible topic partition leaders are not available.",
        builder = { message -> EligibleLeadersNotAvailableException(message = message) },
    ),
    ELECTION_NOT_NEEDED(
        code = 84, 
        defaultExceptionString = "Leader election not needed for topic partition.",
        builder = { message -> ElectionNotNeededException(message = message) },
    ),
    NO_REASSIGNMENT_IN_PROGRESS(
        code = 85, 
        defaultExceptionString = "No partition reassignment is in progress.",
        builder = { message -> NoReassignmentInProgressException(message = message) },
    ),
    GROUP_SUBSCRIBED_TO_TOPIC(
        code = 86,
        defaultExceptionString = "Deleting offsets of a topic is forbidden while the consumer group " +
                "is actively subscribed to it.",
        builder = { message -> GroupSubscribedToTopicException(message = message) },
    ),
    INVALID_RECORD(
        code = 87,
        defaultExceptionString = "This record has failed the validation on broker and hence will be rejected.",
        builder = { InvalidRecordException() }),
    UNSTABLE_OFFSET_COMMIT(
        code = 88, 
        defaultExceptionString = "There are unstable offsets that need to be cleared.",
        builder = { message -> UnstableOffsetCommitException(message = message) },
    ),
    THROTTLING_QUOTA_EXCEEDED(
        code = 89, 
        defaultExceptionString = "The throttling quota has been exceeded.",
        builder = { message -> ThrottlingQuotaExceededException(message = message) },
    ),
    PRODUCER_FENCED(
        code = 90, 
        defaultExceptionString = "There is a newer producer with the same transactionalId " +
            "which fences the current one.",
        { message -> ProducerFencedException(message = message) },
    ),
    RESOURCE_NOT_FOUND(
        code = 91, 
        defaultExceptionString = "A request illegally referred to a resource that does not exist.",
        builder = { message -> ResourceNotFoundException(message = message) },
    ),
    DUPLICATE_RESOURCE(
        code = 92, 
        defaultExceptionString = "A request illegally referred to the same resource twice.",
        builder = { message -> DuplicateResourceException(message = message) },
    ),
    UNACCEPTABLE_CREDENTIAL(
        code = 93, 
        defaultExceptionString = "Requested credential would not meet criteria for acceptability.",
        builder = { message -> UnacceptableCredentialException(message = message) },
    ),
    INCONSISTENT_VOTER_SET(
        code = 94, 
        defaultExceptionString = "Indicates that the either the sender or recipient of a " +
            "voter-only request is not one of the expected voters",
        builder = { message -> InconsistentVoterSetException(message = message) },
    ),
    INVALID_UPDATE_VERSION(
        code = 95, 
        defaultExceptionString = "The given update version was invalid.",
        builder = { message -> InvalidUpdateVersionException(message = message) },
    ),
    FEATURE_UPDATE_FAILED(
        code = 96,
        defaultExceptionString = "Unable to update finalized features due to an unexpected server error.",
        builder = { message -> FeatureUpdateFailedException(message = message) },
    ),
    PRINCIPAL_DESERIALIZATION_FAILURE(
        code = 97,
        defaultExceptionString = "Request principal deserialization failed during forwarding. " +
                "This indicates an internal error on the broker cluster security setup.",
        builder = { message -> PrincipalDeserializationException(message = message) },
    ),
    SNAPSHOT_NOT_FOUND(
        code = 98, 
        defaultExceptionString = "Requested snapshot was not found",
        builder = { message -> SnapshotNotFoundException(message = message) },
    ),
    POSITION_OUT_OF_RANGE(
        code = 99,
        defaultExceptionString = "Requested position is not greater than or equal to zero, and less " +
                "than the size of the snapshot.",
        builder = { message -> PositionOutOfRangeException(message = message) },
    ),
    UNKNOWN_TOPIC_ID(
        code = 100, 
        defaultExceptionString = "This server does not host this topic ID.",
        builder = { message -> UnknownTopicIdException(message = message) },
    ),
    DUPLICATE_BROKER_REGISTRATION(
        code = 101, 
        defaultExceptionString = "This broker ID is already in use.",
        builder = { message -> DuplicateBrokerRegistrationException(message = message) },
    ),
    BROKER_ID_NOT_REGISTERED(
        code = 102, 
        defaultExceptionString = "The given broker ID was not registered.",
        builder = { message -> BrokerIdNotRegisteredException(message = message) },
    ),
    INCONSISTENT_TOPIC_ID(
        code = 103, 
        defaultExceptionString = "The log's topic ID did not match the topic ID in the request",
        builder = { message -> InconsistentTopicIdException(message = message) },
    ),
    INCONSISTENT_CLUSTER_ID(
        code = 104,
        defaultExceptionString = "The clusterId in the request does not match that found on the server",
        builder = { message -> InconsistentClusterIdException(message = message) },
    ),
    TRANSACTIONAL_ID_NOT_FOUND(
        code = 105, 
        defaultExceptionString = "The transactionalId could not be found",
        builder = { message -> TransactionalIdNotFoundException(message = message) },
    ),
    FETCH_SESSION_TOPIC_ID_ERROR(
        code = 106, 
        defaultExceptionString = "The fetch session encountered inconsistent topic ID usage",
        builder = { message -> FetchSessionTopicIdException(message = message) },
    ),
    INELIGIBLE_REPLICA(
        code = 107, 
        defaultExceptionString = "The new ISR contains at least one ineligible replica.",
        builder = { message -> IneligibleReplicaException(message = message) },
    ),
    NEW_LEADER_ELECTED(
        code = 108,
        defaultExceptionString = "The AlterPartition request successfully updated the partition " +
                "state but the leader has changed.",
        builder = { message -> NewLeaderElectedException(message = message) },
    );

    /**
     * The error code for the exception.
     */
    val code: Short

    private var _exception: ApiException? = null

    /**
     * The instance of the exception.
     */
    val exception: ApiException?
        get() = _exception // Throw NullPointerException if exception from Errors.NONE is accessed

    init {
        this.code = code.toShort()
        _exception = builder(defaultExceptionString)
    }

    /**
     * An instance of the exception
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("exception"),
    )
    fun exception(): ApiException = exception

    /**
     * Create an instance of the ApiException that contains the given error message.
     *
     * @param message    The message string to set.
     * @return           The exception.
     * @throws NullPointerException if called for [Errors.NONE].
     */
    fun exception(message: String?): ApiException {
        return if (message == null) {
            // If no error message was specified, return an exception with the default error message.
            exception
        } else builder(message)!!
        // Return an exception with the given error message.
    }

    /**
     * Returns the class name of the exception or null if this is `Errors.NONE`.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("exceptionName"),
    )
    fun exceptionName(): String? {
        return _exception?.javaClass?.name
    }

    /**
     * The class name of the exception or null if this is `Errors.NONE`.
     */
    val exceptionName: String?
        get() = _exception?.javaClass?.name

    /**
     * The error code for the exception
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("code"),
    )
    fun code(): Short = code

    /**
     * Throw the exception corresponding to this error if there is one
     */
    fun maybeThrow() = _exception?.let { throw it }

    /**
     * Get a friendly description of the error (if one is available).
     * @return the error message
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("message"),
    )
    fun message(): String = _exception?.message ?: toString()

    val message: String
        get() = _exception?.message ?: toString()

    companion object {
        private val log = LoggerFactory.getLogger(Errors::class.java)
        private val classToError: MutableMap<Class<*>, Errors> = HashMap()
        private val codeToError: MutableMap<Short, Errors?> = HashMap()

        init {
            for (error: Errors in values()) {
                if (codeToError.put(error.code, error) != null)
                    throw ExceptionInInitializerError(
                        "Code ${error.code} for error $error has already been used"
                    )

                error._exception?.let { classToError[it.javaClass] = error }
            }
        }

        /**
         * Throw the exception if there is one
         */
        fun forCode(code: Short): Errors {
            val error = codeToError[code]
            return if (error != null) error
            else {
                log.warn("Unexpected error code: {}.", code)
                UNKNOWN_SERVER_ERROR
            }
        }

        /**
         * Return the error instance associated with this exception or any of its superclasses
         * (or UNKNOWN if there is none). If there are multiple matches in the class hierarchy,
         * the first match starting from the bottom is used.
         */
        fun forException(t: Throwable): Errors {
            val cause = maybeUnwrapException(t)
            var clazz: Class<*>? = cause.javaClass
            while (clazz != null) {
                val error = classToError[clazz]
                if (error != null) return error
                clazz = clazz.superclass
            }
            return UNKNOWN_SERVER_ERROR
        }

        /**
         * Check if a Throwable is a commonly wrapped exception type (e.g. `CompletionException`)
         * and return the cause if so. This is useful to handle cases where exceptions may be raised
         * from a future or a completion stage (as might be the case for requests sent to the
         * controller in `ControllerApis`).
         *
         * @param t The Throwable to check
         * @return The throwable itself or its cause if it is an instance of a commonly wrapped
         * exception type
         */
        fun maybeUnwrapException(t: Throwable): Throwable {
            return if (t is CompletionException || t is ExecutionException) t.cause ?: t
            else t
        }

        private fun toHtml(): String {
            val b = StringBuilder()
            b.append("<table class=\"data-table\"><tbody>\n")
            b.append("<tr>")
            b.append("<th>Error</th>\n")
            b.append("<th>Code</th>\n")
            b.append("<th>Retriable</th>\n")
            b.append("<th>Description</th>\n")
            b.append("</tr>\n")
            for (error: Errors in values()) {
                b.append("<tr>")
                b.append("<td>")
                b.append(error.name)
                b.append("</td>")
                b.append("<td>")
                b.append(error.code.toInt())
                b.append("</td>")
                b.append("<td>")
                b.append(if (error._exception is RetriableException) "True" else "False")
                b.append("</td>")
                b.append("<td>")
                b.append(error._exception?.message ?: "")
                b.append("</td>")
                b.append("</tr>\n")
            }
            b.append("</tbody></table>\n")
            return b.toString()
        }

        @JvmStatic
        fun main(args: Array<String>) {
            println(toHtml())
        }
    }
}
