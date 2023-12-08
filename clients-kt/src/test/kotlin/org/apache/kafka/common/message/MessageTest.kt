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

package org.apache.kafka.common.message

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponseCollection
import org.apache.kafka.common.message.SimpleExampleMessageData.MyStruct
import org.apache.kafka.common.message.SimpleExampleMessageData.TaggedStruct
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Message
import org.apache.kafka.common.protocol.MessageUtil.toByteBuffer
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.protocol.types.RawTaggedField
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.nio.ByteBuffer
import org.junit.jupiter.api.Disabled
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.test.fail

@Timeout(120)
class MessageTest {


    private val memberId = "memberId"

    private val instanceId = "instanceId"

    private val listOfVersionsNonBatchOffsetFetch = intArrayOf(0, 1, 2, 3, 4, 5, 6, 7)

    @Test
    @Throws(Exception::class)
    fun testAddOffsetsToTxnVersions() {
        testAllMessageRoundTrips(
            AddOffsetsToTxnRequestData()
                .setTransactionalId("foobar")
                .setProducerId(0xbadcafebadcafeL)
                .setProducerEpoch(123)
                .setGroupId("baaz")
        )
        testAllMessageRoundTrips(
            AddOffsetsToTxnResponseData()
                .setThrottleTimeMs(42)
                .setErrorCode(0)
        )
    }

    @Test
    @Throws(Exception::class)
    fun testAddPartitionsToTxnVersions() {
        testAllMessageRoundTrips(
            AddPartitionsToTxnRequestData()
                .setTransactionalId("blah")
                .setProducerId(0xbadcafebadcafeL)
                .setProducerEpoch(30000)
                .setTopics(
                    AddPartitionsToTxnTopicCollection(
                        listOf(
                            AddPartitionsToTxnTopic()
                                .setName("Topic")
                                .setPartitions(intArrayOf(1)),
                        ).iterator()
                    )
                )
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCreateTopicsVersions() {
        testAllMessageRoundTrips(
            CreateTopicsRequestData()
                .setTimeoutMs(1000)
                .setTopics(CreatableTopicCollection())
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeAclsRequest() {
        testAllMessageRoundTrips(
            DescribeAclsRequestData()
                .setResourceTypeFilter(42)
                .setResourceNameFilter(null)
                .setPatternTypeFilter(3)
                .setPrincipalFilter("abc")
                .setHostFilter(null)
                .setOperation(0)
                .setPermissionType(0)
        )
    }

    @Test
    @Throws(Exception::class)
    fun testMetadataVersions() {
        testAllMessageRoundTrips(
            MetadataRequestData().setTopics(
                listOf(
                    MetadataRequestTopic().setName("foo"),
                    MetadataRequestTopic().setName("bar"),
                )
            )
        )
        testAllMessageRoundTripsFromVersion(
            fromVersion = 1,
            message = MetadataRequestData()
                .setTopics(emptyList())
                .setAllowAutoTopicCreation(true)
                .setIncludeClusterAuthorizedOperations(false)
                .setIncludeTopicAuthorizedOperations(false),
        )
        testAllMessageRoundTripsFromVersion(
            fromVersion = 4,
            message = MetadataRequestData()
                .setTopics(emptyList())
                .setAllowAutoTopicCreation(false)
                .setIncludeClusterAuthorizedOperations(false)
                .setIncludeTopicAuthorizedOperations(false),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testHeartbeatVersions() {
        val newRequest: () -> HeartbeatRequestData = {
            HeartbeatRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15)
        }
        testAllMessageRoundTrips(newRequest())
        testAllMessageRoundTrips(newRequest().setGroupInstanceId(null))
        testAllMessageRoundTripsFromVersion(
            fromVersion = 3,
            message = newRequest().setGroupInstanceId("instanceId"),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testJoinGroupRequestVersions() {
        val newRequest: () -> JoinGroupRequestData = {
            JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setProtocolType("consumer")
                .setProtocols(JoinGroupRequestProtocolCollection())
                .setSessionTimeoutMs(10000)
        }
        testAllMessageRoundTrips(newRequest())
        testAllMessageRoundTripsFromVersion(
            fromVersion = 1,
            message = newRequest().setRebalanceTimeoutMs(20000),
        )
        testAllMessageRoundTrips(newRequest().setGroupInstanceId(null))
        testAllMessageRoundTripsFromVersion(
            fromVersion = 5,
            message = newRequest().setGroupInstanceId("instanceId"),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsRequestVersions() {
        val v = listOf(
            ListOffsetsTopic()
                .setName("topic")
                .setPartitions(
                    listOf(
                        ListOffsetsPartition()
                            .setPartitionIndex(0)
                            .setTimestamp(123L)
                    )
                ),
        )
        val newRequest: () -> ListOffsetsRequestData = {
            ListOffsetsRequestData()
                .setTopics(v)
                .setReplicaId(0)
        }
        testAllMessageRoundTrips(newRequest())
        testAllMessageRoundTripsFromVersion(
            fromVersion = 2,
            message = newRequest().setIsolationLevel(IsolationLevel.READ_COMMITTED.id()),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testListOffsetsResponseVersions() {
        val partition = ListOffsetsPartitionResponse()
            .setErrorCode(Errors.NONE.code)
            .setPartitionIndex(0)
            .setOldStyleOffsets(longArrayOf(321L))
        val topics = listOf(
            ListOffsetsTopicResponse()
                .setName("topic")
                .setPartitions(listOf(partition))
        )
        val response: () -> ListOffsetsResponseData = { ListOffsetsResponseData().setTopics(topics) }
        for (version in ApiKeys.LIST_OFFSETS.allVersions()) {
            val responseData = response()
            if (version > 0) {
                responseData.topics[0].partitions[0]
                    .setOldStyleOffsets(longArrayOf())
                    .setOffset(456L)
                    .setTimestamp(123L)
            }
            if (version > 1) responseData.setThrottleTimeMs(1000)
            if (version > 3) partition.setLeaderEpoch(1)

            testEquivalentMessageRoundTrip(version, responseData)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testJoinGroupResponseVersions() {
        val newResponse: () -> JoinGroupResponseData = {
            JoinGroupResponseData()
                .setMemberId(memberId)
                .setLeader(memberId)
                .setGenerationId(1)
                .setMembers(listOf(JoinGroupResponseMember().setMemberId(memberId)))
        }
        testAllMessageRoundTrips(newResponse())
        testAllMessageRoundTripsFromVersion(
            fromVersion = 2,
            message = newResponse().setThrottleTimeMs(1000),
        )
        testAllMessageRoundTrips(newResponse().members[0].setGroupInstanceId(null))
        testAllMessageRoundTripsFromVersion(
            fromVersion = 5,
            message = newResponse().members[0].setGroupInstanceId("instanceId")
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLeaveGroupResponseVersions() {
        val newResponse: () -> LeaveGroupResponseData = {
            LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code)
        }
        testAllMessageRoundTrips(newResponse())
        testAllMessageRoundTripsFromVersion(
            fromVersion = 1,
            message = newResponse().setThrottleTimeMs(1000),
        )
        testAllMessageRoundTripsFromVersion(
            fromVersion = 3,
            message = newResponse().setMembers(
                listOf(
                    MemberResponse()
                        .setMemberId(memberId)
                        .setGroupInstanceId(instanceId)
                )
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun testSyncGroupDefaultGroupInstanceId() {
        val request: () -> SyncGroupRequestData = {
            SyncGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15)
                .setAssignments(ArrayList())
        }
        testAllMessageRoundTrips(request())
        testAllMessageRoundTrips(request().setGroupInstanceId(null))
        testAllMessageRoundTripsFromVersion(
            fromVersion = 3,
            message = request().setGroupInstanceId(instanceId),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitDefaultGroupInstanceId() {
        testAllMessageRoundTrips(
            OffsetCommitRequestData()
                .setTopics(ArrayList())
                .setGroupId("groupId")
        )
        val request: () -> OffsetCommitRequestData = {
            OffsetCommitRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setTopics(ArrayList())
                .setGenerationId(15)
        }
        testAllMessageRoundTripsFromVersion(fromVersion = 1, message = request())
        testAllMessageRoundTripsFromVersion(fromVersion = 1, message = request().setGroupInstanceId(null))
        testAllMessageRoundTripsFromVersion(fromVersion = 7, message = request().setGroupInstanceId(instanceId))
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeGroupsRequestVersions() {
        testAllMessageRoundTrips(
            DescribeGroupsRequestData()
                .setGroups(listOf("group"))
                .setIncludeAuthorizedOperations(false)
        )
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeGroupsResponseVersions() {
        val baseMember = DescribedGroupMember().setMemberId(memberId)
        val baseGroup = DescribedGroup()
            .setGroupId("group")
            .setGroupState("Stable").setErrorCode(Errors.NONE.code)
            .setMembers(listOf(baseMember))
            .setProtocolType("consumer")
        val baseResponse = DescribeGroupsResponseData().setGroups(listOf(baseGroup))

        testAllMessageRoundTrips(baseResponse)
        testAllMessageRoundTripsFromVersion(fromVersion = 1, message = baseResponse.setThrottleTimeMs(10))

        baseGroup.setAuthorizedOperations(1)
        testAllMessageRoundTripsFromVersion(fromVersion = 3, message = baseResponse)

        baseMember.setGroupInstanceId(instanceId)
        testAllMessageRoundTripsFromVersion(fromVersion = 4, message = baseResponse)
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeClusterRequestVersions() {
        testAllMessageRoundTrips(DescribeClusterRequestData().setIncludeClusterAuthorizedOperations(true))
    }

    @Test
    @Throws(Exception::class)
    fun testDescribeClusterResponseVersions() {
        val data = DescribeClusterResponseData()
            .setBrokers(
                DescribeClusterBrokerCollection(
                    listOf(
                        DescribeClusterBroker()
                            .setBrokerId(1)
                            .setHost("localhost")
                            .setPort(9092)
                            .setRack("rack1")
                    ).iterator()
                )
            )
            .setClusterId("clusterId")
            .setControllerId(1)
            .setClusterAuthorizedOperations(10)
        testAllMessageRoundTrips(data)
    }

    @Test
    @Throws(Exception::class)
    fun testGroupInstanceIdIgnorableInDescribeGroupsResponse() {
        val responseWithGroupInstanceId = DescribeGroupsResponseData()
            .setGroups(
                listOf(
                    DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(
                            listOf(
                                DescribedGroupMember()
                                    .setMemberId(memberId)
                                    .setGroupInstanceId(instanceId)
                            )
                        )
                        .setProtocolType("consumer")
                )
            )
        val expectedResponse = responseWithGroupInstanceId.duplicate()
        // Unset GroupInstanceId
        expectedResponse.groups[0].members[0].setGroupInstanceId(null)
        testAllMessageRoundTripsBeforeVersion(
            beforeVersion = 4,
            message = responseWithGroupInstanceId,
            expected = expectedResponse,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testThrottleTimeIgnorableInDescribeGroupsResponse() {
        val responseWithGroupInstanceId = DescribeGroupsResponseData()
            .setGroups(
                listOf(
                    DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code)
                        .setMembers(listOf(DescribedGroupMember().setMemberId(memberId)))
                        .setProtocolType("consumer")
                )
            )
            .setThrottleTimeMs(10)
        val expectedResponse = responseWithGroupInstanceId.duplicate()

        // Unset throttle time
        expectedResponse.setThrottleTimeMs(0)
        testAllMessageRoundTripsBeforeVersion(
            beforeVersion = 1,
            message = responseWithGroupInstanceId,
            expected = expectedResponse,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetForLeaderEpochVersions() {
        // Version 2 adds optional current leader epoch
        val partitionDataNoCurrentEpoch = OffsetForLeaderPartition()
            .setPartition(0)
            .setLeaderEpoch(3)
        val partitionDataWithCurrentEpoch = OffsetForLeaderPartition()
            .setPartition(0)
            .setLeaderEpoch(3)
            .setCurrentLeaderEpoch(5)
        val data = OffsetForLeaderEpochRequestData()
        data.topics.add(
            OffsetForLeaderTopic()
                .setTopic("foo")
                .setPartitions(listOf(partitionDataNoCurrentEpoch))
        )
        testAllMessageRoundTrips(data)
        testAllMessageRoundTripsBeforeVersion(
            beforeVersion = 2,
            message = partitionDataWithCurrentEpoch,
            expected = partitionDataNoCurrentEpoch,
        )
        testAllMessageRoundTripsFromVersion(
            fromVersion = 2,
            message = partitionDataWithCurrentEpoch,
        )

        // Version 3 adds the optional replica Id field
        testAllMessageRoundTripsFromVersion(
            fromVersion = 3,
            message = OffsetForLeaderEpochRequestData().setReplicaId(5),
        )
        testAllMessageRoundTripsBeforeVersion(
            beforeVersion = 3,
            message = OffsetForLeaderEpochRequestData().setReplicaId(5),
            expected = OffsetForLeaderEpochRequestData(),
        )
        testAllMessageRoundTripsBeforeVersion(
            beforeVersion = 3,
            message = OffsetForLeaderEpochRequestData().setReplicaId(5),
            expected = OffsetForLeaderEpochRequestData().setReplicaId(-2),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testLeaderAndIsrVersions() {
        // Version 3 adds two new fields - AddingReplicas and RemovingReplicas
        val partitionStateNoAddingRemovingReplicas = LeaderAndIsrTopicState()
            .setTopicName("topic")
            .setPartitionStates(
                listOf(
                    LeaderAndIsrPartitionState()
                        .setPartitionIndex(0)
                        .setReplicas(intArrayOf(0))
                )
            )
        val partitionStateWithAddingRemovingReplicas = LeaderAndIsrTopicState()
            .setTopicName("topic")
            .setPartitionStates(
                listOf(
                    LeaderAndIsrPartitionState()
                        .setPartitionIndex(0)
                        .setReplicas(intArrayOf(0))
                        .setAddingReplicas(intArrayOf(1))
                        .setRemovingReplicas(intArrayOf(1))
                )
            )
        testAllMessageRoundTripsBetweenVersions(
            startVersion = 2,
            endVersion = 3,
            message = LeaderAndIsrRequestData().setTopicStates(listOf(partitionStateWithAddingRemovingReplicas)),
            expected = LeaderAndIsrRequestData().setTopicStates(listOf(partitionStateNoAddingRemovingReplicas)),
        )
        testAllMessageRoundTripsFromVersion(
            fromVersion = 3,
            message = LeaderAndIsrRequestData().setTopicStates(listOf(partitionStateWithAddingRemovingReplicas)),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitRequestVersions() {
        val groupId = "groupId"
        val topicName = "topic"
        val metadata = "metadata"
        val partition = 2
        val offset = 100
        testAllMessageRoundTrips(
            OffsetCommitRequestData()
                .setGroupId(groupId)
                .setTopics(
                    listOf(
                        OffsetCommitRequestTopic()
                            .setName(topicName)
                            .setPartitions(
                                listOf(
                                    OffsetCommitRequestPartition()
                                        .setPartitionIndex(partition)
                                        .setCommittedMetadata(metadata)
                                        .setCommittedOffset(offset.toLong())
                                )
                            )
                    )
                )
        )
        val request: () -> OffsetCommitRequestData = {
            OffsetCommitRequestData()
                .setGroupId(groupId)
                .setMemberId("memberId")
                .setGroupInstanceId("instanceId")
                .setTopics(
                    listOf(
                        OffsetCommitRequestTopic()
                            .setName(topicName)
                            .setPartitions(
                                listOf(
                                    OffsetCommitRequestPartition()
                                        .setPartitionIndex(partition)
                                        .setCommittedLeaderEpoch(10)
                                        .setCommittedMetadata(metadata)
                                        .setCommittedOffset(offset.toLong())
                                        .setCommitTimestamp(20)
                                )
                            )
                    )
                )
                .setRetentionTimeMs(20)
        }
        for (version in ApiKeys.OFFSET_COMMIT.allVersions()) {
            val requestData = request()
            if (version < 1) {
                requestData.setMemberId("")
                requestData.setGenerationId(-1)
            }
            if (version.toInt() != 1) requestData.topics[0].partitions[0].setCommitTimestamp(-1)
            if (version < 2 || version > 4) requestData.setRetentionTimeMs(-1)
            if (version < 6) requestData.topics[0].partitions[0].setCommittedLeaderEpoch(-1)
            if (version < 7) requestData.setGroupInstanceId(null)
            if (version.toInt() == 1) testEquivalentMessageRoundTrip(version, requestData)
            else if (version in 2..4) testAllMessageRoundTripsBetweenVersions(
                startVersion = version,
                endVersion = 5,
                message = requestData,
                expected = requestData,
            )
            else testAllMessageRoundTripsFromVersion(version, requestData)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetCommitResponseVersions() {
        val response: () -> OffsetCommitResponseData = {
            OffsetCommitResponseData()
                .setTopics(
                    listOf(
                        OffsetCommitResponseTopic()
                            .setName("topic")
                            .setPartitions(
                                listOf(
                                    OffsetCommitResponsePartition()
                                        .setPartitionIndex(1)
                                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
                                )
                            )
                    )
                )
                .setThrottleTimeMs(20)
        }
        for (version in ApiKeys.OFFSET_COMMIT.allVersions()) {
            val responseData = response()
            if (version < 3) responseData.setThrottleTimeMs(0)
            testAllMessageRoundTripsFromVersion(version, responseData)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTxnOffsetCommitRequestVersions() {
        val groupId = "groupId"
        val topicName = "topic"
        val metadata = "metadata"
        val txnId = "transactionalId"
        val producerId = 25
        val producerEpoch: Short = 10
        val instanceId = "instance"
        val memberId = "member"
        val generationId = 1
        val partition = 2
        val offset = 100
        testAllMessageRoundTrips(
            TxnOffsetCommitRequestData()
                .setGroupId(groupId)
                .setTransactionalId(txnId)
                .setProducerId(producerId.toLong())
                .setProducerEpoch(producerEpoch)
                .setTopics(
                    listOf(
                        TxnOffsetCommitRequestTopic()
                            .setName(topicName)
                            .setPartitions(
                                listOf(
                                    TxnOffsetCommitRequestPartition()
                                        .setPartitionIndex(partition)
                                        .setCommittedMetadata(metadata)
                                        .setCommittedOffset(offset.toLong())
                                )
                            )
                    )
                )
        )
        val request: () -> TxnOffsetCommitRequestData = {
            TxnOffsetCommitRequestData()
                .setGroupId(groupId)
                .setTransactionalId(txnId)
                .setProducerId(producerId.toLong())
                .setProducerEpoch(producerEpoch)
                .setGroupInstanceId(instanceId)
                .setMemberId(memberId)
                .setGenerationId(generationId)
                .setTopics(
                    listOf(
                        TxnOffsetCommitRequestTopic()
                            .setName(topicName)
                            .setPartitions(
                                listOf(
                                    TxnOffsetCommitRequestPartition()
                                        .setPartitionIndex(partition)
                                        .setCommittedLeaderEpoch(10)
                                        .setCommittedMetadata(metadata)
                                        .setCommittedOffset(offset.toLong())
                                )
                            )
                    )
                )
        }
        for (version in ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            val requestData = request()
            if (version < 2) requestData.topics[0].partitions[0].setCommittedLeaderEpoch(-1)
            if (version < 3) {
                val finalVersion = version
                assertFailsWith<UnsupportedVersionException> {
                    testEquivalentMessageRoundTrip(finalVersion, requestData)
                }

                requestData.setGroupInstanceId(null)
                assertFailsWith<UnsupportedVersionException> {
                    testEquivalentMessageRoundTrip(finalVersion, requestData)
                }

                requestData.setMemberId("")
                assertFailsWith<UnsupportedVersionException> {
                    testEquivalentMessageRoundTrip(finalVersion, requestData)
                }
                requestData.setGenerationId(-1)
            }
            testAllMessageRoundTripsFromVersion(version, requestData)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTxnOffsetCommitResponseVersions() {
        testAllMessageRoundTrips(
            TxnOffsetCommitResponseData()
                .setTopics(
                    listOf(
                        TxnOffsetCommitResponseTopic()
                            .setName("topic")
                            .setPartitions(
                                listOf(
                                    TxnOffsetCommitResponsePartition()
                                        .setPartitionIndex(1)
                                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code)
                                )
                            )
                    )
                )
                .setThrottleTimeMs(20)
        )
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetFetchV0ToV7() {
        val groupId = "groupId"
        val topicName = "topic"
        val topics = listOf(
            OffsetFetchRequestTopic()
                .setName(topicName)
                .setPartitionIndexes(intArrayOf(5))
        )
        testAllMessageRoundTripsOffsetFetchV0ToV7(
            OffsetFetchRequestData()
                .setTopics(ArrayList())
                .setGroupId(groupId)
        )
        testAllMessageRoundTripsOffsetFetchV0ToV7(
            OffsetFetchRequestData()
                .setGroupId(groupId)
                .setTopics(topics)
        )
        val allPartitionData = OffsetFetchRequestData()
            .setGroupId(groupId)
            .setTopics(null)
        val requireStableData = OffsetFetchRequestData()
            .setGroupId(groupId)
            .setTopics(topics)
            .setRequireStable(true)
        for (version in listOfVersionsNonBatchOffsetFetch) {
            val finalVersion = version
            if (version < 2) assertFailsWith<NullPointerException> {
                testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(finalVersion.toShort(), allPartitionData)
            }
            else testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(version.toShort(), allPartitionData)

            if (version < 7) assertFailsWith<UnsupportedVersionException> {
                testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(finalVersion.toShort(), requireStableData)
            }
            else testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(finalVersion.toShort(), requireStableData)
        }
        val response: () -> OffsetFetchResponseData = {
            OffsetFetchResponseData()
                .setTopics(
                    listOf(
                        OffsetFetchResponseTopic()
                            .setName(topicName)
                            .setPartitions(
                                listOf(
                                    OffsetFetchResponsePartition()
                                        .setPartitionIndex(5)
                                        .setMetadata(null)
                                        .setCommittedOffset(100)
                                        .setCommittedLeaderEpoch(3)
                                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                )
                            )
                    )
                )
                .setErrorCode(Errors.NOT_COORDINATOR.code)
                .setThrottleTimeMs(10)
        }
        for (version: Int in listOfVersionsNonBatchOffsetFetch) {
            val responseData = response()
            if (version <= 1) responseData.setErrorCode(Errors.NONE.code)
            if (version <= 2) responseData.setThrottleTimeMs(0)
            if (version <= 4) responseData.topics[0].partitions[0].setCommittedLeaderEpoch(-1)

            testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(version.toShort(), responseData)
        }
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsOffsetFetchV0ToV7(message: Message) {
        testDuplication(message)
        testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(message.lowestSupportedVersion(), message)
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsOffsetFetchFromVersionV0ToV7(
        fromVersion: Short,
        message: Message,
    ) {
        for (version in fromVersion..7) testEquivalentMessageRoundTrip(version.toShort(), message)
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetFetchV8AndAboveSingleGroup() {
        val groupId = "groupId"
        val topicName = "topic"
        val topic = listOf(
            OffsetFetchRequestTopics()
                .setName(topicName)
                .setPartitionIndexes(intArrayOf(5))
        )
        val allPartitionData = OffsetFetchRequestData()
            .setGroups(
                listOf(
                    OffsetFetchRequestGroup()
                        .setGroupId(groupId)
                        .setTopics(null)
                )
            )
        val specifiedPartitionData = OffsetFetchRequestData()
            .setGroups(
                listOf(
                    OffsetFetchRequestGroup()
                        .setGroupId(groupId)
                        .setTopics(topic)
                )
            )
            .setRequireStable(true)
        testAllMessageRoundTripsOffsetFetchV8AndAbove(allPartitionData)
        testAllMessageRoundTripsOffsetFetchV8AndAbove(specifiedPartitionData)
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(version, specifiedPartitionData)
                testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(version, allPartitionData)
            }
        }
        val response: () -> OffsetFetchResponseData = {
            OffsetFetchResponseData()
                .setGroups(
                    listOf(
                        OffsetFetchResponseGroup()
                            .setGroupId(groupId)
                            .setTopics(
                                listOf(
                                    OffsetFetchResponseTopics()
                                        .setPartitions(
                                            listOf(
                                                OffsetFetchResponsePartitions()
                                                    .setPartitionIndex(5)
                                                    .setMetadata(null)
                                                    .setCommittedOffset(100)
                                                    .setCommittedLeaderEpoch(3)
                                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                                            )
                                        )
                                )
                            )
                            .setErrorCode(Errors.NOT_COORDINATOR.code)
                    )
                )
                .setThrottleTimeMs(10)
        }
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                val responseData = response()
                testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(version, responseData)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testOffsetFetchV8AndAbove() {
        val groupOne = "group1"
        val groupTwo = "group2"
        val groupThree = "group3"
        val groupFour = "group4"
        val groupFive = "group5"
        val topic1 = "topic1"
        val topic2 = "topic2"
        val topic3 = "topic3"
        val topicOne = OffsetFetchRequestTopics()
            .setName(topic1)
            .setPartitionIndexes(intArrayOf(5))
        val topicTwo = OffsetFetchRequestTopics()
            .setName(topic2)
            .setPartitionIndexes(intArrayOf(10))
        val topicThree = OffsetFetchRequestTopics()
            .setName(topic3)
            .setPartitionIndexes(intArrayOf(15))
        val groupOneTopics = listOf(topicOne)
        val group1 = OffsetFetchRequestGroup()
            .setGroupId(groupOne)
            .setTopics(groupOneTopics)
        val groupTwoTopics = listOf(topicOne, topicTwo)
        val group2 = OffsetFetchRequestGroup()
            .setGroupId(groupTwo)
            .setTopics(groupTwoTopics)
        val groupThreeTopics = listOf(topicOne, topicTwo, topicThree)
        val group3 = OffsetFetchRequestGroup()
            .setGroupId(groupThree)
            .setTopics(groupThreeTopics)
        val group4 = OffsetFetchRequestGroup()
            .setGroupId(groupFour)
            .setTopics(null)
        val group5 = OffsetFetchRequestGroup()
            .setGroupId(groupFive)
            .setTopics(null)
        val requestData = OffsetFetchRequestData()
            .setGroups(listOf(group1, group2, group3, group4, group5))
            .setRequireStable(true)

        testAllMessageRoundTripsOffsetFetchV8AndAbove(requestData)
        testAllMessageRoundTripsOffsetFetchV8AndAbove(requestData.setRequireStable(false))

        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(version, requestData)
        }
        val responseTopic1 = OffsetFetchResponseTopics()
            .setName(topic1)
            .setPartitions(
                listOf(
                    OffsetFetchResponsePartitions()
                        .setPartitionIndex(5)
                        .setMetadata(null)
                        .setCommittedOffset(100)
                        .setCommittedLeaderEpoch(3)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                )
            )
        val responseTopic2 = OffsetFetchResponseTopics()
            .setName(topic2)
            .setPartitions(
                listOf(
                    OffsetFetchResponsePartitions()
                        .setPartitionIndex(10)
                        .setMetadata("foo")
                        .setCommittedOffset(200)
                        .setCommittedLeaderEpoch(2)
                        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                )
            )
        val responseTopic3 = OffsetFetchResponseTopics()
            .setName(topic3)
            .setPartitions(
                listOf(
                    OffsetFetchResponsePartitions()
                        .setPartitionIndex(15)
                        .setMetadata("bar")
                        .setCommittedOffset(300)
                        .setCommittedLeaderEpoch(1)
                        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
                )
            )
        val responseGroup1 = OffsetFetchResponseGroup()
            .setGroupId(groupOne)
            .setTopics(listOf(responseTopic1))
            .setErrorCode(Errors.NOT_COORDINATOR.code)
        val responseGroup2 = OffsetFetchResponseGroup()
            .setGroupId(groupTwo)
            .setTopics(listOf(responseTopic1, responseTopic2))
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
        val responseGroup3 = OffsetFetchResponseGroup()
            .setGroupId(groupThree)
            .setTopics(listOf(responseTopic1, responseTopic2, responseTopic3))
            .setErrorCode(Errors.NONE.code)
        val responseGroup4 = OffsetFetchResponseGroup()
            .setGroupId(groupFour)
            .setTopics(listOf(responseTopic1, responseTopic2, responseTopic3))
            .setErrorCode(Errors.NONE.code)
        val responseGroup5 = OffsetFetchResponseGroup()
            .setGroupId(groupFive)
            .setTopics(listOf(responseTopic1, responseTopic2, responseTopic3))
            .setErrorCode(Errors.NONE.code)
        val response: () -> OffsetFetchResponseData = {
            OffsetFetchResponseData()
                .setGroups(
                    listOf(
                        responseGroup1, responseGroup2, responseGroup3,
                        responseGroup4, responseGroup5
                    )
                )
                .setThrottleTimeMs(10)
        }
        for (version in ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                val responseData = response()
                testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(fromVersion = version, message = responseData)
            }
        }
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsOffsetFetchV8AndAbove(message: Message) {
        testDuplication(message)
        testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(fromVersion = 8, message = message)
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsOffsetFetchFromVersionV8AndAbove(fromVersion: Short, message: Message) {
        for (version in fromVersion..message.highestSupportedVersion()) {
            testEquivalentMessageRoundTrip(version.toShort(), message)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testProduceResponseVersions() {
        val topicName = "topic"
        val partitionIndex = 0
        val errorCode = Errors.INVALID_TOPIC_EXCEPTION.code
        val baseOffset = 12L
        val throttleTimeMs = 1234
        val logAppendTimeMs = 1234L
        val logStartOffset = 1234L
        val batchIndex = 0
        val batchIndexErrorMessage = "error message"
        val errorMessage = "global error message"
        testAllMessageRoundTrips(
            ProduceResponseData()
                .setResponses(
                    TopicProduceResponseCollection(
                        listOf(
                            TopicProduceResponse()
                                .setName(topicName)
                                .setPartitionResponses(
                                    listOf(
                                        PartitionProduceResponse()
                                            .setIndex(partitionIndex)
                                            .setErrorCode(errorCode)
                                            .setBaseOffset(baseOffset)
                                    )
                                )
                        ).iterator()
                    )
                )
        )
        val response: () -> ProduceResponseData = {
            ProduceResponseData()
                .setResponses(
                    TopicProduceResponseCollection(
                        listOf(
                            TopicProduceResponse()
                                .setName(topicName)
                                .setPartitionResponses(
                                    listOf(
                                        PartitionProduceResponse()
                                            .setIndex(partitionIndex)
                                            .setErrorCode(errorCode)
                                            .setBaseOffset(baseOffset)
                                            .setLogAppendTimeMs(logAppendTimeMs)
                                            .setLogStartOffset(logStartOffset)
                                            .setRecordErrors(
                                                listOf(
                                                    BatchIndexAndErrorMessage()
                                                        .setBatchIndex(batchIndex)
                                                        .setBatchIndexErrorMessage(batchIndexErrorMessage)
                                                )
                                            )
                                            .setErrorMessage(errorMessage)
                                    )
                                )
                        ).iterator()
                    )
                )
                .setThrottleTimeMs(throttleTimeMs)
        }
        for (version in ApiKeys.PRODUCE.allVersions()) {
            val responseData = response()
            if (version < 8) {
                responseData.responses.first().partitionResponses[0].setRecordErrors(emptyList())
                responseData.responses.first().partitionResponses[0].setErrorMessage(null)
            }
            if (version < 5) responseData.responses.first().partitionResponses[0].setLogStartOffset(-1)
            if (version < 2) responseData.responses.first().partitionResponses[0].setLogAppendTimeMs(-1)

            if (version < 1) responseData.setThrottleTimeMs(0)
            when (version) {
                in 3..4 -> testAllMessageRoundTripsBetweenVersions(
                    startVersion = version,
                    endVersion = 5,
                    message = responseData,
                    expected = responseData,
                )

                in 6..7 -> testAllMessageRoundTripsBetweenVersions(
                    startVersion = version,
                    endVersion = 8,
                    message = responseData,
                    expected = responseData,
                )

                else -> testEquivalentMessageRoundTrip(version = version, message = responseData)
            }
        }
    }

    @Test
    fun defaultValueShouldBeWritable() {
        with(SimpleExampleMessageData) {
            for (version in LOWEST_SUPPORTED_VERSION..HIGHEST_SUPPORTED_VERSION) {
                toByteBuffer(SimpleExampleMessageData(), version.toShort())
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSimpleMessage() {
        val message = SimpleExampleMessageData()
        message.setMyStruct(
            MyStruct()
                .setStructId(25)
                .setArrayInStruct(listOf(SimpleExampleMessageData.StructArray().setArrayFieldId(20)))
        )
        message.setMyTaggedStruct(TaggedStruct().setStructId("abc"))
        message.setProcessId(Uuid.randomUuid())
        message.setMyNullableString("notNull")
        message.setMyInt16(3)
        message.setMyString("test string")

        val duplicate = message.duplicate()
        assertEquals(duplicate, message)
        assertEquals(message, duplicate)

        duplicate.setMyTaggedIntArray(intArrayOf(123))
        assertNotEquals(duplicate, message)
        assertNotEquals(message, duplicate)
        testAllMessageRoundTripsFromVersion(2, message)
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTrips(message: Message) {
        testDuplication(message)
        testAllMessageRoundTripsFromVersion(message.lowestSupportedVersion(), message)
    }

    private fun testDuplication(message: Message) {
        val duplicate = message.duplicate()
        assertEquals(duplicate, message)
        assertEquals(message, duplicate)
        assertEquals(duplicate.hashCode(), message.hashCode())
        assertEquals(message.hashCode(), duplicate.hashCode())
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsBeforeVersion(beforeVersion: Short, message: Message, expected: Message) {
        testAllMessageRoundTripsBetweenVersions(
            startVersion = 0,
            endVersion = beforeVersion,
            message = message,
            expected = expected,
        )
    }

    /**
     * @param startVersion - the version we want to start at, inclusive
     * @param endVersion - the version we want to end at, exclusive
     */
    @Throws(Exception::class)
    private fun testAllMessageRoundTripsBetweenVersions(
        startVersion: Short,
        endVersion: Short,
        message: Message,
        expected: Message,
    ) {
        for (version in startVersion until endVersion) testMessageRoundTrip(version.toShort(), message, expected)
    }

    @Throws(Exception::class)
    private fun testAllMessageRoundTripsFromVersion(fromVersion: Short, message: Message) {
        for (version in fromVersion..message.highestSupportedVersion()) {
            testEquivalentMessageRoundTrip(version.toShort(), message)
        }
    }

    @Throws(Exception::class)
    private fun testMessageRoundTrip(version: Short, message: Message, expected: Message) {
        testByteBufferRoundTrip(version, message, expected)
    }

    @Throws(Exception::class)
    private fun testEquivalentMessageRoundTrip(version: Short, message: Message) {
        testByteBufferRoundTrip(version, message, message)
        testJsonRoundTrip(version, message, message)
    }

    @Throws(Exception::class)
    private fun testByteBufferRoundTrip(version: Short, message: Message, expected: Message) {
        val cache = ObjectSerializationCache()
        val size = message.size(cache, version)
        val buf = ByteBuffer.allocate(size)
        val byteBufferAccessor = ByteBufferAccessor(buf)
        message.write(byteBufferAccessor, cache, version)
        assertEquals(
            expected = size,
            actual = buf.position(),
            message = "The result of the size function does not match the number of bytes written for version $version",
        )
        val message2 = message.javaClass.getConstructor().newInstance()
        buf.flip()
        message2.read(byteBufferAccessor, version)
        assertEquals(
            expected = size,
            actual = buf.position(),
            message = "The result of the size function does not match the number of bytes read back in for version $version",
        )
        assertEquals(
            expected = expected,
            actual = message2,
            message = "The message object created after a round trip did not match for version $version",
        )
        assertEquals(expected.hashCode(), message2.hashCode())
        assertEquals(expected.toString(), message2.toString())
    }

    @Throws(Exception::class)
    private fun testJsonRoundTrip(version: Short, message: Message, expected: Message) {
        val jsonConverter = jsonConverterTypeName(message.javaClass.getTypeName())
        val converter = Class.forName(jsonConverter)
        val writeMethod = converter.getMethod("write", message.javaClass, Short::class.javaPrimitiveType)
        val jsonNode = writeMethod.invoke(null, message, version) as JsonNode
        val readMethod = converter.getMethod("read", JsonNode::class.java, Short::class.javaPrimitiveType)
        val message2 = readMethod.invoke(null, jsonNode, version) as Message
        assertEquals(expected, message2)
        assertEquals(expected.hashCode(), message2.hashCode())
        assertEquals(expected.toString(), message2.toString())
    }

    /**
     * Verify that the JSON files support the same message versions as the
     * schemas accessible through the ApiKey class.
     */
    @Test
    fun testMessageVersions() {
        for (apiKey: ApiKeys in ApiKeys.values()) {
            var message: Message
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newRequest()
            } catch (_: UnsupportedVersionException) {
                fail("No request message spec found for API $apiKey")
            }
            assertTrue(
                apiKey.latestVersion() <= message.highestSupportedVersion(),
                "Request message spec for " + apiKey + " only " + "supports versions up to " +
                        message.highestSupportedVersion()
            )
            try {
                message = ApiMessageType.fromApiKey(apiKey.id).newResponse()
            } catch (e: UnsupportedVersionException) {
                fail("No response message spec found for API $apiKey")
            }
            assertTrue(
                apiKey.latestVersion() <= message.highestSupportedVersion(),
                "Response message spec for $apiKey only supports versions up to ${message.highestSupportedVersion()}",
            )
        }
    }

    @Test
    fun testDefaultValues() {
        verifyWriteRaisesUve(
            version = 0,
            problemText = "validateOnly",
            message = CreateTopicsRequestData().setValidateOnly(true),
        )
        verifyWriteSucceeds(
            version = 0,
            message = CreateTopicsRequestData().setValidateOnly(false),
        )
        verifyWriteSucceeds(
            version = 0,
            message = OffsetCommitRequestData().setRetentionTimeMs(123),
        )
        verifyWriteRaisesUve(
            version = 5, problemText = "forgotten",
            message = FetchRequestData()
                .setForgottenTopicsData(listOf(ForgottenTopic().setTopic("foo"))),
        )
    }

    @Test
    fun testNonIgnorableFieldWithDefaultNull() {
        // Test non-ignorable string field `groupInstanceId` with default null
        verifyWriteRaisesUve(
            version = 0,
            problemText = "groupInstanceId",
            message = HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(instanceId),
        )
        verifyWriteSucceeds(
            version = 0,
            message = HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(null),
        )
        verifyWriteSucceeds(
            version = 0,
            message = HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId),
        )
    }

    @Test
    @Disabled("Kotlin Migration - Cannot set topics to null, since field is not nullable")
    fun testWriteNullForNonNullableFieldRaisesException() {
//        val createTopics = CreateTopicsRequestData().setTopics(null)
//        for (version in ApiKeys.CREATE_TOPICS.allVersions()) {
//            verifyWriteRaisesNpe(version, createTopics)
//        }
//        val metadata = MetadataRequestData().setTopics(null)
//        verifyWriteRaisesNpe(0, metadata)
    }

    @Test
    fun testUnknownTaggedFields() {
        val createTopics = CreateTopicsRequestData()
        verifyWriteSucceeds(6, createTopics)
        val field1000 = RawTaggedField(1000, byteArrayOf(0x1, 0x2, 0x3))
        createTopics.unknownTaggedFields().add(field1000)
        verifyWriteRaisesUve(0, "Tagged fields were set", createTopics)
        verifyWriteSucceeds(6, createTopics)
    }

    @Test
    @Throws(Exception::class)
    fun testLongTaggedString() {
        val chars = CharArray(1024)
        chars.fill('a')
        val longString = String(chars)
        val message = SimpleExampleMessageData().setMyString(longString)
        val cache = ObjectSerializationCache()
        val version: Short = 1
        val size = message.size(cache, version)
        val buf = ByteBuffer.allocate(size)
        val byteBufferAccessor = ByteBufferAccessor(buf)
        message.write(byteBufferAccessor, cache, version)
        assertEquals(size, buf.position())
    }

    private fun verifyWriteRaisesNpe(version: Short, message: Message) {
        val cache = ObjectSerializationCache()
        assertFailsWith<NullPointerException> {
            val size = message.size(cache, version)
            val buf = ByteBuffer.allocate(size)
            val byteBufferAccessor = ByteBufferAccessor(buf)
            message.write(byteBufferAccessor, cache, version)
        }
    }

    private fun verifyWriteRaisesUve(
        version: Short,
        problemText: String,
        message: Message,
    ) {
        val cache = ObjectSerializationCache()
        val e = assertFailsWith<UnsupportedVersionException> {
            val size = message.size(cache, version)
            val buf = ByteBuffer.allocate(size)
            val byteBufferAccessor = ByteBufferAccessor(buf)
            message.write(byteBufferAccessor, cache, version)
        }
        assertTrue(
            actual = e.message!!.contains(problemText),
            message = "Expected to get an error message about $problemText, but got: ${e.message}",
        )
    }

    private fun verifyWriteSucceeds(version: Short, message: Message) {
        val cache = ObjectSerializationCache()
        val size = message.size(cache, version)
        val buf = ByteBuffer.allocate(size * 2)
        val byteBufferAccessor = ByteBufferAccessor(buf)
        message.write(byteBufferAccessor, cache, version)
        assertEquals(
            expected = size,
            actual = buf.position(),
            message = "Expected the serialized size to be $size, but it was ${buf.position()}"
        )
    }

    @Test
    fun testCompareWithUnknownTaggedFields() {
        val createTopics = CreateTopicsRequestData()
        createTopics.setTimeoutMs(123)
        val createTopics2 = CreateTopicsRequestData()
        createTopics2.setTimeoutMs(123)
        assertEquals(createTopics, createTopics2)
        assertEquals(createTopics2, createTopics)

        // Call the accessor, which will create a new empty list.
        createTopics.unknownTaggedFields()

        // Verify that the equalities still hold after the new empty list has been created.
        assertEquals(createTopics, createTopics2)
        assertEquals(createTopics2, createTopics)

        createTopics.unknownTaggedFields().add(RawTaggedField(0, byteArrayOf(0)))
        assertNotEquals(createTopics, createTopics2)
        assertNotEquals(createTopics2, createTopics)
        createTopics2.unknownTaggedFields().add(RawTaggedField(0, byteArrayOf(0)))
        assertEquals(createTopics, createTopics2)
        assertEquals(createTopics2, createTopics)
    }

    companion object {

        private fun jsonConverterTypeName(source: String): String {
            val outerClassIndex = source.lastIndexOf('$')
            return if (outerClassIndex == -1) "${source}JsonConverter"
            else source.substring(0, outerClassIndex) + "JsonConverter$" +
                    source.substring(outerClassIndex + 1) + "JsonConverter"
        }
    }
}
