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

package org.apache.kafka.clients.consumer.internals

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.test.TestUtils.assertNullable
import org.junit.jupiter.api.Test
import java.util.regex.Pattern
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SubscriptionStateTest {
    
    private var state = SubscriptionState(
        logContext = LogContext(),
        defaultResetStrategy = OffsetResetStrategy.EARLIEST,
    )
    
    private val topic = "test"
    
    private val topic1 = "test1"
    
    private val tp0 = TopicPartition(topic, 0)
    
    private val tp1 = TopicPartition(topic, 1)
    
    private val t1p0 = TopicPartition(topic1, 0)
    
    private val rebalanceListener = MockRebalanceListener()
    
    private val leaderAndEpoch = LeaderAndEpoch.noLeaderOrEpoch()
    
    @Test
    fun partitionAssignment() {
        state.assignFromUser(setOf(tp0))
        assertEquals(setOf(tp0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        assertFalse(state.hasAllFetchPositions())
        state.seek(tp0, 1)
        assertTrue(state.isFetchable(tp0))
        assertEquals(1L, state.position(tp0)!!.offset)
        state.assignFromUser(emptySet())
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        assertFalse(state.isAssigned(tp0))
        assertFalse(state.isFetchable(tp0))
    }

    @Test
    fun partitionAssignmentChangeOnTopicSubscription() {
        state.assignFromUser(setOf(tp0, tp1))

        // assigned partitions should immediately change
        assertEquals(2, state.assignedPartitions().size)
        assertEquals(2, state.numAssignedPartitions())
        assertTrue(state.assignedPartitions().contains(tp0))
        assertTrue(state.assignedPartitions().contains(tp1))
        state.unsubscribe()

        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        state.subscribe(setOf(topic1), rebalanceListener)

        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(t1p0)))
        state.assignFromSubscribed(setOf(t1p0))

        // assigned partitions should immediately change
        assertEquals(setOf(t1p0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        state.subscribe(setOf(topic), rebalanceListener)

        // assigned partitions should remain unchanged
        assertEquals(setOf(t1p0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        state.unsubscribe()

        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
    }

    @Test
    fun testGroupSubscribe() {
        state.subscribe(setOf(topic1), rebalanceListener)
        assertEquals(setOf(topic1), state.metadataTopics())
        assertFalse(state.groupSubscribe(setOf(topic1)))
        assertEquals(setOf(topic1), state.metadataTopics())
        assertTrue(state.groupSubscribe(setOf(topic, topic1)))
        assertEquals(setOf(topic, topic1), state.metadataTopics())

        // `groupSubscribe` does not accumulate
        assertFalse(state.groupSubscribe(setOf(topic1)))
        assertEquals(setOf(topic1), state.metadataTopics())
        state.subscribe(setOf("anotherTopic"), rebalanceListener)
        assertEquals(setOf(topic1, "anotherTopic"), state.metadataTopics())
        assertFalse(state.groupSubscribe(setOf("anotherTopic")))
        assertEquals(setOf("anotherTopic"), state.metadataTopics())
    }

    @Test
    fun partitionAssignmentChangeOnPatternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener)

        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        state.subscribeFromPattern(setOf(topic))

        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp1)))
        state.assignFromSubscribed(setOf(tp1))

        // assigned partitions should immediately change
        assertEquals(setOf(tp1), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        assertEquals(setOf(topic), state.subscription())
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(t1p0)))
        state.assignFromSubscribed(setOf(t1p0))

        // assigned partitions should immediately change
        assertEquals(setOf(t1p0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        assertEquals(setOf(topic), state.subscription())
        state.subscribe(Pattern.compile(".*t"), rebalanceListener)

        // assigned partitions should remain unchanged
        assertEquals(setOf(t1p0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        state.subscribeFromPattern(setOf(topic))

        // assigned partitions should remain unchanged
        assertEquals(setOf(t1p0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp0)))
        state.assignFromSubscribed(setOf(tp0))

        // assigned partitions should immediately change
        assertEquals(setOf(tp0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        assertEquals(setOf(topic), state.subscription())
        state.unsubscribe()

        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
    }

    @Test
    fun verifyAssignmentId() {
        assertEquals(0, state.assignmentId())
        
        val userAssignment = setOf(tp0, tp1)
        state.assignFromUser(userAssignment)
        assertEquals(1, state.assignmentId())
        assertEquals(userAssignment, state.assignedPartitions())
        state.unsubscribe()
        assertEquals(2, state.assignmentId())
        assertEquals(emptySet<Any>(), state.assignedPartitions())
        
        val autoAssignment = setOf(t1p0)
        state.subscribe(setOf(topic1), rebalanceListener)
        assertTrue(state.checkAssignmentMatchedSubscription(autoAssignment))
        state.assignFromSubscribed(autoAssignment)
        assertEquals(3, state.assignmentId())
        assertEquals(autoAssignment, state.assignedPartitions())
    }

    @Test
    fun partitionReset() {
        state.assignFromUser(setOf(tp0))
        state.seek(tp0, 5)
        assertEquals(5L, state.position(tp0)!!.offset)
        
        state.requestOffsetReset(tp0)
        assertFalse(state.isFetchable(tp0))
        assertTrue(state.isOffsetResetNeeded(tp0))
        assertNull(state.position(tp0))

        // seek should clear the reset and make the partition fetchable
        state.seek(tp0, 0)
        assertTrue(state.isFetchable(tp0))
        assertFalse(state.isOffsetResetNeeded(tp0))
    }

    @Test
    fun topicSubscription() {
        state.subscribe(setOf(topic), rebalanceListener)
        assertEquals(1, state.subscription().size)
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        assertTrue(state.hasAutoAssignedPartitions())
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp0)))
        
        state.assignFromSubscribed(setOf(tp0))
        state.seek(tp0, 1)
        assertEquals(1L, state.position(tp0)!!.offset)
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp1)))
        
        state.assignFromSubscribed(setOf(tp1))
        assertTrue(state.isAssigned(tp1))
        assertFalse(state.isAssigned(tp0))
        assertFalse(state.isFetchable(tp1))
        assertEquals(setOf(tp1), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
    }

    @Test
    fun partitionPause() {
        state.assignFromUser(setOf(tp0))
        state.seek(tp0, 100)
        assertTrue(state.isFetchable(tp0))
        
        state.pause(tp0)
        assertFalse(state.isFetchable(tp0))
        
        state.resume(tp0)
        assertTrue(state.isFetchable(tp0))
    }

    @Test
    fun testMarkingPartitionPending() {
        state.assignFromUser(setOf(tp0))
        state.seek(tp0, 100)
        assertTrue(state.isFetchable(tp0))
        state.markPendingRevocation(setOf(tp0))
        assertFalse(state.isFetchable(tp0))
        assertFalse(state.isPaused(tp0))
    }

    @Test
    fun invalidPositionUpdate() {
        state.subscribe(setOf(topic), rebalanceListener)
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp0)))
        state.assignFromSubscribed(setOf(tp0))
        assertFailsWith<IllegalStateException> {
            state.position(tp0, FetchPosition(offset = 0, offsetEpoch = null, currentLeader = leaderAndEpoch))
        }
    }

    @Test
    fun cantAssignPartitionForUnsubscribedTopics() {
        state.subscribe(setOf(topic), rebalanceListener)
        assertFalse(state.checkAssignmentMatchedSubscription(listOf(t1p0)))
    }

    @Test
    fun cantAssignPartitionForUnmatchedPattern() {
        state.subscribe(Pattern.compile(".*t"), rebalanceListener)
        state.subscribeFromPattern(setOf(topic))
        assertFalse(state.checkAssignmentMatchedSubscription(listOf(t1p0)))
    }

    @Test
    fun cantChangePositionForNonAssignedPartition() {
        assertFailsWith<IllegalStateException> {
            state.position(tp0, FetchPosition(offset = 1, offsetEpoch = null, currentLeader = leaderAndEpoch))
        }
    }

    @Test
    fun cantSubscribeTopicAndPattern() {
        state.subscribe(setOf(topic), rebalanceListener)
        assertFailsWith<IllegalStateException> {
            state.subscribe(Pattern.compile(".*"), rebalanceListener)
        }
    }

    @Test
    fun cantSubscribePartitionAndPattern() {
        state.assignFromUser(setOf(tp0))
        assertFailsWith<IllegalStateException> {
            state.subscribe(Pattern.compile(".*"), rebalanceListener)
        }
    }

    @Test
    fun cantSubscribePatternAndTopic() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener)
        assertFailsWith<IllegalStateException> { state.subscribe(setOf(topic), rebalanceListener) }
    }

    @Test
    fun cantSubscribePatternAndPartition() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener)
        assertFailsWith<IllegalStateException> { state.assignFromUser(setOf(tp0)) }
    }

    @Test
    fun patternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener)
        state.subscribeFromPattern(setOf(topic, topic1))
        assertEquals(
            expected = 2,
            actual = state.subscription().size,
            message = "Expected subscribed topics count is incorrect",
        )
    }

    @Test
    fun unsubscribeUserAssignment() {
        state.assignFromUser(setOf(tp0, tp1))
        state.unsubscribe()
        state.subscribe(setOf(topic), rebalanceListener)
        assertEquals(setOf(topic), state.subscription())
    }

    @Test
    fun unsubscribeUserSubscribe() {
        state.subscribe(setOf(topic), rebalanceListener)
        state.unsubscribe()
        state.assignFromUser(setOf(tp0))
        assertEquals(setOf(tp0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
    }

    @Test
    fun unsubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener)
        state.subscribeFromPattern(setOf(topic, topic1))
        assertTrue(state.checkAssignmentMatchedSubscription(setOf(tp1)))
        
        state.assignFromSubscribed(setOf(tp1))
        assertEquals(setOf(tp1), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        
        state.unsubscribe()
        assertEquals(0, state.subscription().size)
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
        
        state.assignFromUser(setOf(tp0))
        assertEquals(setOf(tp0), state.assignedPartitions())
        assertEquals(1, state.numAssignedPartitions())
        
        state.unsubscribe()
        assertEquals(0, state.subscription().size)
        assertTrue(state.assignedPartitions().isEmpty())
        assertEquals(0, state.numAssignedPartitions())
    }

    @Test
    fun testPreferredReadReplicaLease() {
        state.assignFromUser(setOf(tp0))

        // Default state
        assertNull(state.preferredReadReplica(tp0, 0L))

        // Set the preferred replica with lease
        state.updatePreferredReadReplica(tp0, 42) { 10L }
        assertNullable(state.preferredReadReplica(tp0, 9L)) { value -> assertEquals(value, 42) }
        assertNullable(state.preferredReadReplica(tp0, 10L)) { value -> assertEquals(value, 42) }
        assertNull(state.preferredReadReplica(tp0, 11L))

        // Unset the preferred replica
        state.clearPreferredReadReplica(tp0)
        assertNull(state.preferredReadReplica(tp0, 9L))
        assertNull(state.preferredReadReplica(tp0, 11L))

        // Set to new preferred replica with lease
        state.updatePreferredReadReplica(tp0, 43) { 20L }
        assertNullable(state.preferredReadReplica(tp0, 11L)) { value -> assertEquals(value, 43) }
        assertNullable(state.preferredReadReplica(tp0, 20L)) { value -> assertEquals(value, 43) }
        assertNull(state.preferredReadReplica(tp0, 21L))

        // Set to new preferred replica without clearing first
        state.updatePreferredReadReplica(tp0, 44) { 30L }
        assertNullable(state.preferredReadReplica(tp0, 30L)) { value -> assertEquals(value, 44) }
        assertNull(state.preferredReadReplica(tp0, 31L))
    }

    @Test
    fun testSeekUnvalidatedWithNoOffsetEpoch() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0L,
                offsetEpoch = null,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 5),
            ),
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        val apiVersions = ApiVersions()
        apiVersions.update(broker1.idString(), NodeApiVersions.create())
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = null),
            )
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 10),
            )
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
    }

    @Test
    fun testSeekUnvalidatedWithNoEpochClearsAwaitingValidation() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0L,
                offsetEpoch = 2,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 5),
            ),
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0L,
                offsetEpoch = null,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 5),
            ),
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
    }

    @Test
    fun testSeekUnvalidatedWithOffsetEpoch() {
        val broker1 = Node(1, "localhost", 9092)
        val apiVersions = ApiVersions()
        apiVersions.update(broker1.idString(), NodeApiVersions.create())
        state.assignFromUser(setOf(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 0L,
                offsetEpoch = 2,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 5),
            ),
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))

        // Update using the current leader and epoch
        assertTrue(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 5),
            )
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))

        // Update with a newer leader and epoch
        assertTrue(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 15),
            )
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))

        // If the updated leader has no epoch information, then skip validation and begin fetching
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = null),
            )
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
    }

    @Test
    fun testSeekValidatedShouldClearAwaitingValidation() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 5,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            )
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))
        assertEquals(10L, state.position(tp0)!!.offset)
        state.seekValidated(
            tp = tp0,
            position = FetchPosition(
                offset = 8L,
                offsetEpoch = 4,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertEquals(8L, state.position(tp0)!!.offset)
    }

    @Test
    fun testCompleteValidationShouldClearAwaitingValidation() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 5,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))
        assertEquals(10L, state.position(tp0)!!.offset)
        state.completeValidation(tp0)
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertEquals(10L, state.position(tp0)!!.offset)
    }

    @Test
    fun testOffsetResetWhileAwaitingValidation() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 5,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )
        assertTrue(state.awaitingValidation(tp0))
        state.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        assertFalse(state.awaitingValidation(tp0))
        assertTrue(state.isOffsetResetNeeded(tp0))
    }

    @Test
    fun testMaybeCompleteValidation() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(initialOffsetEpoch)
                .setEndOffset(initialOffset + 5),
        )
        assertEquals(null, truncationOpt)
        assertFalse(state.awaitingValidation(tp0))
        assertEquals(initialPosition, state.position(tp0))
    }

    @Test
    fun testMaybeValidatePositionForCurrentLeader() {
        val oldApis = NodeApiVersions.create(
            apiKey = ApiKeys.OFFSET_FOR_LEADER_EPOCH.id,
            minVersion = 0,
            maxVersion = 2,
        )
        val apiVersions = ApiVersions()
        apiVersions.update("1", oldApis)
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 5,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )

        // if API is too old to be usable, we just skip validation
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 10)
            )
        )
        assertTrue(state.hasValidPosition(tp0))

        // New API
        apiVersions.update("1", NodeApiVersions.create())
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 5,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )

        // API is too old to be usable, we just skip validation
        assertTrue(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = apiVersions,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 10),
            ),
        )
        assertFalse(state.hasValidPosition(tp0))
    }

    @Test
    fun testMaybeCompleteValidationAfterPositionChange() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val updateOffset = 20L
        val updateOffsetEpoch = 8
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val updatePosition = FetchPosition(
            offset = updateOffset,
            offsetEpoch = updateOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, updatePosition)
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(initialOffsetEpoch)
                .setEndOffset(initialOffset + 5),
        )
        assertEquals(null, truncationOpt)
        assertTrue(state.awaitingValidation(tp0))
        assertEquals(updatePosition, state.position(tp0))
    }

    @Test
    fun testMaybeCompleteValidationAfterOffsetReset() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        state.requestOffsetReset(tp0)
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(initialOffsetEpoch)
                .setEndOffset(initialOffset + 5),
        )
        assertNull(truncationOpt)
        assertFalse(state.awaitingValidation(tp0))
        assertTrue(state.isOffsetResetNeeded(tp0))
        assertNull(state.position(tp0))
    }

    @Test
    fun testTruncationDetectionWithResetPolicy() {
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val divergentOffset = 5L
        val divergentOffsetEpoch = 7
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(divergentOffsetEpoch)
                .setEndOffset(divergentOffset),
        )
        assertEquals(null, truncationOpt)
        assertFalse(state.awaitingValidation(tp0))
        val updatedPosition = FetchPosition(
            offset = divergentOffset,
            offsetEpoch = divergentOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        assertEquals(updatedPosition, state.position(tp0))
    }

    @Test
    fun testTruncationDetectionWithoutResetPolicy() {
        val broker1 = Node(1, "localhost", 9092)
        state = SubscriptionState(LogContext(), OffsetResetStrategy.NONE)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val divergentOffset = 5L
        val divergentOffsetEpoch = 7
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(divergentOffsetEpoch)
                .setEndOffset(divergentOffset),
        )
        val truncation = assertNotNull(truncationOpt)
        assertEquals(
            expected = OffsetAndMetadata(divergentOffset, divergentOffsetEpoch, ""),
            actual = truncation.divergentOffset,
        )
        assertEquals(initialPosition, truncation.fetchPosition)
        assertTrue(state.awaitingValidation(tp0))
    }

    @Test
    fun testTruncationDetectionUnknownDivergentOffsetWithResetPolicy() {
        val broker1 = Node(1, "localhost", 9092)
        state = SubscriptionState(LogContext(), OffsetResetStrategy.EARLIEST)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
                .setEndOffset(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET),
        )
        assertNull(truncationOpt)
        assertFalse(state.awaitingValidation(tp0))
        assertTrue(state.isOffsetResetNeeded(tp0))
        assertEquals(OffsetResetStrategy.EARLIEST, state.resetStrategy(tp0))
    }

    @Test
    fun testTruncationDetectionUnknownDivergentOffsetWithoutResetPolicy() {
        val broker1 = Node(1, "localhost", 9092)
        state = SubscriptionState(LogContext(), OffsetResetStrategy.NONE)
        state.assignFromUser(setOf(tp0))
        val currentEpoch = 10
        val initialOffset = 10L
        val initialOffsetEpoch = 5
        val initialPosition = FetchPosition(
            offset = initialOffset,
            offsetEpoch = initialOffsetEpoch,
            currentLeader = LeaderAndEpoch(leader = broker1, epoch = currentEpoch),
        )
        state.seekUnvalidated(tp0, initialPosition)
        assertTrue(state.awaitingValidation(tp0))
        val truncationOpt = state.maybeCompleteValidation(
            tp = tp0,
            requestPosition = initialPosition,
            epochEndOffset = OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
                .setEndOffset(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET),
        )
        val truncation = assertNotNull(truncationOpt)
        assertNull(truncation.divergentOffset)
        assertEquals(initialPosition, truncation.fetchPosition)
        assertTrue(state.awaitingValidation(tp0))
    }

    private class MockRebalanceListener : ConsumerRebalanceListener {

        var revoked: Collection<TopicPartition>? = null

        var assigned: Collection<TopicPartition>? = null

        var revokedCount = 0

        var assignedCount = 0

        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            assigned = partitions
            assignedCount++
        }

        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            revoked = partitions
            revokedCount++
        }
    }

    @Test
    fun resetOffsetNoValidation() {
        // Check that offset reset works when we can't validate offsets (older brokers)
        val broker1 = Node(1, "localhost", 9092)
        state.assignFromUser(setOf(tp0))

        // Reset offsets
        state.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)

        // Attempt to validate with older API version, should do nothing
        val oldApis = ApiVersions()
        oldApis.update(
            nodeId = "1",
            nodeApiVersions = NodeApiVersions.create(
                apiKey = ApiKeys.OFFSET_FOR_LEADER_EPOCH.id,
                minVersion = 0,
                maxVersion = 2,
            )
        )
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = oldApis,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = null),
            )
        )
        assertFalse(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertTrue(state.isOffsetResetNeeded(tp0))

        // Complete the reset via unvalidated seek
        state.seekUnvalidated(tp0, FetchPosition(10L))
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertFalse(state.isOffsetResetNeeded(tp0))

        // Next call to validate offsets does nothing
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = oldApis,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = null),
            )
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertFalse(state.isOffsetResetNeeded(tp0))

        // Reset again, and complete it with a seek that would normally require validation
        state.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST)
        state.seekUnvalidated(
            tp = tp0,
            position = FetchPosition(
                offset = 10L,
                offsetEpoch = 10,
                currentLeader = LeaderAndEpoch(leader = broker1, epoch = 2)
            )
        )

        // We are now in AWAIT_VALIDATION
        assertFalse(state.hasValidPosition(tp0))
        assertTrue(state.awaitingValidation(tp0))
        assertFalse(state.isOffsetResetNeeded(tp0))

        // Now ensure next call to validate clears the validation state
        assertFalse(
            state.maybeValidatePositionForCurrentLeader(
                apiVersions = oldApis,
                tp = tp0,
                leaderAndEpoch = LeaderAndEpoch(leader = broker1, epoch = 2),
            )
        )
        assertTrue(state.hasValidPosition(tp0))
        assertFalse(state.awaitingValidation(tp0))
        assertFalse(state.isOffsetResetNeeded(tp0))
    }

    @Test
    fun nullPositionLagOnNoPosition() {
        state.assignFromUser(setOf(tp0))
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_UNCOMMITTED))
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_COMMITTED))
        state.updateHighWatermark(tp0, 1L)
        state.updateLastStableOffset(tp0, 1L)
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_UNCOMMITTED))
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_COMMITTED))
    }
}
