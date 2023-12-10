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

package org.apache.kafka.clients

import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchMetadata
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * FetchSessionHandler maintains the fetch session state for connecting to a broker.
 *
 * Using the protocol outlined by KIP-227, clients can create incremental fetch sessions. These
 * sessions allow the client to fetch information about a set of partition over and over, without
 * explicitly enumerating all the partitions in the request and the response.
 *
 * FetchSessionHandler tracks the partitions which are in the session. It also determines which
 * partitions need to be included in each fetch request, and what the attached fetch session
 * metadata should be for each request. The corresponding class on the receiving broker side is
 * FetchManager.
 */
open class FetchSessionHandler(logContext: LogContext, node: Int) {

    private val log: Logger

    private val node: Int

    /**
     * The metadata for the next fetch request.
     */
    private var nextMetadata = FetchMetadata.INITIAL

    /**
     * All the partitions which exist in the fetch request session.
     */
    private var sessionPartitions = mutableMapOf<TopicPartition, FetchRequest.PartitionData>()

    /**
     * All the topic names mapped to topic ids for topics which exist in the fetch request session.
     */
    private var sessionTopicNames = mapOf<Uuid, String>()

    // visible for testing
    internal val sessionId: Int
        get() = nextMetadata.sessionId

    init {
        log = logContext.logger(FetchSessionHandler::class.java)
        this.node = node
    }

    fun sessionTopicNames(): Map<Uuid, String> =  sessionTopicNames

    /**
     *
     * @property toSend The partitions to send in the fetch request.
     * @property toForget The partitions to send in the request's "forget" list.
     * @property toReplace The partitions to send in the request's "forget" list if the version is
     * >= 13.
     * @property sessionPartitions All the partitions which exist in the fetch request session.
     * @property metadata The metadata to use in this fetch request.
     * @property canUseTopicIds A boolean indicating whether we have a topic ID for every topic in
     * the request so that we can send a request that uses topic IDs.
     */
    data class FetchRequestData internal constructor(
        val toSend: Map<TopicPartition, FetchRequest.PartitionData>,
        val toForget: List<TopicIdPartition>,
        val toReplace: List<TopicIdPartition>,
        val sessionPartitions: Map<TopicPartition, FetchRequest.PartitionData>,
        val metadata: FetchMetadata,
        val canUseTopicIds: Boolean,
    ) {

        /**
         * Get the set of partitions to send in this fetch request.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("toSend"),
        )
        fun toSend(): Map<TopicPartition, FetchRequest.PartitionData> = toSend

        /**
         * Get a list of partitions to forget in this fetch request.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("toForget"),
        )
        fun toForget(): List<TopicIdPartition> = toForget

        /**
         * Get a list of partitions to forget in this fetch request.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("toReplace"),
        )
        fun toReplace(): List<TopicIdPartition> = toReplace

        /**
         * Get the full set of partitions involved in this fetch request.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("sessionPartitions"),
        )
        fun sessionPartitions(): Map<TopicPartition, FetchRequest.PartitionData> = sessionPartitions

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("metadata"),
        )
        fun metadata(): FetchMetadata = metadata

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("canUseTopicIds"),
        )
        fun canUseTopicIds(): Boolean = canUseTopicIds

        override fun toString(): String {
            val bld: StringBuilder
            if (metadata.isFull) {
                bld = StringBuilder("FullFetchRequest(toSend=(")
                var prefix = ""
                for (partition in toSend.keys) {
                    bld.append(prefix)
                    bld.append(partition)
                    prefix = ", "
                }
            } else {
                bld = StringBuilder("IncrementalFetchRequest(toSend=(")
                var prefix = ""
                for (partition in toSend.keys) {
                    bld.append(prefix)
                    bld.append(partition)
                    prefix = ", "
                }
                bld.append("), toForget=(")
                prefix = ""
                for (partition in toForget) {
                    bld.append(prefix)
                    bld.append(partition)
                    prefix = ", "
                }
                bld.append("), toReplace=(")
                prefix = ""
                for (partition in toReplace) {
                    bld.append(prefix)
                    bld.append(partition)
                    prefix = ", "
                }
                bld.append("), implied=(")
                prefix = ""
                for (partition in sessionPartitions.keys) {
                    if (!toSend.containsKey(partition)) {
                        bld.append(prefix)
                        bld.append(partition)
                        prefix = ", "
                    }
                }
            }
            if (canUseTopicIds) bld.append("), canUseTopicIds=True")
            else bld.append("), canUseTopicIds=False")
            bld.append(")")
            return bld.toString()
        }
    }

    inner class Builder {

        /**
         * The next partitions which we want to fetch.
         *
         * It is important to maintain the insertion order of this list by using a LinkedHashMap
         * rather than a regular Map.
         *
         * One reason is that when dealing with FULL fetch requests, if there is not enough response
         * space to return data from all partitions, the server will only return data from
         * partitions early in this list.
         *
         * Another reason is because we make use of the list ordering to optimize the preparation of
         * incremental fetch requests (see below).
         */
        private var next: LinkedHashMap<TopicPartition, FetchRequest.PartitionData>?

        private var topicNames: MutableMap<Uuid, String>

        private val copySessionPartitions: Boolean

        private var partitionsWithoutTopicIds = 0

        internal constructor() {
            next = LinkedHashMap()
            topicNames = HashMap()
            copySessionPartitions = true
        }

        internal constructor(initialSize: Int, copySessionPartitions: Boolean) {
            next = LinkedHashMap(initialSize)
            topicNames = HashMap()
            this.copySessionPartitions = copySessionPartitions
        }

        /**
         * Mark that we want data from this partition in the upcoming fetch.
         */
        fun add(topicPartition: TopicPartition, data: FetchRequest.PartitionData) {
            next!![topicPartition] = data

            // topicIds should not change between adding partitions and building, so we can use
            // putIfAbsent
            if (data.topicId == Uuid.ZERO_UUID) {
                partitionsWithoutTopicIds++
            } else {
                topicNames.putIfAbsent(data.topicId, topicPartition.topic)
            }
        }

        fun build(): FetchRequestData {
            var canUseTopicIds = partitionsWithoutTopicIds == 0
            if (nextMetadata.isFull) {
                if (log.isDebugEnabled) {
                    log.debug(
                        "Built full fetch {} for node {} with {}.",
                        nextMetadata, node, topicPartitionsToLogString(next!!.keys)
                    )
                }
                sessionPartitions = next!!
                next = null
                // Only add topic IDs to the session if we are using topic IDs.
                sessionTopicNames = if (canUseTopicIds) topicNames else emptyMap()

                val toSend = sessionPartitions.toMap()
                return FetchRequestData(
                    toSend = toSend,
                    toForget = emptyList(),
                    toReplace = emptyList(),
                    sessionPartitions = toSend,
                    metadata = nextMetadata,
                    canUseTopicIds = canUseTopicIds,
                )
            }

            val added: MutableList<TopicIdPartition> = ArrayList()
            val removed: MutableList<TopicIdPartition> = ArrayList()
            val altered: MutableList<TopicIdPartition> = ArrayList()
            val replaced: MutableList<TopicIdPartition> = ArrayList()
            val iter = sessionPartitions.entries.iterator()
            while (iter.hasNext()) {
                val entry = iter.next()
                val topicPartition = entry.key
                val prevData = entry.value
                val nextData = next!!.remove(topicPartition)
                if (nextData != null) {
                    // We basically check if the new partition had the same topic ID. If not, we add
                    // it to the "replaced" set. If the request is version 13 or higher, the
                    // replaced partition will be forgotten. In any case, we will send the new
                    // partition in the request.
                    if (prevData.topicId != nextData.topicId
                        && prevData.topicId != Uuid.ZERO_UUID
                        && nextData.topicId != Uuid.ZERO_UUID
                    ) {
                        // Re-add the replaced partition to the end of 'next'
                        next!![topicPartition] = nextData
                        entry.setValue(nextData)
                        replaced.add(TopicIdPartition(prevData.topicId, topicPartition))
                    } else if (prevData != nextData) {
                        // Re-add the altered partition to the end of 'next'
                        next!![topicPartition] = nextData
                        entry.setValue(nextData)
                        altered.add(TopicIdPartition(nextData.topicId, topicPartition))
                    }
                } else {
                    // Remove this partition from the session.
                    iter.remove()
                    // Indicate that we no longer want to listen to this partition.
                    removed.add(TopicIdPartition(prevData.topicId, topicPartition))
                    // If we do not have this topic ID in the builder or the session, we can not use
                    // topic IDs.
                    if (canUseTopicIds && prevData.topicId == Uuid.ZERO_UUID) canUseTopicIds = false
                }
            }

            // Add any new partitions to the session.
            for ((topicPartition, nextData) in next!!)  {
                if (sessionPartitions.containsKey(topicPartition)) {
                    // In the previous loop, all the partitions which existed in both
                    // sessionPartitions and next were moved to the end of next, or removed from
                    // next. Therefore, once we hit one of them, we know there are no more unseen
                    // entries to look at in next.
                    break
                }
                sessionPartitions[topicPartition] = nextData
                added.add(TopicIdPartition(nextData.topicId, topicPartition))
            }

            // Add topic IDs to session if we can use them. If an ID is inconsistent, we will handle
            // in the receiving broker. If we switched from using topic IDs to not using them (or
            // vice versa), that error will also be handled in the receiving broker.
            sessionTopicNames = if (canUseTopicIds) topicNames else emptyMap()

            if (log.isDebugEnabled) {
                log.debug(
                    "Built incremental fetch {} for node {}. Added {}, altered {}, removed {}, " +
                            "replaced {} out of {}",
                    nextMetadata,
                    node,
                    topicIdPartitionsToLogString(added),
                    topicIdPartitionsToLogString(altered),
                    topicIdPartitionsToLogString(removed),
                    topicIdPartitionsToLogString(replaced),
                    topicPartitionsToLogString(sessionPartitions.keys),
                )
            }
            val toSend = next!!.toMap()
            val curSessionPartitions =
                if (copySessionPartitions) sessionPartitions.toMap()
                else sessionPartitions
            next = null
            return FetchRequestData(
                toSend = toSend,
                toForget = removed,
                toReplace = replaced,
                sessionPartitions = curSessionPartitions,
                metadata = nextMetadata,
                canUseTopicIds = canUseTopicIds,
            )
        }
    }

    open fun newBuilder(): Builder = Builder()

    /**
     * A builder that allows for presizing the PartitionData hashmap, and avoiding making a
     * secondary copy of the sessionPartitions, in cases where this is not necessarily. This builder
     * is primarily for use by the Replica Fetcher.
     *
     * @param size the initial size of the PartitionData hashmap
     * @param copySessionPartitions boolean denoting whether the builder should make a deep copy of
     * session partitions
     */
    fun newBuilder(size: Int, copySessionPartitions: Boolean): Builder =
        Builder(size, copySessionPartitions)

    private fun topicPartitionsToLogString(partitions: Collection<TopicPartition>): String {
        return if (!log.isTraceEnabled) String.format("%d partition(s)", partitions.size)
        else "(${partitions.joinToString(", ")})"
    }

    private fun topicIdPartitionsToLogString(partitions: Collection<TopicIdPartition>): String {
        return if (!log.isTraceEnabled) String.format("%d partition(s)", partitions.size)
        else "(${partitions.joinToString(", ")})"
    }

    /**
     * Verify that a full fetch response contains all the partitions in the fetch session.
     *
     * @param topicPartitions The topicPartitions from the FetchResponse.
     * @param ids The topic IDs from the FetchResponse.
     * @param version The version of the FetchResponse.
     * @return `null` if the full fetch response partitions are valid; human-readable problem
     * description otherwise.
     */
    fun verifyFullFetchResponsePartitions(
        topicPartitions: Set<TopicPartition>,
        ids: Set<Uuid>,
        version: Short,
    ): String? {
        val bld = StringBuilder()
        val extra = findMissing(topicPartitions, sessionPartitions.keys)
        val omitted = findMissing(sessionPartitions.keys, topicPartitions)
        var extraIds: Set<Uuid> = hashSetOf()

        if (version >= 13) extraIds = findMissing(ids, sessionTopicNames.keys)
        if (omitted.isNotEmpty()) bld.append("omittedPartitions=(")
            .append(omitted.joinToString(", "))
            .append("), ")

        if (extra.isNotEmpty()) bld.append("extraPartitions=(")
            .append(extra.joinToString(", "))
            .append("), ")

        if (extraIds.isNotEmpty()) bld.append("extraIds=(")
            .append(extraIds.joinToString(", "))
            .append("), ")

        if (omitted.isNotEmpty() || extra.isNotEmpty() || extraIds.isNotEmpty()) {
            bld.append("response=(")
                .append(topicPartitions.joinToString(", "))
                .append(")")

            return bld.toString()
        }
        return null
    }

    /**
     * Verify that the partitions in an incremental fetch response are contained in the session.
     *
     * @param topicPartitions The topicPartitions from the FetchResponse.
     * @param ids The topic IDs from the FetchResponse.
     * @param version The version of the FetchResponse.
     * @return `null` if the incremental fetch response partitions are valid; human-readable problem
     * description otherwise.
     */
    fun verifyIncrementalFetchResponsePartitions(
        topicPartitions: Set<TopicPartition>,
        ids: Set<Uuid>,
        version: Short,
    ): String? {
        var extraIds: Set<Uuid> = hashSetOf()
        if (version >= 13) extraIds = findMissing(ids, sessionTopicNames.keys)

        val extra = findMissing(topicPartitions, sessionPartitions.keys)
        val bld = StringBuilder()
        if (extra.isNotEmpty()) bld.append("extraPartitions=(")
            .append(extra.joinToString(", "))
            .append("), ")
        if (extraIds.isNotEmpty()) bld.append("extraIds=(")
            .append(extraIds.joinToString(", "))
            .append("), ")
        if (extra.isNotEmpty() || extraIds.isNotEmpty()) {
            bld.append("response=(")
                .append(topicPartitions.joinToString(", "))
                .append(")")

            return bld.toString()
        }
        return null
    }

    /**
     * Create a string describing the partitions in a FetchResponse.
     *
     * @param topicPartitions The topicPartitions from the FetchResponse.
     * @return The string to log.
     */
    private fun responseDataToLogString(topicPartitions: Set<TopicPartition>): String {
        if (!log.isTraceEnabled) {
            val implied = sessionPartitions.size - topicPartitions.size
            return if (implied > 0) String.format(
                " with %d response partition(s), %d implied partition(s)",
                topicPartitions.size,
                implied,
            )
            else String.format(
                " with %d response partition(s)",
                topicPartitions.size,
            )
        }
        val bld = StringBuilder()
        bld.append(" with response=(")
            .append(topicPartitions.joinToString(", "))
            .append(")")
        var prefix = ", implied=("
        var suffix = ""
        for (partition in sessionPartitions.keys) {
            if (!topicPartitions.contains(partition)) {
                bld.append(prefix)
                bld.append(partition)
                prefix = ", "
                suffix = ")"
            }
        }
        bld.append(suffix)
        return bld.toString()
    }

    /**
     * Handle the fetch response.
     *
     * @param response The response.
     * @param version The version of the request.
     * @return `true` if the response is well-formed; false if it can't be processed because of
     * missing or unexpected partitions.
     */
    open fun handleResponse(response: FetchResponse, version: Short): Boolean {
        if (response.error() !== Errors.NONE) {
            log.info(
                "Node {} was unable to process the fetch request with {}: {}.",
                node,
                nextMetadata,
                response.error(),
            )
            nextMetadata =
                if (response.error() === Errors.FETCH_SESSION_ID_NOT_FOUND) FetchMetadata.INITIAL
                else nextMetadata.nextCloseExistingAttemptNew()

            return false
        }
        val topicPartitions = response.responseData(sessionTopicNames, version).keys
        return if (nextMetadata.isFull) {
            if (topicPartitions.isEmpty() && response.throttleTimeMs() > 0) {
                // Normally, an empty full fetch response would be invalid. However, KIP-219
                // specifies that if the broker wants to throttle the client, it will respond
                // to a full fetch request with an empty response and a throttleTimeMs
                // value set. We don't want to log this with a warning, since it's not an error.
                // However, the empty full fetch response can't be processed, so it's still
                // appropriate to return false here.
                if (log.isDebugEnabled) log.debug(
                    "Node {} sent a empty full fetch response to indicate that this " +
                            "client should be throttled for {} ms.",
                    node,
                    response.throttleTimeMs()
                )
                nextMetadata = FetchMetadata.INITIAL
                return false
            }
            val problem = verifyFullFetchResponsePartitions(
                topicPartitions = topicPartitions,
                ids = response.topicIds(),
                version = version
            )

            if (problem != null) {
                log.info("Node {} sent an invalid full fetch response with {}", node, problem)
                nextMetadata = FetchMetadata.INITIAL
                false
            } else if (response.sessionId() == FetchMetadata.INVALID_SESSION_ID) {
                if (log.isDebugEnabled) log.debug(
                    "Node {} sent a full fetch response{}",
                    node,
                    responseDataToLogString(topicPartitions),
                )
                nextMetadata = FetchMetadata.INITIAL
                true
            } else {
                // The server created a new incremental fetch session.
                if (log.isDebugEnabled) log.debug(
                    "Node {} sent a full fetch response that created a new incremental " +
                            "fetch session {}{}",
                    node,
                    response.sessionId(),
                    responseDataToLogString(topicPartitions),
                )
                nextMetadata = FetchMetadata.newIncremental(response.sessionId())
                true
            }
        } else {
            val problem = verifyIncrementalFetchResponsePartitions(
                topicPartitions = topicPartitions,
                ids = response.topicIds(),
                version = version,
            )
            if (problem != null) {
                log.info(
                    "Node {} sent an invalid incremental fetch response with {}",
                    node,
                    problem
                )
                nextMetadata = nextMetadata.nextCloseExistingAttemptNew()
                false
            } else if (response.sessionId() == FetchMetadata.INVALID_SESSION_ID) {
                // The incremental fetch session was closed by the server.
                if (log.isDebugEnabled) log.debug(
                    "Node {} sent an incremental fetch response closing session {}{}",
                    node,
                    nextMetadata.sessionId,
                    responseDataToLogString(topicPartitions),
                )
                nextMetadata = FetchMetadata.INITIAL
                true
            } else {
                // The incremental fetch session was continued by the server.
                // We don't have to do anything special here to support KIP-219, since an empty
                // incremental fetch request is perfectly valid.
                if (log.isDebugEnabled) log.debug(
                    "Node {} sent an incremental fetch response with throttleTimeMs = {} " +
                            "for session {}{}",
                    node,
                    response.throttleTimeMs(),
                    response.sessionId(),
                    responseDataToLogString(topicPartitions),
                )
                nextMetadata = nextMetadata.nextIncremental()
                true
            }
        }
    }

    /**
     * The client will initiate the session close on next fetch request.
     */
    fun notifyClose() {
        log.debug(
            "Set the metadata for next fetch request to close the existing session ID={}",
            nextMetadata.sessionId
        )
        nextMetadata = nextMetadata.nextCloseExisting()
    }

    /**
     * Handle an error sending the prepared request.
     *
     * When a network error occurs, we close any existing fetch session on our next request, and try
     * to create a new session.
     *
     * @param t The exception.
     */
    open fun handleError(t: Throwable?) {
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t)
        nextMetadata = nextMetadata.nextCloseExistingAttemptNew()
    }

    /**
     * Get the fetch request session's partitions.
     */
    fun sessionTopicPartitions(): Set<TopicPartition> = sessionPartitions.keys

    companion object {

        /**
         * Return missing items which are expected to be in a particular set, but which are not.
         *
         * @param toFind The items to look for.
         * @param toSearch The set of items to search.
         * @return Empty set if all items were found; some of the missing ones in a set, if not.
         */
        fun <T> findMissing(toFind: Set<T>, toSearch: Set<T>): Set<T> =
            toFind.filter { !toSearch.contains(it) }.toSet()
    }
}
