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
import java.util.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.WriteTxnMarkersRequestData
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarker
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarkerTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors


class WriteTxnMarkersRequest private constructor(
    private val data: WriteTxnMarkersRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.WRITE_TXN_MARKERS, version) {

    override fun data(): WriteTxnMarkersRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): WriteTxnMarkersResponse {
        val error = Errors.forException(e)
        val errors: MutableMap<Long, Map<TopicPartition, Errors>> = HashMap(data.markers.size)

        for (markerEntry in data.markers) {
            val errorsPerPartition: MutableMap<TopicPartition, Errors> = HashMap()
            for (topic in markerEntry.topics)
                for (partitionIdx in topic.partitionIndexes)
                    errorsPerPartition[TopicPartition(topic.name, partitionIdx)] = error

            errors[markerEntry.producerId] = errorsPerPartition
        }
        return WriteTxnMarkersResponse(errors)
    }

    fun markers(): List<TxnMarkerEntry> {
        val markers: MutableList<TxnMarkerEntry> = ArrayList()
        for (markerEntry in data.markers) {
            val topicPartitions: MutableList<TopicPartition> = ArrayList()
            for (topic in markerEntry.topics)
                for (partitionIdx in topic.partitionIndexes)
                    topicPartitions.add(TopicPartition(topic.name, partitionIdx))

            markers.add(
                TxnMarkerEntry(
                    markerEntry.producerId,
                    markerEntry.producerEpoch,
                    markerEntry.coordinatorEpoch,
                    TransactionResult.forId(markerEntry.transactionResult),
                    topicPartitions
                )
            )
        }
        return markers
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as WriteTxnMarkersRequest
        return data == that.data
    }

    override fun hashCode(): Int = data.hashCode()

    class TxnMarkerEntry(
        val producerId: Long,
        val producerEpoch: Short,
        val coordinatorEpoch: Int,
        val transactionResult: TransactionResult,
        val partitions: List<TopicPartition>,
    ) {
        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("producerId")
        )
        fun producerId(): Long = producerId

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("producerEpoch")
        )
        fun producerEpoch(): Short = producerEpoch

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("coordinatorEpoch")
        )
        fun coordinatorEpoch(): Int = coordinatorEpoch

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("transactionResult")
        )
        fun transactionResult(): TransactionResult = transactionResult

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("partitions")
        )
        fun partitions(): List<TopicPartition> = partitions

        override fun toString(): String {
            return "TxnMarkerEntry{" +
                    "producerId=$producerId" +
                    ", producerEpoch=$producerEpoch" +
                    ", coordinatorEpoch=$coordinatorEpoch" +
                    ", result=$transactionResult" +
                    ", partitions=$partitions" +
                    '}'
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TxnMarkerEntry

            if (producerId != other.producerId) return false
            if (producerEpoch != other.producerEpoch) return false
            if (coordinatorEpoch != other.coordinatorEpoch) return false
            if (transactionResult != other.transactionResult) return false
            return partitions == other.partitions
        }

        override fun hashCode(): Int {
            var result = producerId.hashCode()
            result = 31 * result + producerEpoch
            result = 31 * result + coordinatorEpoch
            result = 31 * result + transactionResult.hashCode()
            result = 31 * result + partitions.hashCode()
            return result
        }
    }

    class Builder : AbstractRequest.Builder<WriteTxnMarkersRequest> {

        val data: WriteTxnMarkersRequestData

        constructor(data: WriteTxnMarkersRequestData) : super(ApiKeys.WRITE_TXN_MARKERS) {
            this.data = data
        }

        constructor(
            version: Short,
            markers: List<TxnMarkerEntry>,
        ) : super(ApiKeys.WRITE_TXN_MARKERS, version) {
            val dataMarkers: MutableList<WritableTxnMarker> = ArrayList()
            for (marker in markers) {
                val topicMap: MutableMap<String, WritableTxnMarkerTopic> = HashMap()
                for ((topic1, partition) in marker.partitions) {
                    val topic = topicMap.getOrDefault(
                        topic1,
                        WritableTxnMarkerTopic().setName(topic1)
                    )
                    topic.partitionIndexes += partition
                    topicMap[topic1] = topic
                }
                dataMarkers.add(
                    WritableTxnMarker()
                        .setProducerId(marker.producerId)
                        .setProducerEpoch(marker.producerEpoch)
                        .setCoordinatorEpoch(marker.coordinatorEpoch)
                        .setTransactionResult(marker.transactionResult.id)
                        .setTopics(ArrayList(topicMap.values))
                )
            }
            data = WriteTxnMarkersRequestData().setMarkers(dataMarkers)
        }

        override fun build(version: Short): WriteTxnMarkersRequest {
            return WriteTxnMarkersRequest(data, version)
        }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): WriteTxnMarkersRequest {
            return WriteTxnMarkersRequest(
                WriteTxnMarkersRequestData(
                    ByteBufferAccessor(buffer),
                    version
                ), version
            )
        }
    }
}
