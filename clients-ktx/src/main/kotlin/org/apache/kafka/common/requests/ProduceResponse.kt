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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ProduceResponseData
import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch

/**
 * This wrapper supports both v0 and v8 of ProduceResponse.
 *
 * Possible error code:
 *
 * - [Errors.CORRUPT_MESSAGE]
 * - [Errors.UNKNOWN_TOPIC_OR_PARTITION]
 * - [Errors.NOT_LEADER_OR_FOLLOWER]
 * - [Errors.MESSAGE_TOO_LARGE]
 * - [Errors.INVALID_TOPIC_EXCEPTION]
 * - [Errors.RECORD_LIST_TOO_LARGE]
 * - [Errors.NOT_ENOUGH_REPLICAS]
 * - [Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND]
 * - [Errors.INVALID_REQUIRED_ACKS]
 * - [Errors.TOPIC_AUTHORIZATION_FAILED]
 * - [Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT]
 * - [Errors.INVALID_PRODUCER_EPOCH]
 * - [Errors.CLUSTER_AUTHORIZATION_FAILED]
 * - [Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED]
 * - [Errors.INVALID_RECORD]
 */
class ProduceResponse(private val data: ProduceResponseData) : AbstractResponse(ApiKeys.PRODUCE) {

    /**
     * Constructor for Version 0 and the latest version
     *
     * @param responses Produced data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    @Deprecated("")
    constructor(
        responses: Map<TopicPartition, PartitionResponse>,
        throttleTimeMs: Int = DEFAULT_THROTTLE_TIME,
    ) : this(data = toData(responses, throttleTimeMs))

    override fun data(): ProduceResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()

        data.responses().forEach { t: TopicProduceResponse ->
            t.partitionResponses().forEach { p: PartitionProduceResponse ->
                updateErrorCounts(errorCounts, Errors.forCode(p.errorCode()))
            }
        }

        return errorCounts
    }

    data class PartitionResponse(
        val error: Errors,
        val baseOffset: Long = INVALID_OFFSET,
        val logAppendTime: Long = RecordBatch.NO_TIMESTAMP,
        val logStartOffset: Long = INVALID_OFFSET,
        val recordErrors: List<RecordError> = emptyList(),
        val errorMessage: String? = null
    ) {

        constructor(error: Errors, errorMessage: String?) : this(
            error = error,
            baseOffset = INVALID_OFFSET,
            logAppendTime = RecordBatch.NO_TIMESTAMP,
            logStartOffset = INVALID_OFFSET,
            recordErrors = emptyList(),
            errorMessage = errorMessage
        )

        override fun toString(): String {
            return '{' +
                    "error: $error" +
                    ",offset: $baseOffset" +
                    ",logAppendTime: $logAppendTime" +
                    ", logStartOffset: $logStartOffset" +
                    ", recordErrors: $recordErrors" +
                    ", errorMessage: $errorMessage" +
                    '}'
        }
    }

    data class RecordError(
        val batchIndex: Int,
        val message: String? = null,
    ) {

        override fun toString(): String {
            return "RecordError(" +
                    "batchIndex=$batchIndex" +
                    ", message=" + (if (message == null) "null" else "'$message'") +
            ")"
        }
    }

    override fun shouldClientThrottle(version: Short): Boolean = version >= 6

    companion object {

        const val INVALID_OFFSET = -1L

        private fun toData(
            responses: Map<TopicPartition, PartitionResponse>,
            throttleTimeMs: Int,
        ): ProduceResponseData {
            val data = ProduceResponseData().setThrottleTimeMs(throttleTimeMs)

            responses.forEach { (tp: TopicPartition, response: PartitionResponse) ->

                (data.responses().find(tp.topic) ?: TopicProduceResponse().setName(tp.topic).also {
                    data.responses().add(it)
                }).apply {
                    partitionResponses().add(
                        PartitionProduceResponse()
                            .setIndex(tp.partition)
                            .setBaseOffset(response.baseOffset)
                            .setLogStartOffset(response.logStartOffset)
                            .setLogAppendTimeMs(response.logAppendTime)
                            .setErrorMessage(response.errorMessage)
                            .setErrorCode(response.error.code)
                            .setRecordErrors(
                                response.recordErrors.map { e: RecordError ->
                                    BatchIndexAndErrorMessage()
                                        .setBatchIndex(e.batchIndex)
                                        .setBatchIndexErrorMessage(e.message)
                                }
                            )
                    )
                }
            }
            return data
        }

        fun parse(buffer: ByteBuffer, version: Short): ProduceResponse {
            return ProduceResponse(ProduceResponseData(ByteBufferAccessor(buffer), version))
        }
    }
}
