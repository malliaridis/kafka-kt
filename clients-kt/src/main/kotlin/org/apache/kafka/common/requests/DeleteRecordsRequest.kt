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

import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import java.nio.ByteBuffer

class DeleteRecordsRequest private constructor(
    private val data: DeleteRecordsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DELETE_RECORDS, version) {

    override fun data(): DeleteRecordsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val result = DeleteRecordsResponseData().setThrottleTimeMs(throttleTimeMs)
        val errorCode = Errors.forException(e).code

        for (topic in data.topics()) {
            val topicResult = DeleteRecordsTopicResult().setName(topic.name())
            result.topics().add(topicResult)

            topicResult.partitions().addAll(
                topic.partitions().map { partition ->
                    DeleteRecordsPartitionResult()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(errorCode)
                        .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                }
            )
        }

        return DeleteRecordsResponse(result)
    }

    class Builder(
        private val data: DeleteRecordsRequestData
    ) : AbstractRequest.Builder<DeleteRecordsRequest>(ApiKeys.DELETE_RECORDS) {

        override fun build(version: Short): DeleteRecordsRequest =
            DeleteRecordsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        const val HIGH_WATERMARK = -1L

        fun parse(buffer: ByteBuffer, version: Short): DeleteRecordsRequest =
            DeleteRecordsRequest(
                DeleteRecordsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
