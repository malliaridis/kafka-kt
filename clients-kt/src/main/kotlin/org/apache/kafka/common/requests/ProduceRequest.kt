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
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceResponseData
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.utils.Utils.mkString

class ProduceRequest(
    data: ProduceRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.PRODUCE, version) {

    // This is set to null by `clearPartitionRecords` to prevent unnecessary memory retention when a
    // produce request is put in the purgatory (due to client throttling, it can take a while before
    // the response is sent). Care should be taken in methods that use this field.
    @field:Volatile
    private var data: ProduceRequestData? = data

    /**
     * We have to copy acks, timeout, transactionalId and partitionSizes from data since data maybe
     * reset to eliminate the reference to ByteBuffer but those metadata are still useful.
     */
    val acks: Short = data.acks

    val timeout: Int = data.timeoutMs

    val transactionalId: String? = data.transactionalId

    // the partitionSizes is lazily initialized since it is used by server-side in production.
    val partitionSizes: MutableMap<TopicPartition, Int> by lazy {
        // this method may be called by different thread (see the comment on data)
        val partitionSizes = mutableMapOf<TopicPartition, Int>()
        this.data!!.topicData.forEach { topicData ->
            topicData.partitionData.forEach { partitionData ->
                partitionSizes.compute(
                    TopicPartition(topicData.name, partitionData.index)
                ) { _, previousValue ->
                    (partitionData.records?.sizeInBytes() ?: 0) + (previousValue ?: 0)
                }
            }
        }
        partitionSizes
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("partitionSizes")
    )
    // visible for testing
    fun partitionSizes(): Map<TopicPartition, Int> = partitionSizes

    /**
     * @return data or IllegalStateException if the data is removed (to prevent unnecessary memory
     * retention).
     */
    override fun data(): ProduceRequestData {
        // Store it in a local variable to protect against concurrent updates
        return data ?: error(
            "The partition records are no longer available because clearPartitionRecords() has " +
                    "been invoked."
        )
    }

    override fun toString(verbose: Boolean): String {
        // Use the same format as `Struct.toString()`
        val bld = StringBuilder()
        bld.append("{acks=")
            .append(acks.toInt())
            .append(",timeout=")
            .append(timeout)
        if (verbose) bld.append(",partitionSizes=")
            .append(
                mkString(
                    partitionSizes,
                    "[",
                    "]",
                    "=",
                    ","
                )
            ) else bld.append(",numPartitions=")
            .append(partitionSizes.size)
        bld.append("}")
        return bld.toString()
    }

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): ProduceResponse? {
        // In case the producer doesn't actually want any response
        if (acks.toInt() == 0) return null
        val apiError = ApiError.fromThrowable(e)
        val data = ProduceResponseData().setThrottleTimeMs(throttleTimeMs)

        partitionSizes.forEach { (tp, _) ->
            val tpr = data.responses.find(tp.topic) ?: run {
                TopicProduceResponse().setName(tp.topic).also { data.responses.add(it) }
            }
            tpr.partitionResponses += PartitionProduceResponse()
                .setIndex(tp.partition)
                .setRecordErrors(emptyList())
                .setBaseOffset(ProduceResponse.INVALID_OFFSET)
                .setLogAppendTimeMs(RecordBatch.NO_TIMESTAMP)
                .setLogStartOffset(ProduceResponse.INVALID_OFFSET)
                .setErrorMessage(apiError.message)
                .setErrorCode(apiError.error.code)
        }
        return ProduceResponse(data)
    }

    override fun errorCounts(e: Throwable): Map<Errors, Int> {
        val error = Errors.forException(e)
        return mapOf(error to partitionSizes.size)
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("acks"),
    )
    fun acks(): Short = acks

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("timeout"),
    )
    fun timeout(): Int = timeout

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionalId"),
    )
    fun transactionalId(): String? = transactionalId

    fun clearPartitionRecords() {
        // lazily initialize partitionSizes.
        partitionSizes
        data = null
    }

    class Builder(
        minVersion: Short,
        maxVersion: Short,
        private val data: ProduceRequestData,
    ) : AbstractRequest.Builder<ProduceRequest>(ApiKeys.PRODUCE, minVersion, maxVersion) {

        override fun build(version: Short): ProduceRequest = build(version, true)

        // Visible for testing only
        fun buildUnsafe(version: Short): ProduceRequest = build(version, false)

        private fun build(version: Short, validate: Boolean): ProduceRequest {
            if (validate) {
                // Validate the given records first
                data.topicData.forEach { tpd ->
                    tpd.partitionData.forEach { partitionProduceData ->
                        validateRecords(version, partitionProduceData.records)
                    }
                }
            }
            return ProduceRequest(data, version)
        }

        override fun toString(): String {
            val bld = StringBuilder()
            bld.append("(type=ProduceRequest")
                .append(", acks=")
                .append(data.acks.toInt())
                .append(", timeout=")
                .append(data.timeoutMs)
                .append(", partitionRecords=(")
                .append(data.topicData.flatMap { it.partitionData })
                .append("), transactionalId='")
                .append(if (data.transactionalId != null) data.transactionalId else "")
                .append("'")

            return bld.toString()
        }
    }

    companion object {

        fun forMagic(magic: Byte, data: ProduceRequestData): Builder {
            // Message format upgrades correspond with a bump in the produce request version. Older
            // message format versions are generally not supported by the produce request versions
            // following the bump.
            val minVersion: Short
            val maxVersion: Short
            if (magic < RecordBatch.MAGIC_VALUE_V2) {
                minVersion = 2
                maxVersion = 2
            } else {
                minVersion = 3
                maxVersion = ApiKeys.PRODUCE.latestVersion()
            }
            return Builder(minVersion, maxVersion, data)
        }

        fun forCurrentMagic(data: ProduceRequestData): Builder =
            forMagic(RecordBatch.CURRENT_MAGIC_VALUE, data)

        fun validateRecords(version: Short, baseRecords: BaseRecords?) {
            if (version >= 3) {
                if (baseRecords is Records) {
                    val iterator = baseRecords.batches().iterator()
                    if (!iterator.hasNext()) throw InvalidRecordException(
                        "Produce requests with version $version must have at least one record batch per partition"
                    )

                    val entry = iterator.next()
                    if (entry.magic() != RecordBatch.MAGIC_VALUE_V2) throw InvalidRecordException(
                        "Produce requests with version $version are only allowed to " +
                                "contain record batches with magic version 2"
                    )

                    if (version < 7 && entry.compressionType() === CompressionType.ZSTD)
                        throw UnsupportedCompressionTypeException(
                            "Produce requests with version $version are not allowed to " +
                                    "use ZStandard compression"
                        )

                    if (iterator.hasNext()) throw InvalidRecordException(
                        "Produce requests with version $version are only allowed to " +
                                "contain exactly one record batch per partition"
                    )
                }
            }
            // Note that we do not do similar validation for older versions to ensure compatibility
            // with clients which send the wrong magic version in the wrong version of the produce
            // request. The broker did not do this validation before, so we maintain that behavior here.
        }

        fun parse(buffer: ByteBuffer, version: Short): ProduceRequest =
            ProduceRequest(
                ProduceRequestData(ByteBufferAccessor((buffer)), version),
                version
            )

        fun requiredMagicForVersion(produceRequestVersion: Short): Byte {
            require(
                produceRequestVersion >= ApiKeys.PRODUCE.oldestVersion()
                        && produceRequestVersion <= ApiKeys.PRODUCE.latestVersion()
            ) {
                "Magic value to use for produce request version $produceRequestVersion is not known"
            }

            return when (produceRequestVersion.toInt()) {
                0, 1 -> RecordBatch.MAGIC_VALUE_V0
                2 -> RecordBatch.MAGIC_VALUE_V1
                else -> RecordBatch.MAGIC_VALUE_V2
            }
        }
    }
}
