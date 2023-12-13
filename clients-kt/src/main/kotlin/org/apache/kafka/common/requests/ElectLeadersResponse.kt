package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ElectLeadersResponseData
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

class ElectLeadersResponse(
    private val data: ElectLeadersResponseData
) : AbstractResponse(apiKey = ApiKeys.ELECT_LEADERS) {

    constructor(
        throttleTimeMs: Int,
        errorCode: Short,
        electionResults: List<ReplicaElectionResult>,
        version: Short
    ) : this(data = ElectLeadersResponseData()) {
        data.setThrottleTimeMs(throttleTimeMs)
        if (version >= 1) data.setErrorCode(errorCode)
        data.setReplicaElectionResults(electionResults)
    }

    override fun data(): ElectLeadersResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun errorCounts(): Map<Errors, Int> {
        val counts = HashMap<Errors, Int>()
        updateErrorCounts(counts, Errors.forCode(data.errorCode))

        data.replicaElectionResults.forEach { result ->
            result.partitionResult.forEach { partitionResult ->
                updateErrorCounts(counts, Errors.forCode(partitionResult.errorCode))
            }
        }

        return counts
    }

    override fun shouldClientThrottle(version: Short): Boolean = true

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): ElectLeadersResponse =
            ElectLeadersResponse(ElectLeadersResponseData(ByteBufferAccessor(buffer), version))

        fun electLeadersResult(data: ElectLeadersResponseData): Map<TopicPartition, Throwable?> {
            val map: MutableMap<TopicPartition, Throwable?> = HashMap()

            data.replicaElectionResults.forEach { topicResult ->
                topicResult.partitionResult.forEach { partitionResult ->
                    val error = Errors.forCode(partitionResult.errorCode)
                    map[TopicPartition(topicResult.topic, partitionResult.partitionId)] =
                        if (error != Errors.NONE) error.exception(partitionResult.errorMessage)
                        else null
                }
            }
            return map
        }
    }
}
