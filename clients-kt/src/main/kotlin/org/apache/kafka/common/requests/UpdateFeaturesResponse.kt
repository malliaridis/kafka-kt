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
import org.apache.kafka.common.message.UpdateFeaturesResponseData
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResultCollection
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * - [Errors.CLUSTER_AUTHORIZATION_FAILED]
 * - [Errors.NOT_CONTROLLER]
 * - [Errors.INVALID_REQUEST]
 * - [Errors.FEATURE_UPDATE_FAILED]
 */
class UpdateFeaturesResponse(
    private val data: UpdateFeaturesResponseData,
) : AbstractResponse(ApiKeys.UPDATE_FEATURES) {

    fun topLevelError(): ApiError = ApiError(Errors.forCode(data.errorCode), data.errorMessage)

    override fun errorCounts(): Map<Errors, Int> {
        val errorCounts = mutableMapOf<Errors, Int>()

        updateErrorCounts(errorCounts, Errors.forCode(data.errorCode))

        for (result in data.results)
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode))

        return errorCounts
    }

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    override fun toString(): String = data.toString()

    override fun data(): UpdateFeaturesResponseData = data

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): UpdateFeaturesResponse {
            return UpdateFeaturesResponse(
                UpdateFeaturesResponseData(ByteBufferAccessor(buffer), version)
            )
        }

        fun createWithErrors(
            topLevelError: ApiError,
            updateErrors: Map<String, ApiError>,
            throttleTimeMs: Int,
        ): UpdateFeaturesResponse {
            val results = UpdatableFeatureResultCollection()

            for ((feature, error) in updateErrors) {
                val result = UpdatableFeatureResult()
                result.setFeature(feature)
                    .setErrorCode(error.error.code)
                    .setErrorMessage(error.message)
                results.add(result)
            }
            val responseData = UpdateFeaturesResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(topLevelError.error.code)
                .setErrorMessage(topLevelError.message)
                .setResults(results)
                .setThrottleTimeMs(throttleTimeMs)

            return UpdateFeaturesResponse(responseData)
        }
    }
}
