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
import java.util.Objects
import java.util.Optional
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.FindCoordinatorResponseData
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors

/**
 * Possible error codes:
 *
 * COORDINATOR_LOAD_IN_PROGRESS (14)
 * COORDINATOR_NOT_AVAILABLE (15)
 * GROUP_AUTHORIZATION_FAILED (30)
 * INVALID_REQUEST (42)
 * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
 */
class FindCoordinatorResponse(
    private val data: FindCoordinatorResponseData,
) : AbstractResponse(ApiKeys.FIND_COORDINATOR) {

    fun coordinatorByKey(key: String): Coordinator? {
        return if (data.coordinators.isEmpty()) {
            // version <= 3
            Coordinator()
                .setErrorCode(data.errorCode)
                .setErrorMessage(data.errorMessage)
                .setHost(data.host)
                .setPort(data.port)
                .setNodeId(data.nodeId)
                .setKey(key)
        } else data.coordinators.firstOrNull() { it.key == key } // version >= 4
    }

    override fun data(): FindCoordinatorResponseData = data

    fun node(): Node = Node(data.nodeId, data.host, data.port)

    override fun throttleTimeMs(): Int = data.throttleTimeMs

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun hasError(): Boolean = error() !== Errors.NONE

    fun error(): Errors = Errors.forCode(data.errorCode)

    override fun errorCounts(): Map<Errors, Int> {
        return if (data.coordinators.isNotEmpty()) {
            val errorCounts = mutableMapOf<Errors, Int>()
            for (coordinator in data.coordinators)
                updateErrorCounts(errorCounts, Errors.forCode(coordinator.errorCode))
            errorCounts
        } else errorCounts(error())
    }

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 2

    fun coordinators(): List<Coordinator> =
        data.coordinators.ifEmpty {
            val coordinator = Coordinator()
                .setErrorCode(data.errorCode)
                .setErrorMessage(data.errorMessage)
                .setKey("")
                .setNodeId(data.nodeId)
                .setHost(data.host)
                .setPort(data.port)
            listOf(coordinator)
        }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): FindCoordinatorResponse =
            FindCoordinatorResponse(
                FindCoordinatorResponseData(ByteBufferAccessor(buffer), version)
            )

        fun prepareOldResponse(error: Errors, node: Node): FindCoordinatorResponse {
            val data = FindCoordinatorResponseData()
            data.setErrorCode(error.code)
                .setErrorMessage(error.message)
                .setNodeId(node.id)
                .setHost(node.host)
                .setPort(node.port)

            return FindCoordinatorResponse(data)
        }

        fun prepareResponse(error: Errors, key: String, node: Node): FindCoordinatorResponse {
            val data = FindCoordinatorResponseData()
            data.setCoordinators(
                listOf(
                    Coordinator()
                        .setErrorCode(error.code)
                        .setErrorMessage(error.message)
                        .setKey(key)
                        .setHost(node.host)
                        .setPort(node.port)
                        .setNodeId(node.id)
                )
            )

            return FindCoordinatorResponse(data)
        }

        fun prepareErrorResponse(error: Errors, keys: List<String>): FindCoordinatorResponse {
            val data = FindCoordinatorResponseData()
            data.setCoordinators(
                keys.map { key ->
                    Coordinator()
                        .setErrorCode(error.code)
                        .setErrorMessage(error.message)
                        .setKey(key)
                        .setHost(Node.noNode().host)
                        .setPort(Node.noNode().port)
                        .setNodeId(Node.noNode().id)
                }
            )

            return FindCoordinatorResponse(data)
        }
    }
}
