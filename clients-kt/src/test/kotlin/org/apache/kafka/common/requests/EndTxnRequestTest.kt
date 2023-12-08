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

import org.apache.kafka.common.message.EndTxnRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class EndTxnRequestTest {

    @Test
    fun testConstructor() {
        val producerEpoch: Short = 0
        val producerId = 1
        val transactionId = "txn_id"
        val throttleTimeMs = 10
        val builder = EndTxnRequest.Builder(
            EndTxnRequestData()
                .setCommitted(true)
                .setProducerEpoch(producerEpoch)
                .setProducerId(producerId.toLong())
                .setTransactionalId(transactionId)
        )
        for (version in ApiKeys.END_TXN.allVersions()) {
            val request = builder.build(version)
            val response = request.getErrorResponse(
                throttleTimeMs = throttleTimeMs,
                e = Errors.NOT_COORDINATOR.exception!!,
            )
            assertEquals(mapOf(Errors.NOT_COORDINATOR to 1), response.errorCounts())
            assertEquals(TransactionResult.COMMIT, request.result)
            assertEquals(throttleTimeMs, response.throttleTimeMs())
        }
    }
}
