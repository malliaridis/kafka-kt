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

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import java.nio.ByteBuffer
import java.util.stream.Collectors
import java.util.stream.Stream

class AlterUserScramCredentialsRequest private constructor(
    private val data: AlterUserScramCredentialsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, version) {

    override fun data(): AlterUserScramCredentialsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error, errorMessage) = ApiError.fromThrowable(e)
        val errorCode = error.code
        val users =
            data.deletions.map { deletion -> deletion.name } +
                data.upsertions.map { upsertion -> upsertion.name }

        val results = users.sorted().map { user ->
            AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                .setUser(user)
                .setErrorCode(errorCode)
                .setErrorMessage(errorMessage)
        }

        return AlterUserScramCredentialsResponse(
            AlterUserScramCredentialsResponseData().setResults(results)
        )
    }

    class Builder(
        private val data: AlterUserScramCredentialsRequestData,
    ) : AbstractRequest.Builder<AlterUserScramCredentialsRequest>(
        ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
    ) {

        override fun build(version: Short): AlterUserScramCredentialsRequest =
            AlterUserScramCredentialsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): AlterUserScramCredentialsRequest =
            AlterUserScramCredentialsRequest(
                AlterUserScramCredentialsRequestData(ByteBufferAccessor(buffer), version), version
            )
    }
}
