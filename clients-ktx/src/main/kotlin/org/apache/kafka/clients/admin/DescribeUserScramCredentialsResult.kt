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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.errors.ResourceNotFoundException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData
import org.apache.kafka.common.protocol.Errors

/**
 * The result of the [Admin.describeUserScramCredentials] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 *
 * @constructor Package-private constructor
 * @property dataFuture the future indicating response data from the call
 */
@Evolving
class DescribeUserScramCredentialsResult internal constructor(
    private val dataFuture: KafkaFuture<DescribeUserScramCredentialsResponseData>,
) {

    /**
     * @return a future for the results of all described users with map keys (one per user) being
     * consistent with the contents of the list returned by [users]. The future will complete
     * successfully only if all such user descriptions complete successfully.
     */
    fun all(): KafkaFuture<Map<String, UserScramCredentialsDescription>> {
        val retval = KafkaFutureImpl<Map<String, UserScramCredentialsDescription>>()
        dataFuture.whenComplete { data, throwable ->
            if (throwable != null) retval.completeExceptionally(throwable)
            else {
                /* Check to make sure every individual described user succeeded. Note that a
                 * successfully described user is one that appears with *either* a NONE error code
                 * or a RESOURCE_NOT_FOUND error code. The RESOURCE_NOT_FOUND means the client
                 * explicitly requested a describe of that particular user but it could not be
                 * described because it does not exist; such a user will not appear as a key in the
                 * returned map.
                 */
                val firstFailedDescribe = data!!.results().firstOrNull { result ->
                    result.errorCode() != Errors.NONE.code
                            && result.errorCode() != Errors.RESOURCE_NOT_FOUND.code
                }

                if (firstFailedDescribe != null) retval.completeExceptionally(
                    Errors.forCode(firstFailedDescribe.errorCode())
                        .exception(firstFailedDescribe.errorMessage())
                ) else {
                    val retvalMap = data.results().associate { userResult ->
                        userResult.user() to UserScramCredentialsDescription(
                            name = userResult.user(),
                            credentialInfos = getScramCredentialInfosFor(userResult)
                        )
                    }
                    retval.complete(retvalMap)
                }
            }
        }
        return retval
    }

    /**
     *
     * @return a future indicating the distinct users that meet the request criteria and that have
     * at least one credential. The future will not complete successfully if the user is not
     * authorized to perform the describe operation; otherwise, it will complete successfully as
     * long as the list of users with credentials can be successfully determined within some
     * hard-coded timeout period. Note that the returned list will not include users that do not
     * exist/have no credentials: a request to describe an explicit list of users, none of which
     * existed/had a credential, will result in a future that returns an empty list being returned
     * here. A returned list will include users that have a credential but that could not be
     * described.
     */
    fun users(): KafkaFuture<List<String>> {
        val retval = KafkaFutureImpl<List<String>>()
        dataFuture.whenComplete { data, throwable ->
            if (throwable != null) retval.completeExceptionally(throwable)
            else retval.complete(
                data!!.results()
                    .filter { result -> result.errorCode() != Errors.RESOURCE_NOT_FOUND.code }
                    .map { result -> result.user() }
            )
        }
        return retval
    }

    /**
     * @param userName the name of the user description being requested
     * @return a future indicating the description results for the given user. The future will
     * complete exceptionally if the future returned by [users] completes exceptionally. Note that
     * if the given user does not exist in the list of described users then the returned future will
     * complete exceptionally with [org.apache.kafka.common.errors.ResourceNotFoundException].
     */
    fun description(userName: String): KafkaFuture<UserScramCredentialsDescription> {
        val retval = KafkaFutureImpl<UserScramCredentialsDescription>()
        dataFuture.whenComplete { data, throwable ->
            if (throwable != null) retval.completeExceptionally(throwable)
            else {
                // it is possible that there is no future for this user (for example, the original
                // describe request was for users 1, 2, and 3 but this is looking for user 4), so
                // explicitly take care of that case
                val userResult = data!!.results()
                    .firstOrNull { result -> (result.user() == userName) }

                if (userResult == null) retval.completeExceptionally(
                    ResourceNotFoundException("No such user: $userName"),
                ) else {
                    // RESOURCE_NOT_FOUND is included here
                    if (userResult.errorCode() != Errors.NONE.code) retval.completeExceptionally(
                        Errors.forCode(userResult.errorCode())
                            .exception(userResult.errorMessage())
                    )
                    else retval.complete(
                        UserScramCredentialsDescription(
                            name = userResult.user(),
                            credentialInfos = getScramCredentialInfosFor(userResult),
                        )
                    )
                }
            }
        }
        return retval
    }

    companion object {

        private fun getScramCredentialInfosFor(
            userResult: DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult,
        ): List<ScramCredentialInfo> = userResult.credentialInfos().map { credentialInfo ->
                ScramCredentialInfo(
                    mechanism = ScramMechanism.fromType(credentialInfo.mechanism()),
                    iterations = credentialInfo.iterations(),
                )
            }
    }
}
