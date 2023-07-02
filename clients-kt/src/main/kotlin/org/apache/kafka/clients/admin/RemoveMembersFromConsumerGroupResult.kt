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

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.protocol.Errors

/**
 * The result of the [Admin.removeMembersFromConsumerGroup] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
class RemoveMembersFromConsumerGroupResult internal constructor(
    private val future: KafkaFuture<Map<MemberIdentity, Errors>>,
    private val memberInfos: Set<MemberToRemove>
) {
    /**
     * Returns a future which indicates whether the request was 100% success, i.e. no
     * either top level or member level error.
     * If not, the first member error shall be returned.
     */
    fun all(): KafkaFuture<Unit> {
        val result = KafkaFutureImpl<Unit>()
        future.whenComplete { memberErrors, throwable ->
            if (throwable != null) result.completeExceptionally(throwable)
            else {
                requireNotNull(memberErrors)
                if (removeAll()) {
                    for ((key, value) in memberErrors) {
                        val exception = value.exception
                        if (exception != null) {
                            val ex: Throwable = KafkaException(
                                message = "Encounter exception when trying to remove: $key",
                                cause = exception,
                            )
                            result.completeExceptionally(ex)
                            return@whenComplete
                        }
                    }
                } else {
                    for (memberToRemove in memberInfos) {
                        if (maybeCompleteExceptionally(
                                memberErrors,
                                memberToRemove.toMemberIdentity(),
                                result
                            )
                        ) {
                            return@whenComplete
                        }
                    }
                }
                result.complete(Unit)
            }
        }
        return result
    }

    /**
     * Returns the selected member future.
     */
    fun memberResult(member: MemberToRemove): KafkaFuture<Unit> {
        require(!removeAll()) { "The method: memberResult is not applicable in 'removeAll' mode" }
        require(memberInfos.contains(member)) { "Member $member was not included in the original request" }
        val result = KafkaFutureImpl<Unit>()
        future.whenComplete { memberErrors, throwable ->
            if (throwable != null) result.completeExceptionally(throwable)
            else if (!maybeCompleteExceptionally(
                    requireNotNull(memberErrors),
                    member.toMemberIdentity(),
                    result
                )
            ) result.complete(Unit)
        }
        return result
    }

    private fun maybeCompleteExceptionally(
        memberErrors: Map<MemberIdentity, Errors>,
        member: MemberIdentity,
        result: KafkaFutureImpl<Unit>
    ): Boolean {
        val exception = KafkaAdminClient.getSubLevelError(
            subLevelErrors = memberErrors,
            subKey = member,
            keyNotFoundMsg = "Member \"$member\" was not included in the removal response"
        )
        return if (exception != null) {
            result.completeExceptionally(exception)
            true
        } else false
    }

    private fun removeAll(): Boolean = memberInfos.isEmpty()
}
