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

/**
 * A detailed description of a single group instance in the cluster.
 */
class MemberDescription(
    val memberId: String = "",
    val groupInstanceId: String? = null,
    val clientId: String = "",
    val host: String = "",
    val assignment: MemberAssignment = MemberAssignment(emptySet())
) {

    /**
     * The consumer id of the group member.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("memberId"),
    )
    fun consumerId(): String = memberId

    /**
     * The instance id of the group member.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("host"),
    )
    fun groupInstanceId(): String? = groupInstanceId

    /**
     * The client id of the group member.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("clientId"),
    )
    fun clientId(): String = clientId

    /**
     * The host where the group member is running.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("host"),
    )
    fun host(): String  = host

    /**
     * The assignment of the group member.
     */
    @Deprecated(
        message = "Replace with property instead.",
        replaceWith = ReplaceWith("assignment"),
    )
    fun assignment(): MemberAssignment = assignment

    override fun toString(): String {
        return "(memberId=$memberId" +
                ", groupInstanceId=$groupInstanceId" +
                ", clientId=$clientId" +
                ", host=$host" +
                ", assignment=$assignment)"
    }
}

