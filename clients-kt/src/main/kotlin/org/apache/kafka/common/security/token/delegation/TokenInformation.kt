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

package org.apache.kafka.common.security.token.delegation

import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
 * A class representing a delegation token details.
 */
@Evolving
data class TokenInformation(
    val tokenId: String,
    val owner: KafkaPrincipal,
    val tokenRequester: KafkaPrincipal = owner,
    val renewers: Collection<KafkaPrincipal>,
    val issueTimestamp: Long,
    val maxTimestamp: Long,
    var expiryTimestamp: Long
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("owner"),
    )
    fun owner(): KafkaPrincipal = owner

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenRequester"),
    )
    fun tokenRequester(): KafkaPrincipal = tokenRequester

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("owner.toString()"),
    )
    fun ownerAsString(): String = owner.toString()

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("renewers"),
    )
    fun renewers(): Collection<KafkaPrincipal> = renewers

    fun renewersAsString(): Collection<String> = renewers.map(KafkaPrincipal::toString)

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("issueTimestamp"),
    )
    fun issueTimestamp(): Long = issueTimestamp

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("expiryTimestamp"),
    )
    fun expiryTimestamp(): Long = expiryTimestamp

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tokenId"),
    )
    fun tokenId(): String = tokenId

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("maxTimestamp"),
    )
    fun maxTimestamp(): Long = maxTimestamp

    fun ownerOrRenewer(principal: KafkaPrincipal): Boolean {
        return owner == principal || tokenRequester == principal || renewers.contains(principal)
    }

    override fun toString(): String {
        return "TokenInformation{" +
                "owner=$owner" +
                ", tokenRequester=$tokenRequester" +
                ", renewers=$renewers" +
                ", issueTimestamp=$issueTimestamp" +
                ", maxTimestamp=$maxTimestamp" +
                ", expiryTimestamp=$expiryTimestamp" +
                ", tokenId='$tokenId'" +
                '}'
    }
}
