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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import java.util.*

@Evolving
data class TransactionListing(
    val transactionalId: String,
    val producerId: Long,
    val state: TransactionState,
) {

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionalId"),
    )
    fun transactionalId(): String = transactionalId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("producerId"),
    )
    fun producerId(): Long = producerId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("transactionState"),
    )
    fun state(): TransactionState = state

    override fun toString(): String {
        return "TransactionListing(" +
                "transactionalId='$transactionalId'" +
                ", producerId=$producerId" +
                ", transactionState=$state" +
                ')'
    }
}