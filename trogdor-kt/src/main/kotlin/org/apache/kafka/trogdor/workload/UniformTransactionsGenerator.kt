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

package org.apache.kafka.trogdor.workload

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.trogdor.workload.TransactionGenerator.TransactionAction

/**
 * A uniform transactions generator where every N records are grouped in a separate transaction
 */
class UniformTransactionsGenerator @JsonCreator constructor(
    @JsonProperty("messagesPerTransaction") messagesPerTransaction: Int,
) : TransactionGenerator {

    private val messagesPerTransaction: Int

    private var messagesInTransaction = -1

    init {
        require(messagesPerTransaction >= 1) { "Cannot have less than one message per transaction." }
        this.messagesPerTransaction = messagesPerTransaction
    }

    @JsonProperty
    fun messagesPerTransaction(): Int = messagesPerTransaction

    @Synchronized
    override fun nextAction(): TransactionAction {
        return if (messagesInTransaction == -1) {
            messagesInTransaction = 0
            TransactionAction.BEGIN_TRANSACTION
        } else if (messagesInTransaction == messagesPerTransaction) {
            messagesInTransaction = -1
            TransactionAction.COMMIT_TRANSACTION
        } else {
            messagesInTransaction += 1
            TransactionAction.NO_OP
        }
    }
}
