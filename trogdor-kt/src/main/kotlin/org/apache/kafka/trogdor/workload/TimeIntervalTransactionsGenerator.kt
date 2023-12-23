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
import org.apache.kafka.common.utils.Time
import org.apache.kafka.trogdor.workload.TransactionGenerator.TransactionAction

/**
 * A transactions generator where we commit a transaction every N milliseconds
 */
class TimeIntervalTransactionsGenerator internal constructor(
    @JsonProperty("transactionIntervalMs") intervalMs: Int,
    time: Time,
) : TransactionGenerator {

    private val time: Time

    private val intervalMs: Int

    private var lastTransactionStartMs = NULL_START_MS

    @JsonCreator
    constructor(@JsonProperty("transactionIntervalMs") intervalMs: Int) : this(intervalMs, Time.SYSTEM)

    init {
        require(intervalMs >= 1) { "Cannot have a negative interval" }
        this.time = time
        this.intervalMs = intervalMs
    }

    @JsonProperty
    fun transactionIntervalMs(): Int = intervalMs

    @Synchronized
    override fun nextAction(): TransactionAction {
        return if (lastTransactionStartMs == NULL_START_MS) {
            lastTransactionStartMs = time.milliseconds()
            TransactionAction.BEGIN_TRANSACTION
        } else if (time.milliseconds() - lastTransactionStartMs >= intervalMs) {
            lastTransactionStartMs = NULL_START_MS
            TransactionAction.COMMIT_TRANSACTION
        } else TransactionAction.NO_OP
    }

    companion object {
        private const val NULL_START_MS: Long = -1
    }
}
