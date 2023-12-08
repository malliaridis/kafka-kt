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

data class ProducerState(
    val producerId: Long,
    val producerEpoch: Int,
    val lastSequence: Int,
    val lastTimestamp: Long,
    val coordinatorEpoch: Int?,
    val currentTransactionStartOffset: Long?
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("producerId"),
    )
    fun producerId(): Long = producerId

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("producerEpoch"),
    )
    fun producerEpoch(): Int = producerEpoch

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("lastSequence"),
    )
    fun lastSequence(): Int = lastSequence

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("lastTimestamp"),
    )
    fun lastTimestamp(): Long = lastTimestamp

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("currentTransactionStartOffset"),
    )
    fun currentTransactionStartOffset(): Long? = currentTransactionStartOffset

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("coordinatorEpoch"),
    )
    fun coordinatorEpoch(): Int? = coordinatorEpoch

    override fun toString(): String {
        return "ProducerState(" +
                "producerId=$producerId" +
                ", producerEpoch=$producerEpoch" +
                ", lastSequence=$lastSequence" +
                ", lastTimestamp=$lastTimestamp" +
                ", coordinatorEpoch=$coordinatorEpoch" +
                ", currentTransactionStartOffset=$currentTransactionStartOffset" +
                ')'
    }
}
