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

package org.apache.kafka.clients.consumer.internals.events

import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class CommitApplicationEvent(
    private val offsets: Map<TopicPartition, OffsetAndMetadata>,
) : ApplicationEvent(Type.COMMIT) {

    private val future: CompletableFuture<Unit>

    init {
        val exception = isValid(offsets)
        if (exception != null) throw RuntimeException(exception)

        future = CompletableFuture()
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("future"),
    )
    fun future(): CompletableFuture<Unit> = future

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("offsets"),
    )
    fun offsets(): Map<TopicPartition, OffsetAndMetadata> = offsets

    private fun isValid(offsets: Map<TopicPartition, OffsetAndMetadata>): Exception? {
        for ((_, offsetAndMetadata) in offsets) {
            if (offsetAndMetadata.offset < 0)
                return IllegalArgumentException("Invalid offset: ${offsetAndMetadata.offset}")
        }
        return null
    }

    override fun toString(): String = "CommitApplicationEvent(offsets=$offsets)"
}
