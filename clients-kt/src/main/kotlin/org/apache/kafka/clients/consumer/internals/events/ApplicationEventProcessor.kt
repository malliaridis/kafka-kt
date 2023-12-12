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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.CommitRequestManager
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata
import org.apache.kafka.clients.consumer.internals.NoopBackgroundEvent
import org.apache.kafka.clients.consumer.internals.RequestManager
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition

class ApplicationEventProcessor(
    private val backgroundEventQueue: BlockingQueue<BackgroundEvent>,
    private val registry: Map<RequestManager.Type, RequestManager?>,
    private val metadata: ConsumerMetadata,
) {

    fun process(event: ApplicationEvent): Boolean {
        return when (event.type) {
            ApplicationEvent.Type.NOOP -> process(event as NoopApplicationEvent)
            ApplicationEvent.Type.COMMIT -> process(event as CommitApplicationEvent)
            ApplicationEvent.Type.POLL -> process(event as PollApplicationEvent)
            ApplicationEvent.Type.FETCH_COMMITTED_OFFSET -> process(event as OffsetFetchApplicationEvent)
            ApplicationEvent.Type.METADATA_UPDATE -> process(event as NewTopicsMetadataUpdateRequestEvent)
            ApplicationEvent.Type.ASSIGNMENT_CHANGE -> process(event as AssignmentChangeApplicationEvent)
        }
    }

    /**
     * Processes [NoopApplicationEvent] and equeue a
     * [NoopBackgroundEvent]. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a [NoopApplicationEvent]
     */
    private fun process(event: NoopApplicationEvent): Boolean {
        return backgroundEventQueue.add(NoopBackgroundEvent(event.message))
    }

    private fun process(event: PollApplicationEvent): Boolean {
        val commitRequestManger = registry[RequestManager.Type.COMMIT] ?: return true
        (commitRequestManger as CommitRequestManager).updateAutoCommitTimer(event.pollTimeMs)
        return true
    }

    private fun process(event: CommitApplicationEvent): Boolean {
        val commitRequestManger = registry[RequestManager.Type.COMMIT]
        if (commitRequestManger == null) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront so we should never get to this block.
            val exception = KafkaException("Unable to commit offset. Most likely because the group.id wasn't set")
            event.future().completeExceptionally(exception)
            return false
        }
        (commitRequestManger as CommitRequestManager)
            .addOffsetCommitRequest(event.offsets())
            .whenComplete { _, exception ->
                if (exception != null) {
                    event.future().completeExceptionally(exception)
                    return@whenComplete
                }
                event.future().complete(null)
            }

        return true
    }

    private fun process(event: OffsetFetchApplicationEvent): Boolean {
        val commitRequestManger = registry[RequestManager.Type.COMMIT]
        val future = event.future
        if (commitRequestManger == null) {
            future.completeExceptionally(
                KafkaException(
                    "Unable to fetch committed offset because the CommittedRequestManager is not available. " +
                            "Check if group.id was set correctly"
                )
            )
            return false
        }
        (commitRequestManger as CommitRequestManager)
            .addOffsetFetchRequest(event.partitions())
            .whenComplete { response, exception ->
                if (exception != null) {
                    future.completeExceptionally(exception)
                    return@whenComplete
                }
                future.complete(response)
            }
        return true
    }

    private fun process(event: NewTopicsMetadataUpdateRequestEvent): Boolean {
        metadata.requestUpdateForNewTopics()
        return true
    }

    private fun process(event: AssignmentChangeApplicationEvent): Boolean {
        val commitRequestManger = registry[RequestManager.Type.COMMIT] ?: return false

        commitRequestManger as CommitRequestManager
        commitRequestManger.updateAutoCommitTimer(event.currentTimeMs)
        commitRequestManger.maybeAutoCommit(event.offsets)
        return true
    }
}
