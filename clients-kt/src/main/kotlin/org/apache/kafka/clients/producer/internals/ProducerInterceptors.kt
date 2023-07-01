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

package org.apache.kafka.clients.producer.internals

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch
import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * A container that holds the list [org.apache.kafka.clients.producer.ProducerInterceptor] and wraps
 * calls to the chain of custom interceptors.
 */
class ProducerInterceptors<K, V>(
    private val interceptors: List<ProducerInterceptor<K, V>>,
) : Closeable {

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets
     * serialized. The method calls [ProducerInterceptor.onSend] method. ProducerRecord returned
     * from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on
     * in the interceptor chain. The record returned from the last interceptor is returned from this
     * method.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are
     * caught and ignored. If an interceptor in the middle of the chain, that normally modifies the
     * record, throws an exception, the next interceptor in the chain will be called with a record
     * returned by the previous interceptor that did not throw an exception.
     *
     * @param record the record from client
     * @return producer record to send to topic/partition
     */
    fun onSend(record: ProducerRecord<K, V>): ProducerRecord<K, V> {
        var interceptRecord = record
        for (interceptor in interceptors) {
            try {
                interceptRecord = interceptor.onSend(interceptRecord)
            } catch (e: Exception) {
                // do not propagate interceptor exception, log and continue calling other
                // interceptors be careful not to throw exception from here
                log.warn(
                    "Error executing interceptor onSend callback for topic: {}, partition: {}",
                    record.topic,
                    record.partition,
                    e,
                )
            }
        }
        return interceptRecord
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when
     * sending the record fails before it gets sent to the server. This method calls
     * [ProducerInterceptor.onAcknowledgement] method for each interceptor.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are
     * caught and ignored.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     * If an error occurred, metadata will only contain valid topic and maybe partition.
     * @param exception The exception thrown during processing of this record. `null` if no error
     * occurred.
     */
    fun onAcknowledgement(metadata: RecordMetadata, exception: Exception?) {
        for (interceptor in interceptors) {
            try {
                interceptor.onAcknowledgement(metadata, exception)
            } catch (e: Exception) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e)
            }
        }
    }

    /**
     * This method is called when sending the record fails in
     * ([ProducerRecord][ProducerInterceptor.onSend]) method. This method calls
     * [ProducerInterceptor.onAcknowledgement] method for each interceptor
     *
     * @param record The record from client
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred
     * after partition gets assigned; the topic part of interceptTopicPartition is the same as in
     * record.
     * @param exception The exception thrown during processing of this record.
     */
    fun onSendError(
        record: ProducerRecord<K, V>?,
        interceptTopicPartition: TopicPartition?,
        exception: Exception?,
    ) {
        var interceptTopicPartition = interceptTopicPartition
        for (interceptor in interceptors) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    interceptor.onAcknowledgement(null, exception)
                } else {
                    if (interceptTopicPartition == null)
                        interceptTopicPartition = extractTopicPartition(record!!)

                    interceptor.onAcknowledgement(
                        metadata = RecordMetadata(
                            topicPartition = interceptTopicPartition,
                            baseOffset = -1,
                            batchIndex = -1,
                            timestamp = RecordBatch.NO_TIMESTAMP,
                            serializedKeySize = -1,
                            serializedValueSize = -1,
                        ),
                        exception = exception,
                    )
                }
            } catch (e: Exception) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e)
            }
        }
    }

    /**
     * Closes every interceptor in a container.
     */
    override fun close() {
        for (interceptor in interceptors) try {
            interceptor.close()
        } catch (e: Exception) {
            log.error("Failed to close producer interceptor ", e)
        }
    }

    companion object {

        private val log = LoggerFactory.getLogger(ProducerInterceptors::class.java)

        fun <K, V> extractTopicPartition(record: ProducerRecord<K, V>): TopicPartition =
            TopicPartition(
                topic = record.topic,
                partition = record.partition ?: RecordMetadata.UNKNOWN_PARTITION,
            )
    }
}
