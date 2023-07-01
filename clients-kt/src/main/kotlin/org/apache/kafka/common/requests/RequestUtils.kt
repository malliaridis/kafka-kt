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

package org.apache.kafka.common.requests

import java.nio.ByteBuffer
import java.util.function.Predicate
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Message
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.Records

object RequestUtils {

    fun getLeaderEpoch(leaderEpoch: Int): Int? {
        return if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) null
        else leaderEpoch
    }

    fun hasTransactionalRecords(request: ProduceRequest): Boolean {
        return flag(request, RecordBatch::isTransactional)
    }

    /**
     * find a flag from all records of a produce request.
     * @param request produce request
     * @param predicate used to predicate the record
     * @return true if there is any matched flag in the produce request. Otherwise, false
     */
    fun flag(request: ProduceRequest, predicate: Predicate<RecordBatch>): Boolean {
        for (tp in request.data().topicData()) {
            for (p in tp.partitionData()) {
                if (p.records() is Records) {
                    val iter: Iterator<RecordBatch> = (p.records() as Records).batchIterator()
                    if (iter.hasNext() && predicate.test(iter.next())) return true
                }
            }
        }
        return false
    }

    fun serialize(
        header: Message,
        headerVersion: Short,
        apiMessage: Message,
        apiVersion: Short
    ): ByteBuffer {
        val cache = ObjectSerializationCache()
        val headerSize = header.size(cache, headerVersion)
        val messageSize = apiMessage.size(cache, apiVersion)
        val writable = ByteBufferAccessor(ByteBuffer.allocate(headerSize + messageSize))
        header.write(writable, cache, headerVersion)
        apiMessage.write(writable, cache, apiVersion)
        writable.flip()
        return writable.buffer()
    }
}
