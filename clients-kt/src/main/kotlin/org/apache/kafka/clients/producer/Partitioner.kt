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

package org.apache.kafka.clients.producer

import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Configurable
import java.io.Closeable

/**
 * Partitioner Interface
 */
interface Partitioner : Configurable, Closeable {
    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int

    /**
     * This is called when partitioner is closed.
     */
    override fun close()

    /**
     * Note this method is only implemented in DefatultPartitioner and UniformStickyPartitioner
     * which are now deprecated. See KIP-794 for more info.
     *
     * Notifies the partitioner a new batch is about to be created. When using the sticky
     * partitioner, this method can change the chosen sticky partition for the new batch.
     * @param topic The topic name
     * @param cluster The current cluster metadata
     * @param prevPartition The partition previously selected for the record that triggered a new
     * batch
     */
    @Deprecated("")
    fun onNewBatch(topic: String, cluster: Cluster, prevPartition: Int) = Unit
}
