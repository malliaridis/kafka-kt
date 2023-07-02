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

package org.apache.kafka.test

import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.ClusterResourceListener
import org.apache.kafka.common.serialization.Serializer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class MockSerializer : ClusterResourceListener, Serializer<ByteArray?> {

    init {
        INIT_COUNT.incrementAndGet()
    }

    override fun serialize(topic: String, data: ByteArray?): ByteArray? {
        // This will ensure that we get the cluster metadata when serialize is called for the first
        // time as subsequent compareAndSet operations will fail.
        CLUSTER_ID_BEFORE_SERIALIZE.compareAndSet(NO_CLUSTER_ID, CLUSTER_META.get())
        return data
    }

    override fun close() {
        CLOSE_COUNT.incrementAndGet()
    }

    override fun onUpdate(clusterResource: ClusterResource?) {
        CLUSTER_META.set(clusterResource)
    }

    companion object {

        val INIT_COUNT = AtomicInteger(0)

        val CLOSE_COUNT = AtomicInteger(0)

        val CLUSTER_META = AtomicReference<ClusterResource?>()

        val NO_CLUSTER_ID = ClusterResource("no_cluster_id")

        val CLUSTER_ID_BEFORE_SERIALIZE = AtomicReference(NO_CLUSTER_ID)
    }
}
