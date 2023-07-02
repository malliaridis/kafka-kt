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
import org.apache.kafka.common.serialization.Deserializer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class MockDeserializer : ClusterResourceListener, Deserializer<ByteArray?> {

    var isKey = false

    var configs: Map<String, *>? = null

    init {
        initCount.incrementAndGet()
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        this.configs = configs
        this.isKey = isKey
    }

    override fun deserialize(topic: String, data: ByteArray?): ByteArray? {
        // This will ensure that we get the cluster metadata when deserialize is called for the
        // first time as subsequent compareAndSet operations will fail.
        clusterIdBeforeDeserialize.compareAndSet(noClusterId, clusterMeta.get())
        return data
    }

    override fun close() {
        closeCount.incrementAndGet()
    }

    override fun onUpdate(clusterResource: ClusterResource?) {
        clusterMeta.set(clusterResource)
    }

    companion object {

        var initCount = AtomicInteger(0)

        var closeCount = AtomicInteger(0)

        var clusterMeta = AtomicReference<ClusterResource?>()

        var noClusterId = ClusterResource("no_cluster_id")

        var clusterIdBeforeDeserialize = AtomicReference(noClusterId)

        fun resetStaticVariables() {
            initCount = AtomicInteger(0)
            closeCount = AtomicInteger(0)
            clusterMeta = AtomicReference()
            clusterIdBeforeDeserialize = AtomicReference(noClusterId)
        }
    }
}