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

package org.apache.kafka.common

/**
 * The `ClusterResource` class encapsulates metadata for a Kafka cluster.
 *
 * Create [ClusterResource] with a cluster id. Note that cluster id may be `null` if the
 * metadata request was sent to a broker without support for cluster ids. The first version of Kafka
 * to support cluster id is 0.10.1.0.
 *
 * @property clusterId
 */
data class ClusterResource(val clusterId: String?) {

    /**
     * Return the cluster id. Note that it may be `null` if the metadata request was sent to a
     * broker without support for cluster ids. The first version of Kafka to support cluster id is
     * 0.10.1.0.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("clusterId")
    )
    fun clusterId(): String? {
        return clusterId
    }

    override fun toString(): String {
        return "ClusterResource(clusterId=$clusterId)"
    }
}
