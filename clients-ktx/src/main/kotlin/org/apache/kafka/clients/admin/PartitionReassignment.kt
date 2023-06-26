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

/**
 * A partition reassignment, which has been listed via [AdminClient.listPartitionReassignments].
 *
 * @property replicas The brokers which this partition currently resides on.
 * @property addingReplicas The brokers that we are adding this partition to as part of a
 * reassignment. A subset of replicas.
 * @property removingReplicas The brokers that we are removing this partition from as part of a
 * reassignment. A subset of replicas.
 */
data class PartitionReassignment(
    val replicas: List<Int>,
    val addingReplicas: List<Int>,
    val removingReplicas: List<Int>,
) {

    /**
     * The brokers which this partition currently resides on.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("replicas"),
    )
    fun replicas(): List<Int> = replicas

    /**
     * The brokers that we are adding this partition to as part of a reassignment. A subset of
     * replicas.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("addingReplicas"),
    )
    fun addingReplicas(): List<Int> = addingReplicas

    /**
     * The brokers that we are removing this partition from as part of a reassignment. A subset of
     * replicas.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("removingReplicas"),
    )
    fun removingReplicas(): List<Int> = removingReplicas

    override fun toString(): String {
        return "PartitionReassignment(" +
                "replicas=$replicas" +
                ", addingReplicas=$addingReplicas" +
                ", removingReplicas=$removingReplicas" +
                ')'
    }
}
