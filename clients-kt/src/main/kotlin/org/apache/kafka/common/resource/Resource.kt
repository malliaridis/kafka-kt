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

package org.apache.kafka.common.resource

import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * Represents a cluster resource with a tuple of (type, name).
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 *
 * @constructor Create an instance of this class with the provided parameters.
 * @property resourceType resource type
 * @property name resource name
 */
@Evolving
data class Resource(
    val resourceType: ResourceType,
    val name: String,
) {

    /**
     * The resource type.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("resourceType")
    )
    fun resourceType(): ResourceType = resourceType

    /**
     * The resource name.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String = name

    /**
     * Whether this resource has any UNKNOWN components.
     */
    val isUnknown: Boolean
        get() = resourceType.isUnknown

    override fun toString(): String {
        return "(resourceType=$resourceType, name=$name)"
    }

    companion object {

        /**
         * The name of the CLUSTER resource.
         */
        const val CLUSTER_NAME = "kafka-cluster"

        /**
         * A resource representing the whole cluster.
         */
        val CLUSTER = Resource(ResourceType.CLUSTER, CLUSTER_NAME)
    }
}
