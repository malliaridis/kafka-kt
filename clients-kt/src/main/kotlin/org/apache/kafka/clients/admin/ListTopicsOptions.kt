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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import java.util.*

/**
 * Options for [Admin.listTopics].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
data class ListTopicsOptions(
    private var listInternal: Boolean = false,
) : AbstractOptions<ListTopicsOptions>() {

    /**
     * Set the timeout in milliseconds for this operation or `null` if the default api timeout for
     * the [AdminClient] should be used.
     *
     */
    // This method is retained to keep binary compatibility with 0.11
    fun timeoutMs(timeoutMs: Int?): ListTopicsOptions {
        this.timeoutMs = timeoutMs
        return this
    }

    /**
     * Set whether we should list internal topics.
     *
     * @param listInternal Whether we should list internal topics. `null` means to use the default.
     * @return This ListTopicsOptions object.
     */
    fun listInternal(listInternal: Boolean?): ListTopicsOptions {
        this.listInternal = listInternal ?: false
        return this
    }

    /**
     * Return true if we should list internal topics.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("shouldListInternal"),
    )
    fun shouldListInternal(): Boolean = listInternal

    /**
     * `true` if we should list internal topics.
     */
    val shouldListInternal: Boolean
        get() = listInternal

    override fun toString(): String = "ListTopicsOptions(listInternal=$listInternal)"
}
