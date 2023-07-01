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

/**
 * Options for [Admin.describeConfigs].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DescribeConfigsOptions : AbstractOptions<DescribeConfigsOptions>() {

    var includeSynonyms = false

    var includeDocumentation = false

    /**
     * Set the timeout in milliseconds for this operation or `null` if the default api timeout for
     * the AdminClient should be used.
     */
    // This method is retained to keep binary compatibility with 0.11
    fun timeoutMs(timeoutMs: Int?): DescribeConfigsOptions {
        this.timeoutMs = timeoutMs
        return this
    }

    /**
     * Return true if synonym configs should be returned in the response.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("includeSynonyms"),
    )
    fun includeSynonyms(): Boolean = includeSynonyms

    /**
     * Return true if config documentation should be returned in the response.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("includeDocumentation"),
    )
    fun includeDocumentation(): Boolean = includeDocumentation

    /**
     * Set to true if synonym configs should be returned in the response.
     */
    @Deprecated("User property instead")
    fun includeSynonyms(includeSynonyms: Boolean): DescribeConfigsOptions {
        this.includeSynonyms = includeSynonyms
        return this
    }

    /**
     * Set to true if config documentation should be returned in the response.
     */
    @Deprecated("User property instead")
    fun includeDocumentation(includeDocumentation: Boolean): DescribeConfigsOptions {
        this.includeDocumentation = includeDocumentation
        return this
    }
}
