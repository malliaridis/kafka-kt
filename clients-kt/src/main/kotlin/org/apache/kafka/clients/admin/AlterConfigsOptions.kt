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
 * Options for [Admin.incrementalAlterConfigs] and [Admin.alterConfigs].
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class AlterConfigsOptions : AbstractOptions<AlterConfigsOptions>() {
    var validateOnly = false

    /**
     * Set the timeout in milliseconds for this operation or `null` if the default api timeout for the
     * AdminClient should be used.
     */
    @Deprecated(message = "Use property instead.")
    // This method is retained to keep binary compatibility with 0.11
    override fun timeoutMs(timeoutMs: Int): AlterConfigsOptions {
        this.timeoutMs = timeoutMs
        return this
    }

    /**
     * Return true if the request should be validated without altering the configs.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("validateOnly"),
    )
    fun shouldValidateOnly(): Boolean {
        return validateOnly
    }

    /**
     * Set to true if the request should be validated without altering the configs.
     */
    @Deprecated(message = "Use property instead.")
    fun validateOnly(validateOnly: Boolean): AlterConfigsOptions {
        this.validateOnly = validateOnly
        return this
    }
}
