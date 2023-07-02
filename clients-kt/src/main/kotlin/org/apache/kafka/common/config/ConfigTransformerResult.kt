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

package org.apache.kafka.common.config

import org.apache.kafka.common.config.provider.ConfigProvider

/**
 * The result of a transformation from [ConfigTransformer].
 *
 * @constructor Creates a new ConfigTransformerResult with the given data and TTL values for a set
 * of paths.
 * @property data the transformed data, with variables replaced with corresponding values from the
 * ConfigProvider instances if found.
 * @property ttls the TTL values (in milliseconds) returned from the ConfigProvider instances for a
 * given set of paths.
 */
data class ConfigTransformerResult(
    val data: Map<String, String>,
    val ttls: Map<String, Long>,
) {

    /**
     * Returns the transformed data, with variables replaced with corresponding values from the
     * ConfigProvider instances if found.
     *
     * Modifying the transformed data that is returned does not affect the [ConfigProvider] nor the
     * original data that was used as the source of the transformation.
     *
     * @return data a Map of key-value pairs
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("data")
    )
    fun data(): Map<String, String> = data

    /**
     * Returns the TTL values (in milliseconds) returned from the ConfigProvider instances for a
     * given set of paths.
     *
     * @return data a Map of path and TTL values
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("ttls")
    )
    fun ttls(): Map<String, Long> = ttls
}
