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

import java.util.*
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * A configuration object containing the configuration entries for a resource.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
data class Config(
    val entries: Map<String, ConfigEntry>,
) {

    constructor(
        entries: Collection<ConfigEntry>,
    ) : this(
        entries = entries.associateBy { entry -> entry.name }
    )

    /**
     * Configuration entries for a resource.
     */
    fun entries(): Collection<ConfigEntry> {
        return Collections.unmodifiableCollection(entries.values)
    }

    /**
     * Get the configuration entry with the provided name or null if there isn't one.
     */
    operator fun get(name: String): ConfigEntry? {
        return entries[name]
    }

    override fun toString(): String {
        return "Config(entries=" + entries.values + ")"
    }
}
