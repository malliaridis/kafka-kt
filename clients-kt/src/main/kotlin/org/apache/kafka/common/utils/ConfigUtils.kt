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

package org.apache.kafka.common.utils

import java.util.stream.Collectors
import org.apache.kafka.common.config.ConfigDef
import org.slf4j.LoggerFactory

object ConfigUtils {

    private val log = LoggerFactory.getLogger(ConfigUtils::class.java)

    /**
     * Translates deprecated configurations into their non-deprecated equivalents
     *
     * This is a convenience method for [ConfigUtils.translateDeprecatedConfigs] until we can use
     * Java 9+ `Map.of(..)` and `Set.of(...)`
     *
     * @param configs the input configuration
     * @param aliasGroups An array of arrays of synonyms. Each synonym array begins with the
     * non-deprecated synonym. For example, `new String[][] { { a, b }, { c, d, e} }`
     * would declare b as a deprecated synonym for a, and d and e as deprecated synonyms for c. The
     * ordering of synonyms determines the order of precedence (e.g. the first synonym takes
     * precedence over the second one)
     * @return a new configuration map with deprecated  keys translated to their non-deprecated
     * equivalents
     */
    fun <T> translateDeprecatedConfigs(
        configs: Map<String, T>,
        aliasGroups: Array<Array<String>>,
    ): Map<String, T?> {
        return translateDeprecatedConfigs(
            configs = configs,
            aliasGroups = aliasGroups.associate { it[0] to it.drop(1) },
        )
    }

    /**
     * Translates deprecated configurations into their non-deprecated equivalents
     *
     * @param configs the input configuration
     * @param aliasGroups A map of config to synonyms. Each key is the non-deprecated synonym
     * For example, `Map.of(a , Set.of(b), c, Set.of(d, e))` would declare b as a deprecated synonym
     * for a, and d and e as deprecated synonyms for c. The ordering of synonyms determines the
     * order of precedence (e.g. the first synonym takes precedence over the second one).
     *
     * @return a new configuration map with deprecated keys translated to their non-deprecated
     * equivalents
     */
    fun <T> translateDeprecatedConfigs(
        configs: Map<String, T>,
        aliasGroups: Map<String, List<String>>,
    ): Map<String, T?> {
        val aliasSet = aliasGroups.keys + aliasGroups.values.flatten()

        // pass through all configurations without aliases
        val newConfigs: MutableMap<String, T?> = configs.filter { !aliasSet.contains(it.key) }
            .mapNotNull { (key, value) -> value?.let { key to it } }
            .associateBy(
                keySelector = Pair<String, T>::first,
                valueTransform = Pair<String, T>::second,
            )
            .toMutableMap()

        aliasGroups.forEach { (target, aliases) ->
            val deprecated = aliases.filter { configs.containsKey(it) }

            if (deprecated.isEmpty()) {
                // No deprecated key(s) found.
                if (configs.containsKey(target)) newConfigs[target] = configs[target]
                return@forEach
            }

            val aliasString = deprecated.joinToString(", ")

            if (configs.contains(target)) {
                // Ignore the deprecated key(s) because the actual key was set.
                log.error(
                    "$target was configured, as well as the deprecated alias(es) $aliasString. " +
                            "Using the value of $target"
                )
                newConfigs[target] = configs[target]
            }
            else if (deprecated.size > 1) {
                log.error(
                    "The configuration keys $aliasString are deprecated and may be removed in " +
                            "the future. Additionally, this configuration is ambiguous because " +
                            "these configuration keys are all aliases for $target. Please update " +
                            "your configuration to have only $target set."
                )
                newConfigs[target] = configs[deprecated[0]]
            } else {
                log.warn(
                    "Configuration key ${deprecated[0]} is deprecated and may be removed in " +
                            "the future. Please update your configuration to use $target instead."
                )
                newConfigs[target] = configs[deprecated[0]]
            }
        }

        return newConfigs
    }

    fun configMapToRedactedString(map: Map<String, Any?>, configDef: ConfigDef): String {
        val bld = StringBuilder("{")
        val keys: List<String> = ArrayList(map.keys).sorted()

        var prefix = ""
        keys.forEach { key ->
            bld.append(prefix).append(key).append("=")
            val configKey = configDef.configKeys[key]

            if (configKey == null || configKey.type.isSensitive) bld.append("(redacted)")
            else {
                val value = map[key]
                if (value == null) bld.append("null")
                else if (configKey.type === ConfigDef.Type.STRING)
                    bld.append("\"").append(value).append("\"")
                else bld.append(value)
            }

            prefix = ", "
        }
        bld.append("}")
        return bld.toString()
    }
}
