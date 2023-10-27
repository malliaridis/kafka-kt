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

import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.kafka.common.config.provider.ConfigProvider

/**
 * This class wraps a set of [ConfigProvider] instances and uses them to perform transformations.
 *
 * The default variable pattern is of the form `${provider:[path:]key}`, where the `provider`
 * corresponds to a [ConfigProvider] instance, as passed to [ConfigTransformer.ConfigTransformer].
 * The pattern will extract a set of paths (which are optional) and keys and then pass them to
 * [ConfigProvider.get] to obtain the values with which to replace the variables.
 *
 * For example, if a Map consisting of an entry with a provider name "file" and provider instance
 * [FileConfigProvider] is passed to the [ConfigTransformer.ConfigTransformer], and a Properties
 * file with contents
 *
 * ```plaintext
 * fileKey=someValue
 * ```
 * 
 * resides at the path "/tmp/properties.txt", then when a configuration Map which has an entry with
 * a key "someKey" and a value "${file:/tmp/properties.txt:fileKey}" is passed to the [transform]
 * method, then the transformed Map will have an entry with key "someKey" and a value "someValue".
 *
 * This class only depends on [ConfigProvider.get] and does not depend on subscription support in a
 * [ConfigProvider], such as the [ConfigProvider.subscribe] and [ConfigProvider.unsubscribe]
 * methods.
 *
 * @constructor Creates a ConfigTransformer with the default pattern, of the form
 * `${provider:[path:]key}`.
 * @property configProviders a Map of provider names and [ConfigProvider] instances.
 */
class ConfigTransformer(private val configProviders: Map<String, ConfigProvider>) {

    /**
     * Transforms the given configuration data by using the [ConfigProvider] instances to look up
     * values to replace the variables in the pattern.
     *
     * @param configs the configuration values to be transformed
     * @return an instance of [ConfigTransformerResult]
     */
    fun transform(configs: Map<String, String>): ConfigTransformerResult {
        val keysByProvider = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()
        val lookupsByProvider = mutableMapOf<String, MutableMap<String, Map<String, String>>>()

        // Collect the variables from the given configs that need transformation
        configs.values.forEach { value ->
            getVars(value, DEFAULT_PATTERN).forEach { configVar ->
                val keysByPath = keysByProvider.computeIfAbsent(configVar.providerName) { HashMap() }
                val keys = keysByPath.computeIfAbsent(configVar.path) { HashSet() }
                keys.add(configVar.variable)
            }
        }

        // Retrieve requested variables from the ConfigProviders
        val ttls: MutableMap<String, Long> = HashMap()
        keysByProvider.forEach { (providerName, keysByPath) ->
            val provider = configProviders[providerName] ?: return@forEach

            keysByPath.forEach { (path, value) ->
                val keys: Set<String> = HashSet(value)
                val (data, ttl) = provider[path, keys]
                if (ttl != null && ttl >= 0) ttls[path] = ttl

                val keyValuesByPath = lookupsByProvider.computeIfAbsent(providerName) { HashMap() }
                keyValuesByPath[path] = data
            }
        }

        // Perform the transformations by performing variable replacements
        val data: MutableMap<String, String> = HashMap(configs)
        configs.forEach { (key, value) ->
            data[key] = replace(lookupsByProvider, value, DEFAULT_PATTERN)
        }

        return ConfigTransformerResult(data, ttls)
    }

    private class ConfigVariable(matcher: Matcher) {

        val providerName: String

        val path: String

        val variable: String

        init {
            providerName = matcher.group(1)
            path = if (matcher.group(3) != null) matcher.group(3) else EMPTY_PATH
            variable = matcher.group(4)
        }

        override fun toString(): String = "($providerName:$path:$variable)"
    }

    companion object {

        val DEFAULT_PATTERN = Pattern.compile("\\$\\{([^}]*?):(([^}]*?):)?([^}]*?)\\}")

        private const val EMPTY_PATH = ""

        private fun getVars(value: String, pattern: Pattern): List<ConfigVariable> {
            val configVars: MutableList<ConfigVariable> = ArrayList()
            val matcher = pattern.matcher(value)

            while (matcher.find()) configVars.add(ConfigVariable(matcher))

            return configVars
        }

        private fun replace(
            lookupsByProvider: Map<String, MutableMap<String, Map<String, String>>>,
            value: String,
            pattern: Pattern
        ): String {
            val matcher = pattern.matcher(value)
            val builder = StringBuilder()
            var i = 0

            while (matcher.find()) {
                val configVar = ConfigVariable(matcher)
                val lookupsByPath: Map<String, Map<String, String>>? =
                    lookupsByProvider[configVar.providerName]

                if (lookupsByPath != null) {
                    val keyValues = lookupsByPath[configVar.path]!!
                    val replacement = keyValues[configVar.variable]
                    builder.append(value, i, matcher.start())

                    // No replacements will be performed; just return the original value if null
                    if (replacement == null) builder.append(matcher.group(0))
                    else builder.append(replacement)

                    i = matcher.end()
                }
            }

            builder.append(value, i, value.length)
            return builder.toString()
        }
    }
}
