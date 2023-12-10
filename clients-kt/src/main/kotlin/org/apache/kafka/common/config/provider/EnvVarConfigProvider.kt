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

package org.apache.kafka.common.config.provider

import java.io.IOException
import java.util.regex.Pattern
import org.apache.kafka.common.config.ConfigData
import org.apache.kafka.common.config.ConfigException
import org.slf4j.LoggerFactory

/**
 * An implementation of [ConfigProvider] based on environment variables.
 * Keys correspond to the names of the environment variables, paths are currently not being used.
 * Using an allowlist pattern [EnvVarConfigProvider.ALLOWLIST_PATTERN_CONFIG] that supports regular expressions,
 * it is possible to limit access to specific environment variables. Default allowlist pattern is ".*".
 */
class EnvVarConfigProvider : ConfigProvider {

    private val envVarMap: Map<String, String>

    private var filteredEnvVarMap: Map<String, String>? = null

    constructor() {
        envVarMap = getEnvVars()
    }

    constructor(envVarsAsArgument: Map<String, String>) {
        envVarMap = envVarsAsArgument
    }

    override fun configure(configs: Map<String, Any?>) {
        val envVarPattern: Pattern
        if (configs.containsKey(ALLOWLIST_PATTERN_CONFIG)) {
            envVarPattern = Pattern.compile(configs[ALLOWLIST_PATTERN_CONFIG].toString())
        } else {
            envVarPattern = Pattern.compile(".*")
            log.info("No pattern for environment variables provided. Using default pattern '(.*)'.")
        }
        filteredEnvVarMap = envVarMap.filter { envVar -> envVarPattern.matcher(envVar.key).matches() }
    }

    @Throws(IOException::class)
    override fun close() = Unit

    /**
     * @param path unused
     * @return returns environment variables as configuration
     */
    override fun get(path: String?): ConfigData = get(path, emptySet())

    /**
     * @param path    path, not used for environment variables
     * @param keys the keys whose values will be retrieved.
     * @return the configuration data.
     */
    override fun get(path: String?, keys: Set<String>): ConfigData {
        if (!path.isNullOrEmpty()) {
            log.error("Path is not supported for EnvVarConfigProvider, invalid value '{}'", path)
            throw ConfigException("Path is not supported for EnvVarConfigProvider, invalid value '$path'")
        }
        val filteredEnvVarMap = filteredEnvVarMap!!
        if (keys.isEmpty()) return ConfigData(filteredEnvVarMap)

        val filteredData = filteredEnvVarMap.filterKeys { it in keys }
        return ConfigData(filteredData)
    }

    private fun getEnvVars(): Map<String, String> = try {
        System.getenv()
    } catch (e: Exception) {
        log.error("Could not read environment variables", e)
        throw ConfigException("Could not read environment variables")
    }

    companion object {

        private val log = LoggerFactory.getLogger(EnvVarConfigProvider::class.java)

        const val ALLOWLIST_PATTERN_CONFIG = "allowlist.pattern"

        const val ALLOWLIST_PATTERN_CONFIG_DOC =
            "A pattern / regular expression that needs to match for environment variables" +
                    " to be used by this config provider."
    }
}
