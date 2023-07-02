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
import java.io.Reader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import org.apache.kafka.common.config.ConfigData
import org.apache.kafka.common.config.ConfigException
import org.slf4j.LoggerFactory

/**
 * An implementation of [ConfigProvider] that represents a Properties file.
 * All property keys and values are stored as cleartext.
 */
open class FileConfigProvider : ConfigProvider {

    override fun configure(configs: Map<String, *>) = Unit

    /**
     * Retrieves the data at the given Properties file.
     *
     * @param path the file where the data resides
     * @return the configuration data
     */
    override fun get(path: String?): ConfigData {
        val data: MutableMap<String, String> = HashMap()
        if (path.isNullOrEmpty()) return ConfigData(data)

        try {
            reader(path).use { reader ->
                val properties = Properties()
                properties.load(reader)

                return ConfigData(
                    properties.map { (key, value) -> key.toString() to value.toString() }
                        .toMap()
                )
            }
        } catch (e: IOException) {
            log.error("Could not read properties from file {}", path, e)
            throw ConfigException("Could not read properties from file $path")
        }
    }

    /**
     * Retrieves the data with the given keys at the given Properties file.
     *
     * @param path the file where the data resides
     * @param keys the keys whose values will be retrieved
     * @return the configuration data
     */
    override fun get(path: String?, keys: Set<String>): ConfigData {
        if (path.isNullOrEmpty()) return ConfigData(emptyMap())

        try {
            reader(path).use { reader ->
                val properties = Properties()
                properties.load(reader)
                return ConfigData(
                    keys.associateBy(
                        keySelector = { it },
                        valueTransform = { key -> properties.getProperty(key) }
                    ).filter { (_, value) -> value != null }
                )
            }
        } catch (e: IOException) {
            log.error("Could not read properties from file {}", path, e)
            throw ConfigException("Could not read properties from file $path")
        }
    }

    // visible for testing
    @Throws(IOException::class)
    protected open fun reader(path: String): Reader {
        return Files.newBufferedReader(Paths.get(path))
    }

    override fun close() = Unit

    companion object {
        private val log = LoggerFactory.getLogger(FileConfigProvider::class.java)
    }
}
