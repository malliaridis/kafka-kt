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

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors
import org.apache.kafka.common.config.ConfigData
import org.apache.kafka.common.config.ConfigException
import org.slf4j.LoggerFactory

/**
 * An implementation of [ConfigProvider] based on a directory of files.
 * Property keys correspond to the names of the regular (i.e. non-directory)
 * files in a directory given by the path parameter.
 * Property values are taken from the file contents corresponding to each key.
 */
class DirectoryConfigProvider : ConfigProvider {

    override fun configure(configs: Map<String, *>) = Unit

    @Throws(IOException::class)
    override fun close() = Unit

    /**
     * Retrieves the data contained in regular files in the directory given by `path`.
     * Non-regular files (such as directories) in the given directory are silently ignored.
     * @param path the directory where data files reside.
     * @return the configuration data.
     */
    override fun get(path: String?): ConfigData {
        return Companion[path, { Files.isRegularFile(it) }]
    }

    /**
     * Retrieves the data contained in the regular files named by [keys] in the directory given by
     * [path]. Non-regular files (such as directories) in the given directory are silently ignored.
     *
     * @param path the directory where data files reside.
     * @param keys the keys whose values will be retrieved.
     * @return the configuration data.
     */
    override fun get(path: String?, keys: Set<String>): ConfigData {
        return Companion[path, { pathName: Path ->
            (Files.isRegularFile(pathName) && keys.contains(pathName.fileName.toString()))
        }]
    }

    companion object {

        private val log = LoggerFactory.getLogger(DirectoryConfigProvider::class.java)

        private operator fun get(path: String?, fileFilter: Predicate<Path>): ConfigData {
            if (path.isNullOrEmpty()) return ConfigData(emptyMap())

            var map = emptyMap<String, String>()

            val dir = File(path).toPath()
            if (!Files.isDirectory(dir)) log.warn("The path {} is not a directory", path)
            else {
                try {
                    Files.list(dir).use { stream ->
                        map = stream
                            .filter(fileFilter)
                            .collect(
                                Collectors.toMap(
                                    { it.fileName.toString() },
                                    { p: Path -> read(p) })
                            )
                    }
                } catch (e: IOException) {
                    log.error("Could not list directory {}", dir, e)
                    throw ConfigException("Could not list directory $dir")
                }
            }
            return ConfigData(map)
        }

        private fun read(path: Path): String {
            return try {
                String(Files.readAllBytes(path), StandardCharsets.UTF_8)
            } catch (e: IOException) {
                log.error("Could not read file {} for property {}", path, path.fileName, e)
                throw ConfigException("Could not read file $path for property ${path.fileName}")
            }
        }
    }
}
