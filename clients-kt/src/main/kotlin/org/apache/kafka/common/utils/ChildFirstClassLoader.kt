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

import java.io.File
import java.io.IOException
import java.net.URL
import java.net.URLClassLoader
import java.util.Enumeration
import org.apache.kafka.common.KafkaException

/**
 * A class loader that looks for classes and resources in a specified class path first, before delegating to its parent
 * class loader.
 *
 * @param classPath Class path string
 * @param parent The parent classloader. If the required class / resource cannot be found in the given classPath,
 * this classloader will be used to find the class / resource.
 */
class ChildFirstClassLoader(
    classPath: String,
    parent: ClassLoader?,
) : URLClassLoader(classpathToURLs(classPath), parent) {

    @Throws(ClassNotFoundException::class)
    override fun loadClass(name: String, resolve: Boolean): Class<*>? {
        synchronized(getClassLoadingLock(name)) {
            var c = findLoadedClass(name)
            if (c == null) {
                c = try {
                    findClass(name)
                } catch (e: ClassNotFoundException) {
                    // Try parent
                    super.loadClass(name, false)
                }
            }
            if (resolve) resolveClass(c)
            return c
        }
    }

    override fun getResource(name: String): URL? {
        return findResource(name) ?: super.getResource(name)
    }

    @Throws(IOException::class)
    override fun getResources(name: String): Enumeration<URL> {
        val urls1: Enumeration<URL>? = findResources(name)
        val urls2: Enumeration<URL>? = getParent()?.getResources(name)
        return object : Enumeration<URL> {
            override fun hasMoreElements(): Boolean {
                return urls1 != null && urls1.hasMoreElements() || urls2 != null && urls2.hasMoreElements()
            }

            override fun nextElement(): URL {
                if (urls1 != null && urls1.hasMoreElements()) return urls1.nextElement()
                if (urls2 != null && urls2.hasMoreElements()) return urls2.nextElement()
                throw NoSuchElementException()
            }
        }
    }

    companion object {

        init {
            registerAsParallelCapable()
        }

        private fun classpathToURLs(classPath: String): Array<URL> {
            val urls = ArrayList<URL>()
            for (path in classPath.split(File.pathSeparator.toRegex())) {
                if (path == null || path.trim { it <= ' ' }.isEmpty()) continue
                val file = File(path)
                try {
                    if (path.endsWith("/*")) {
                        val parent = File(File(file.getCanonicalPath()).getParent())
                        if (parent.isDirectory()) {
                            val files = parent.listFiles { _, name ->
                                val lower = name.lowercase()
                                lower.endsWith(".jar") || lower.endsWith(".zip")
                            }
                            if (files != null) {
                                for (jarFile in files) {
                                    urls.add(jarFile.getCanonicalFile().toURI().toURL())
                                }
                            }
                        }
                    } else if (file.exists()) {
                        urls.add(file.getCanonicalFile().toURI().toURL())
                    }
                } catch (e: IOException) {
                    throw KafkaException(e)
                }
            }
            return urls.toTypedArray<URL>()
        }
    }
}
