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

import org.apache.kafka.common.utils.Utils.delete
import org.apache.kafka.test.TestUtils.tempDirectory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.*
import java.util.stream.StreamSupport
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DirectoryConfigProviderTest {
    
    private var provider: DirectoryConfigProvider? = null
    
    private var parent: File? = null
    
    private var dir: File? = null
    
    private var bar: File? = null
    
    private var foo: File? = null
    
    private var subdir: File? = null
    
    private var subdirFile: File? = null
    
    private var siblingDir: File? = null
    
    private var siblingDirFile: File? = null
    
    private var siblingFile: File? = null
    
    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
        provider = DirectoryConfigProvider()
        provider!!.configure(emptyMap<String, Any>())
        parent = tempDirectory()
        dir = File(parent, "dir")
        dir!!.mkdir()
        foo = writeFile(File(dir, "foo"))
        bar = writeFile(File(dir, "bar"))
        subdir = File(dir, "subdir")
        subdir!!.mkdir()
        subdirFile = writeFile(File(subdir, "subdirFile"))
        siblingDir = File(parent, "siblingdir")
        siblingDir!!.mkdir()
        siblingDirFile = writeFile(File(siblingDir, "siblingdirFile"))
        siblingFile = writeFile(File(parent, "siblingFile"))
    }

    @AfterEach
    @Throws(IOException::class)
    fun close() {
        provider!!.close()
        delete(parent)
    }

    @Test
    @Throws(IOException::class)
    fun testGetAllKeysAtPath() {
        val (data, ttl) = provider!![dir!!.absolutePath]
        assertEquals(setOf(foo!!.getName(), bar!!.getName()), data.keys)
        assertEquals("FOO", data[foo!!.getName()])
        assertEquals("BAR", data[bar!!.getName()])
        assertNull(ttl)
    }

    @Test
    fun testGetSetOfKeysAtPath() {
        val keys: Set<String> = setOf(foo!!.getName(), "baz")
        val (data, ttl) = provider!![dir!!.absolutePath, keys]
        assertEquals(setOf(foo!!.getName()), data.keys)
        assertEquals("FOO", data[foo!!.getName()])
        assertNull(ttl)
    }

    @Test
    fun testNoSubdirs() {
        // Only regular files directly in the path directory are allowed, not in subdirs
        val keys: Set<String> = setOf(
            subdir!!.getName(),
            listOf(subdir!!.getName(), subdirFile!!.getName()).joinToString(File.separator)
        )
        val (data, ttl) = provider!![dir!!.absolutePath, keys]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testNoTraversal() {
        // Check we can't escape outside the path directory
        val keys: Set<String> = setOf(
            listOf("..", siblingFile!!.getName()).joinToString(File.separator),
            listOf("..", siblingDir!!.getName()).joinToString(File.separator),
            listOf("..", siblingDir!!.getName(), siblingDirFile!!.getName()).joinToString(File.separator)
        )
        val (data, ttl) = provider!![dir!!.absolutePath, keys]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testEmptyPath() {
        val (data, ttl) = provider!![""]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testEmptyPathWithKey() {
        val (data, ttl) = provider!!["", setOf("foo")]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testNullPath() {
        val (data, ttl) = provider!![null]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testNullPathWithKey() {
        val (data, ttl) = provider!![null, setOf("foo")]
        assertTrue(data.isEmpty())
        assertNull(ttl)
    }

    @Test
    fun testServiceLoaderDiscovery() {
        val serviceLoader = ServiceLoader.load(ConfigProvider::class.java)
        assertTrue(
            StreamSupport.stream(serviceLoader.spliterator(), false)
                .anyMatch { configProvider -> configProvider is DirectoryConfigProvider }
        )
    }

    companion object {

        @Throws(IOException::class)
        private fun writeFile(file: File): File {
            Files.write(file.toPath(), file.getName().uppercase().toByteArray(StandardCharsets.UTF_8))
            return file
        }
    }
}
