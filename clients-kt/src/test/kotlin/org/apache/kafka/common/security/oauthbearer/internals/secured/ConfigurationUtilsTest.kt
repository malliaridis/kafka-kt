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

package org.apache.kafka.common.security.oauthbearer.internals.secured

import java.io.File
import java.io.IOException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.Test

class ConfigurationUtilsTest : OAuthBearerTest() {

    @Test
    fun testUrl() {
        testUrl("http://www.example.com")
    }

    @Test
    fun testUrlWithSuperfluousWhitespace() {
        testUrl("  http://www.example.com  ")
    }

    @Test
    fun testUrlCaseInsensitivity() {
        testUrl("HTTPS://WWW.EXAMPLE.COM")
    }

    @Test
    fun testUrlFile() {
        testUrl("file:///tmp/foo.txt")
    }

    @Test
    fun testUrlFullPath() {
        testUrl("https://myidp.example.com/oauth2/default/v1/token")
    }

    @Test
    fun testUrlMissingProtocol() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testUrl("www.example.com") },
            substring = "no protocol",
        )
    }

    @Test
    fun testUrlInvalidProtocol() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testUrl("ftp://ftp.example.com") },
            substring = "invalid protocol",
        )
    }

    @Test
    fun testUrlNull() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testUrl(null) },
            substring = "must be non-null",
        )
    }

    @Test
    fun testUrlEmptyString() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testUrl("") },
            substring = "must not contain only whitespace",
        )
    }

    @Test
    fun testUrlWhitespace() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testUrl("    ") },
            substring = "must not contain only whitespace",
        )
    }

    private fun testUrl(value: String?) {
        val configs = mapOf(URL_CONFIG_NAME to value)
        val cu = ConfigurationUtils(configs)
        cu.validateUrl(URL_CONFIG_NAME)
    }

    @Test
    @Throws(IOException::class)
    fun testFile() {
        val file = tempFile("some contents!")
        testFile(file.toURI().toURL().toString())
    }

    @Test
    @Throws(IOException::class)
    fun testFileWithSuperfluousWhitespace() {
        val file = tempFile()
        testFile("  ${file.toURI().toURL()}  ")
    }

    @Test
    fun testFileDoesNotExist() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testFile(File("/tmp/not/a/real/file.txt").toURI().toURL().toString()) },
            substring = "that doesn't exist",
        )
    }

    @Test
    @Throws(IOException::class)
    fun testFileUnreadable() {
        val file = tempFile()
        check(file.setReadable(false)) {
            "Can't test file permissions as test couldn't programmatically " +
                    "make temp file ${file.absolutePath} un-readable"
        }
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testFile(file.toURI().toURL().toString()) },
            substring = "that doesn't have read permission",
        )
    }

    @Test
    fun testFileNull() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testFile(null) },
            substring = "must be non-null",
        )
    }

    @Test
    fun testFileEmptyString() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testFile("") },
            substring = "must not contain only whitespace",
        )
    }

    @Test
    fun testFileWhitespace() {
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { testFile("    ") },
            substring = "must not contain only whitespace",
        )
    }

    fun testFile(value: String?) {
        val configs = mapOf(URL_CONFIG_NAME to value)
        val cu = ConfigurationUtils(configs)
        cu.validateFile(URL_CONFIG_NAME)
    }

    companion object {
        private const val URL_CONFIG_NAME = "url"
    }
}
