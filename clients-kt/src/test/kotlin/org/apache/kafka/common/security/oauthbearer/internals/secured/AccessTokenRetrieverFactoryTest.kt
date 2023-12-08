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
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetrieverFactory.create
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AccessTokenRetrieverFactoryTest : OAuthBearerTest() {
    
    @Test
    @Throws(Exception::class)
    fun testConfigureRefreshingFileAccessTokenRetriever() {
        val expected = "{}"
        val tmpDir = createTempDir("access-token")
        val accessTokenFile = createTempFile(
            tmpDir = tmpDir,
            prefix = "access-token-",
            suffix = ".json",
            contents = expected,
        )
        val configs = mapOf(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL to accessTokenFile.toURI().toString())
        val jaasConfig = emptyMap<String, Any>()

        create(configs = configs, jaasConfig = jaasConfig).use { accessTokenRetriever ->
            accessTokenRetriever.init()
            assertEquals(expected, accessTokenRetriever.retrieve())
        }
    }

    @Test
    fun testConfigureRefreshingFileAccessTokenRetrieverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        val configs = getSaslConfigs(
            SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString()
        )
        val jaasConfig = emptyMap<String, Any>()
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { create(configs = configs, jaasConfig = jaasConfig) },
            substring = "that doesn't exist",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testConfigureRefreshingFileAccessTokenRetrieverWithInvalidFile() {
        // Should fail because the while the parent path exists, the file itself doesn't.
        val tmpDir = createTempDir(directory = "this-directory-does-exist")
        val accessTokenFile = File(tmpDir, "this-file-does-not-exist.json")
        val configs = getSaslConfigs(
            name = SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            value = accessTokenFile.toURI().toString(),
        )
        val jaasConfig = emptyMap<String, Any>()
        assertThrowsWithMessage(
            clazz = ConfigException::class.java,
            executable = { create(configs = configs, jaasConfig = jaasConfig) },
            substring = "that doesn't exist",
        )
    }
}
