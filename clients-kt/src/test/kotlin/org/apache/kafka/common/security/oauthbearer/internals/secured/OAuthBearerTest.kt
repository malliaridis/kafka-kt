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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.util.Base64
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
import org.apache.kafka.common.utils.Utils.utf8
import org.jose4j.jwk.PublicJsonWebKey
import org.jose4j.jwk.RsaJwkGenerator
import org.jose4j.lang.JoseException
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.test.assertTrue
import kotlin.test.fail

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class OAuthBearerTest {

    protected val log: Logger = LoggerFactory.getLogger(javaClass)

    protected var mapper = ObjectMapper()
    
    protected fun assertThrowsWithMessage(
        clazz: Class<out Exception?>,
        executable: Executable,
        substring: String,
    ) {
        var failed = false
        try {
            executable.execute()
        } catch (t: Throwable) {
            failed = true
            assertTrue(
                actual = clazz.isInstance(t),
                message = "Test failed by exception ${t.javaClass}, but expected $clazz",
            )
            assertErrorMessageContains(t.message!!, substring)
        }
        if (!failed) fail("Expected test to fail with $clazz that contains the string $substring")
    }

    protected fun assertErrorMessageContains(actual: String, expectedSubstring: String) {
        assertTrue(
            actual = actual.contains(expectedSubstring),
            message = """Expected exception message ("$actual") to contain substring ("$expectedSubstring")""",
        )
    }

    protected fun configureHandler(
        handler: AuthenticateCallbackHandler,
        configs: Map<String, *>,
        jaasConfig: Map<String, Any?>?,
    ) {
        val config = TestJaasConfig()
        config.createOrUpdateEntry(
            name = "KafkaClient",
            loginModule = OAuthBearerLoginModule::class.java.getName(),
            options = jaasConfig,
        )
        val kafkaClient = config.getAppConfigurationEntry("KafkaClient")!![0]!!
        handler.configure(
            configs = configs,
            saslMechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            jaasConfigEntries = listOf(kafkaClient),
        )
    }

    protected fun createBase64JsonJwtSection(block: (ObjectNode) -> Unit): String {
        val json = createJsonJwtSection(block)
        return try {
            utf8(Base64.getEncoder().encode(json.toByteArray()))
        } catch (t: Throwable) {
            fail(t.message)
        }
    }

    protected fun createJsonJwtSection(block: (ObjectNode) -> Unit): String {
        val node = mapper.createObjectNode()
        block(node)
        return try {
            mapper.writeValueAsString(node)
        } catch (t: Throwable) {
            fail(t.message)
        }
    }

    protected fun createRetryable(attempts: Array<Exception?>): Retryable<String> {
        val i = attempts.iterator()
        return Retryable {
            val e = if (i.hasNext()) i.next() else null
            if (e == null) return@Retryable "success!"
            else {
                when (e) {
                    is IOException -> throw ExecutionException(e)
                    is RuntimeException -> throw e
                    else -> throw RuntimeException(e)
                }
            }
        }
    }

    @Throws(IOException::class)
    protected fun createHttpURLConnection(response: String): HttpURLConnection {
        val mockedCon = Mockito.mock(HttpURLConnection::class.java)
        Mockito.`when`(mockedCon.url).thenReturn(URL("https://www.example.com"))
        Mockito.`when`(mockedCon.getResponseCode()).thenReturn(200)
        Mockito.`when`(mockedCon.outputStream).thenReturn(ByteArrayOutputStream())
        Mockito.`when`(mockedCon.inputStream).thenReturn(
            ByteArrayInputStream(response.toByteArray())
        )
        return mockedCon
    }

    @Throws(IOException::class)
    protected fun createTempDir(directory: String?): File {
        var tmpDir = File(System.getProperty("java.io.tmpdir"))
        if (directory != null) tmpDir = File(tmpDir, directory)
        if (!tmpDir.exists() && !tmpDir.mkdirs()) throw IOException("Could not create $tmpDir")
        tmpDir.deleteOnExit()
        log.debug("Created temp directory {}", tmpDir)
        return tmpDir
    }

    @Throws(IOException::class)
    protected fun createTempFile(
        tmpDir: File?,
        prefix: String?,
        suffix: String?,
        contents: String?,
    ): File {
        val file = File.createTempFile(prefix, suffix, tmpDir)
        log.debug("Created new temp file {}", file)
        file.deleteOnExit()
        FileWriter(file).use { writer -> writer.write(contents) }
        return file
    }

    protected fun getSaslConfigs(configs: Map<String, *>): Map<String, *> {
        val configDef = ConfigDef()
        configDef.withClientSaslSupport()
        val sslClientConfig = AbstractConfig(configDef, configs)
        return sslClientConfig.values()
    }

    protected fun getSaslConfigs(name: String, value: Any): Map<String, *> {
        return getSaslConfigs(mapOf(name to value))
    }

    protected val saslConfigs: Map<String, *>
        get() = getSaslConfigs(emptyMap<String, Any>())

    @Throws(JoseException::class)
    protected fun createRsaJwk(): PublicJsonWebKey {
        val jwk = RsaJwkGenerator.generateJwk(2048)
        jwk.keyId = "key-1"
        return jwk
    }

    @Throws(JoseException::class)
    protected fun createEcJwk(): PublicJsonWebKey {
        val jwk = PublicJsonWebKey.Factory.newPublicJwk(
            "{" +
                    "  \"kty\": \"EC\"," +
                    "  \"d\": \"Tk7qzHNnSBMioAU7NwZ9JugFWmWbUCyzeBRjVcTp_so\"," +
                    "  \"use\": \"sig\"," +
                    "  \"crv\": \"P-256\"," +
                    "  \"kid\": \"key-1\"," +
                    "  \"x\": \"qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I\"," +
                    "  \"y\": \"wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc\"" +
                    "}"
        )
        jwk.keyId = "key-1"
        return jwk
    }
}