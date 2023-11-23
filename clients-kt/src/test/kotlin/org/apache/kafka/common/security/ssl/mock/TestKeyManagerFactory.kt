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

package org.apache.kafka.common.security.ssl.mock

import java.io.IOException
import java.net.Socket
import java.security.GeneralSecurityException
import java.security.KeyPair
import java.security.KeyStore
import java.security.Principal
import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactorySpi
import javax.net.ssl.ManagerFactoryParameters
import javax.net.ssl.X509ExtendedKeyManager
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.test.TestSslUtils
import org.apache.kafka.test.TestSslUtils.CertificateBuilder
import org.apache.kafka.test.TestSslUtils.createTrustStore
import org.apache.kafka.test.TestSslUtils.generateKeyPair
import org.apache.kafka.test.TestUtils.tempFile

class TestKeyManagerFactory : KeyManagerFactorySpi() {

    override fun engineInit(keyStore: KeyStore, chars: CharArray) = Unit

    override fun engineInit(managerFactoryParameters: ManagerFactoryParameters) = Unit

    override fun engineGetKeyManagers(): Array<KeyManager> = arrayOf(TestKeyManager())

    class TestKeyManager : X509ExtendedKeyManager() {

        private val keyPair: KeyPair

        private val certificate: X509Certificate

        init {
            try {
                keyPair = generateKeyPair(SIGNATURE_ALGORITHM)
                val certBuilder = CertificateBuilder()
                certificate = certBuilder.generate("CN=$CN, O=A server", keyPair)
                val certificates = mapOf(ALIAS to certificate)
                val trustStoreFile = tempFile("testTrustStore", ".jks")
                mockTrustStoreFile = trustStoreFile.path
                createTrustStore(
                    filename = mockTrustStoreFile,
                    password = Password(TestSslUtils.TRUST_STORE_PASSWORD),
                    certs = certificates,
                )
            } catch (e: IOException) {
                throw RuntimeException(e)
            } catch (e: GeneralSecurityException) {
                throw RuntimeException(e)
            }
        }

        override fun getClientAliases(
            s: String,
            principals: Array<Principal>,
        ): Array<String> = arrayOf(ALIAS)

        override fun chooseClientAlias(
            strings: Array<String>,
            principals: Array<Principal>,
            socket: Socket,
        ): String = ALIAS

        override fun getServerAliases(
            s: String,
            principals: Array<Principal>,
        ): Array<String> = arrayOf(ALIAS)

        override fun chooseServerAlias(
            s: String,
            principals: Array<Principal>,
            socket: Socket,
        ): String = ALIAS

        override fun getCertificateChain(s: String): Array<X509Certificate> = arrayOf(certificate)

        override fun getPrivateKey(s: String): PrivateKey = keyPair.private

        companion object {

            lateinit var mockTrustStoreFile: String

            const val ALIAS = "TestAlias"

            private const val CN = "localhost"

            private const val SIGNATURE_ALGORITHM = "RSA"
        }
    }

    companion object {
        const val ALGORITHM = "TestAlgorithm"
    }
}
