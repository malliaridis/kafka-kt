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

import java.net.Socket
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import javax.net.ssl.ManagerFactoryParameters
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactorySpi
import javax.net.ssl.X509ExtendedTrustManager

class TestTrustManagerFactory : TrustManagerFactorySpi() {

    override fun engineInit(keyStore: KeyStore) {}

    override fun engineInit(managerFactoryParameters: ManagerFactoryParameters) {}

    override fun engineGetTrustManagers(): Array<TrustManager> = arrayOf(TestTrustManager())

    class TestTrustManager : X509ExtendedTrustManager() {
        @Throws(CertificateException::class)
        override fun checkClientTrusted(x509Certificates: Array<X509Certificate>, s: String) = Unit

        @Throws(CertificateException::class)
        override fun checkServerTrusted(x509Certificates: Array<X509Certificate>, s: String) = Unit

        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()

        @Throws(CertificateException::class)
        override fun checkClientTrusted(
            x509Certificates: Array<X509Certificate>,
            s: String,
            socket: Socket,
        ) = Unit

        @Throws(CertificateException::class)
        override fun checkServerTrusted(
            x509Certificates: Array<X509Certificate>,
            s: String,
            socket: Socket,
        ) = Unit

        @Throws(CertificateException::class)
        override fun checkClientTrusted(
            x509Certificates: Array<X509Certificate>,
            s: String,
            sslEngine: SSLEngine,
        ) = Unit

        @Throws(CertificateException::class)
        override fun checkServerTrusted(
            x509Certificates: Array<X509Certificate>,
            s: String,
            sslEngine: SSLEngine,
        ) = Unit

        companion object {
            const val ALIAS = "TestAlias"
        }
    }

    companion object {
        const val ALGORITHM = "TestAlgorithm"
    }
}

