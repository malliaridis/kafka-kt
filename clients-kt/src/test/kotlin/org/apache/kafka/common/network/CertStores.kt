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

package org.apache.kafka.common.network

import java.net.InetAddress
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.test.TestSslUtils.CertificateBuilder
import org.apache.kafka.test.TestSslUtils.SslConfigsBuilder
import org.apache.kafka.test.TestUtils.tempFile

class CertStores private constructor(
    server: Boolean,
    commonName: String,
    keyAlgorithm: String,
    certBuilder: CertificateBuilder,
    usePem: Boolean,
) {

    private lateinit var sslConfig: Map<String, Any?>

    val untrustingConfig: Map<String, Any?>
        get() = sslConfig

    constructor(server: Boolean, hostName: String) : this(
        server = server,
        commonName = hostName,
        certBuilder = CertificateBuilder(),
    )

    constructor(server: Boolean, commonName: String, sanHostName: String?) : this(
        server = server,
        commonName = commonName,
        certBuilder = CertificateBuilder().sanDnsNames(sanHostName),
    )

    constructor(server: Boolean, commonName: String, hostAddress: InetAddress) : this(
        server = server,
        commonName = commonName,
        certBuilder = CertificateBuilder().sanIpAddress(hostAddress),
    )

    private constructor(
        server: Boolean,
        commonName: String,
        certBuilder: CertificateBuilder,
    ) : this(
        server = server,
        commonName = commonName,
        keyAlgorithm = "RSA",
        certBuilder = certBuilder,
        usePem = false,
    )

    init {
        val name = if (server) "server" else "client"
        val mode = if (server) Mode.SERVER else Mode.CLIENT
        val truststoreFile = if (usePem) null else tempFile(name + "TS", ".jks")
        sslConfig = SslConfigsBuilder(mode)
            .useClientCert(!server)
            .certAlias(name)
            .cn(commonName)
            .createNewTrustStore(truststoreFile)
            .certBuilder(certBuilder)
            .algorithm(keyAlgorithm)
            .usePem(usePem)
            .build()
    }

    fun getTrustingConfig(truststoreConfig: CertStores): Map<String, Any?> {
        return sslConfig + TRUSTSTORE_PROPS.associateWith { truststoreConfig.sslConfig[it] }
    }

    fun keyStoreProps(): Map<String, Any?> {
        return KEYSTORE_PROPS.associateWith { propName -> untrustingConfig[propName] }
    }

    fun trustStoreProps(): Map<String, Any?> {
        return TRUSTSTORE_PROPS.associateWith { propName -> untrustingConfig[propName] }
    }

    class Builder(private val isServer: Boolean) {

        private var cn: String? = null

        private val sanDns = mutableListOf<String>()

        private var sanIp: InetAddress? = null

        private var keyAlgorithm = "RSA"

        private var usePem = false

        fun cn(cn: String?): Builder {
            this.cn = cn
            return this
        }

        fun addHostName(hostname: String): Builder {
            sanDns.add(hostname)
            return this
        }

        fun hostAddress(hostAddress: InetAddress?): Builder {
            sanIp = hostAddress
            return this
        }

        fun keyAlgorithm(keyAlgorithm: String): Builder {
            this.keyAlgorithm = keyAlgorithm
            return this
        }

        fun usePem(usePem: Boolean): Builder {
            this.usePem = usePem
            return this
        }

        @Throws(Exception::class)
        fun build(): CertStores {
            var certBuilder = CertificateBuilder().sanDnsNames(*sanDns.toTypedArray())

            if (sanIp != null) certBuilder = certBuilder.sanIpAddress(sanIp!!)

            return CertStores(
                server = isServer,
                commonName = cn!!,
                keyAlgorithm = keyAlgorithm,
                certBuilder = certBuilder,
                usePem = usePem
            )
        }
    }

    companion object {

        val KEYSTORE_PROPS = setOf(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_KEY_CONFIG,
            SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
        )

        val TRUSTSTORE_PROPS = setOf(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
        )
    }
}
