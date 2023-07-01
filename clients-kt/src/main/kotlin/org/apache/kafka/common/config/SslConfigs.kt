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

package org.apache.kafka.common.config

import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.utils.Java
import org.apache.kafka.common.utils.Utils.mkSet


object SslConfigs {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL
     * BREAK USER CODE.
     */

    const val SSL_PROTOCOL_CONFIG = "ssl.protocol"

    const val SSL_PROTOCOL_DOC = ("The SSL protocol used to generate the SSLContext. "
            + "The default is 'TLSv1.3' when running with Java 11 or newer, 'TLSv1.2' otherwise. "
            + "This value should be fine for most use cases. "
            + "Allowed values in recent JVMs are 'TLSv1.2' and 'TLSv1.3'. 'TLS', 'TLSv1.1', 'SSL', 'SSLv2' and 'SSLv3' "
            + "may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities. "
            + "With the default value for this config and 'ssl.enabled.protocols', clients will downgrade to 'TLSv1.2' if "
            + "the server does not support 'TLSv1.3'. If this config is set to 'TLSv1.2', clients will not use 'TLSv1.3' even "
            + "if it is one of the values in ssl.enabled.protocols and the server only supports 'TLSv1.3'.")

    val DEFAULT_SSL_PROTOCOL: String = if (Java.IS_JAVA11_COMPATIBLE) "TLSv1.3" else "TLSv1.2"

    const val SSL_PROVIDER_CONFIG = "ssl.provider"

    const val SSL_PROVIDER_DOC =
        "The name of the security provider used for SSL connections. Default value is the default security provider of the JVM."

    const val SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites"

    const val SSL_CIPHER_SUITES_DOC =
        ("A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. "
                + "By default all the available cipher suites are supported.")

    const val SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols"

    const val SSL_ENABLED_PROTOCOLS_DOC = ("The list of protocols enabled for SSL connections. "
            + "The default is 'TLSv1.2,TLSv1.3' when running with Java 11 or newer, 'TLSv1.2' otherwise. With the "
            + "default value for Java 11, clients and servers will prefer TLSv1.3 if both support it and fallback "
            + "to TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine for most "
            + "cases. Also see the config documentation for `ssl.protocol`.")

    val DEFAULT_SSL_ENABLED_PROTOCOLS: String =
        if (Java.IS_JAVA11_COMPATIBLE) "TLSv1.2,TLSv1.3" else "TLSv1.2"

    const val SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type"

    const val SSL_KEYSTORE_TYPE_DOC = ("The file format of the key store file. "
            + "This is optional for client. The values currently supported by the default `ssl.engine.factory.class` are [JKS, PKCS12, PEM].")

    const val DEFAULT_SSL_KEYSTORE_TYPE = "JKS"

    const val SSL_KEYSTORE_KEY_CONFIG = "ssl.keystore.key"

    const val SSL_KEYSTORE_KEY_DOC = ("Private key in the format specified by 'ssl.keystore.type'. "
            + "Default SSL engine factory supports only PEM format with PKCS#8 keys. If the key is encrypted, "
            + "key password must be specified using 'ssl.key.password'")

    const val SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = "ssl.keystore.certificate.chain"

    const val SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC =
        ("Certificate chain in the format specified by 'ssl.keystore.type'. "
                + "Default SSL engine factory supports only PEM format with a list of X.509 certificates")

    const val SSL_TRUSTSTORE_CERTIFICATES_CONFIG = "ssl.truststore.certificates"

    const val SSL_TRUSTSTORE_CERTIFICATES_DOC =
        ("Trusted certificates in the format specified by 'ssl.truststore.type'. "
                + "Default SSL engine factory supports only PEM format with X.509 certificates.")

    const val SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location"

    const val SSL_KEYSTORE_LOCATION_DOC = ("The location of the key store file. "
            + "This is optional for client and can be used for two-way authentication for client.")

    const val SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password"

    const val SSL_KEYSTORE_PASSWORD_DOC = ("The store password for the key store file. "
            + "This is optional for client and only needed if 'ssl.keystore.location' is configured. "
            + "Key store password is not supported for PEM format.")

    const val SSL_KEY_PASSWORD_CONFIG = "ssl.key.password"

    const val SSL_KEY_PASSWORD_DOC = ("The password of the private key in the key store file or "
            + "the PEM key specified in 'ssl.keystore.key'.")

    const val SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type"

    const val SSL_TRUSTSTORE_TYPE_DOC =
        "The file format of the trust store file. The values currently supported by the default `ssl.engine.factory.class` are [JKS, PKCS12, PEM]."

    const val DEFAULT_SSL_TRUSTSTORE_TYPE = "JKS"

    const val SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location"

    const val SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file."

    const val SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password"

    const val SSL_TRUSTSTORE_PASSWORD_DOC = ("The password for the trust store file. "
            + "If a password is not set, trust store file configured will still be used, but integrity checking is disabled. "
            + "Trust store password is not supported for PEM format.")

    const val SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm"

    const val SSL_KEYMANAGER_ALGORITHM_DOC =
        ("The algorithm used by key manager factory for SSL connections. "
                + "Default value is the key manager factory algorithm configured for the Java Virtual Machine.")

    val DEFAULT_SSL_KEYMANGER_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm()

    const val SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm"

    const val SSL_TRUSTMANAGER_ALGORITHM_DOC =

        ("The algorithm used by trust manager factory for SSL connections. "
                + "Default value is the trust manager factory algorithm configured for the Java Virtual Machine.")

    val DEFAULT_SSL_TRUSTMANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm()

    const val SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "ssl.endpoint.identification.algorithm"

    const val SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
        "The endpoint identification algorithm to validate server hostname using server certificate. "

    const val DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "https"

    const val SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = "ssl.secure.random.implementation"

    const val SSL_SECURE_RANDOM_IMPLEMENTATION_DOC =
        "The SecureRandom PRNG implementation to use for SSL cryptography operations. "

    const val SSL_ENGINE_FACTORY_CLASS_CONFIG = "ssl.engine.factory.class"

    const val SSL_ENGINE_FACTORY_CLASS_DOC =
        "The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects. Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory"

    fun addClientSslSupport(config: ConfigDef) {
        config.define(
            name = SSL_PROTOCOL_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_PROTOCOL,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SSL_PROTOCOL_DOC
        ).define(
            name = SSL_PROVIDER_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SSL_PROVIDER_DOC
        ).define(
            name = SSL_CIPHER_SUITES_CONFIG,
            type = ConfigDef.Type.LIST,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_CIPHER_SUITES_DOC
        ).define(
            name = SSL_ENABLED_PROTOCOLS_CONFIG,
            type = ConfigDef.Type.LIST,
            defaultValue = DEFAULT_SSL_ENABLED_PROTOCOLS,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SSL_ENABLED_PROTOCOLS_DOC
        ).define(
            name = SSL_KEYSTORE_TYPE_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_KEYSTORE_TYPE,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SSL_KEYSTORE_TYPE_DOC
        ).define(
            name = SSL_KEYSTORE_LOCATION_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_KEYSTORE_LOCATION_DOC
        ).define(
            name = SSL_KEYSTORE_PASSWORD_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_KEYSTORE_PASSWORD_DOC
        ).define(
            name = SSL_KEY_PASSWORD_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_KEY_PASSWORD_DOC
        ).define(
            name = SSL_KEYSTORE_KEY_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_KEYSTORE_KEY_DOC
        ).define(
            name = SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC
        ).define(
            name = SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_TRUSTSTORE_CERTIFICATES_DOC
        ).define(
            name = SSL_TRUSTSTORE_TYPE_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_TRUSTSTORE_TYPE,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = SSL_TRUSTSTORE_TYPE_DOC
        ).define(
            name = SSL_TRUSTSTORE_LOCATION_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_TRUSTSTORE_LOCATION_DOC
        ).define(
            name = SSL_TRUSTSTORE_PASSWORD_CONFIG,
            type = ConfigDef.Type.PASSWORD,
            defaultValue = null,
            importance = ConfigDef.Importance.HIGH,
            documentation = SSL_TRUSTSTORE_PASSWORD_DOC
        ).define(
            name = SSL_KEYMANAGER_ALGORITHM_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_KEYMANGER_ALGORITHM,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_KEYMANAGER_ALGORITHM_DOC
        ).define(
            name = SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_TRUSTMANAGER_ALGORITHM,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_TRUSTMANAGER_ALGORITHM_DOC
        ).define(
            name = SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
        ).define(
            name = SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            type = ConfigDef.Type.STRING,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_SECURE_RANDOM_IMPLEMENTATION_DOC
        ).define(
            name = SSL_ENGINE_FACTORY_CLASS_CONFIG,
            type = ConfigDef.Type.CLASS,
            defaultValue = null,
            importance = ConfigDef.Importance.LOW,
            documentation = SSL_ENGINE_FACTORY_CLASS_DOC
        )
    }

    val RECONFIGURABLE_CONFIGS = setOf(
        SSL_KEYSTORE_TYPE_CONFIG,
        SSL_KEYSTORE_LOCATION_CONFIG,
        SSL_KEYSTORE_PASSWORD_CONFIG,
        SSL_KEY_PASSWORD_CONFIG,
        SSL_TRUSTSTORE_TYPE_CONFIG,
        SSL_TRUSTSTORE_LOCATION_CONFIG,
        SSL_TRUSTSTORE_PASSWORD_CONFIG,
        SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
        SSL_KEYSTORE_KEY_CONFIG,
        SSL_TRUSTSTORE_CERTIFICATES_CONFIG
    )

    val NON_RECONFIGURABLE_CONFIGS = setOf(
        BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
        SSL_PROTOCOL_CONFIG,
        SSL_PROVIDER_CONFIG,
        SSL_CIPHER_SUITES_CONFIG,
        SSL_ENABLED_PROTOCOLS_CONFIG,
        SSL_KEYMANAGER_ALGORITHM_CONFIG,
        SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
        SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
        SSL_ENGINE_FACTORY_CLASS_CONFIG
    )
}
