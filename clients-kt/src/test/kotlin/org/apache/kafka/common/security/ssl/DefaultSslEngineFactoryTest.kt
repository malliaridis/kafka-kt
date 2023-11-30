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

package org.apache.kafka.common.security.ssl

import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.test.TestUtils.tempFile
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DefaultSslEngineFactoryTest {

    private var factory = DefaultSslEngineFactory()

    var configs = mutableMapOf<String, Any?>()

    @BeforeEach
    fun setUp() {
        factory = DefaultSslEngineFactory()
        configs[SslConfigs.SSL_PROTOCOL_CONFIG] = "TLSv1.2"
    }

    @Test
    @Throws(Exception::class)
    fun testPemTrustStoreConfigWithOneCert() {
        configs[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] = pemAsConfigValue(CA1)
        configs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        factory.configure(configs)
        val trustStore = factory.truststore()!!
        val aliases = trustStore.aliases().toList()
        assertEquals(listOf("kafka0"), aliases)
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not loaded")
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key")
    }

    @Test
    @Throws(Exception::class)
    fun testPemTrustStoreConfigWithMultipleCerts() {
        configs[SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG] = pemAsConfigValue(CA1, CA2)
        configs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        factory.configure(configs)
        val trustStore = factory.truststore()!!
        val aliases = trustStore.aliases().toList()
        assertEquals(mutableListOf("kafka0", "kafka1"), aliases)
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not loaded")
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key")
        assertNotNull(trustStore.getCertificate("kafka1"), "Certificate not loaded")
        assertNull(trustStore.getKey("kafka1", null), "Unexpected private key")
    }

    @Test
    @Throws(Exception::class)
    fun testPemKeyStoreConfigNoPassword() = verifyPemKeyStoreConfig(
        keyFileName = KEY,
        keyPassword = null,
    )

    @Test
    @Throws(Exception::class)
    fun testPemKeyStoreConfigWithKeyPassword() = verifyPemKeyStoreConfig(
        keyFileName = ENCRYPTED_KEY,
        keyPassword = KEY_PASSWORD,
    )

    @Test
    @Throws(Exception::class)
    fun testTrailingNewLines() = verifyPemKeyStoreConfig(
        keyFileName = ENCRYPTED_KEY + "\n\n",
        keyPassword = KEY_PASSWORD,
    )

    @Test
    @Throws(Exception::class)
    fun testLeadingNewLines() {
        verifyPemKeyStoreConfig(
            keyFileName = """
                            
                            
                            $ENCRYPTED_KEY
                            """.trimIndent(),
            keyPassword = KEY_PASSWORD,
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCarriageReturnLineFeed() {
        verifyPemKeyStoreConfig(
            ENCRYPTED_KEY.replace("\n".toRegex(), "\r\n"),
            KEY_PASSWORD,
        )
    }

    @Throws(Exception::class)
    private fun verifyPemKeyStoreConfig(keyFileName: String, keyPassword: Password?) {
        configs[SslConfigs.SSL_KEYSTORE_KEY_CONFIG] = pemAsConfigValue(keyFileName)
        configs[SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG] = pemAsConfigValue(CERTCHAIN)
        configs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = keyPassword
        configs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        factory.configure(configs)
        val keyStore = factory.keystore()!!
        val aliases = keyStore.aliases().toList()
        assertEquals(listOf("kafka"), aliases)
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not loaded")
        assertNotNull(keyStore.getKey("kafka", keyPassword?.value?.toCharArray()), "Private key not loaded")
    }

    @Test
    @Throws(Exception::class)
    fun testPemTrustStoreFile() {
        configs[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = pemFilePath(CA1)
        configs[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        factory.configure(configs)
        val trustStore = factory.truststore()!!
        val aliases = trustStore.aliases().toList()
        assertEquals(listOf("kafka0"), aliases)
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not found")
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key")
    }

    @Test
    @Throws(Exception::class)
    fun testPemKeyStoreFileNoKeyPassword() {
        configs[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = pemFilePath(pemAsConfigValue(KEY, CERTCHAIN).value)
        configs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        configs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = null
        factory.configure(configs)
        val keyStore = factory.keystore()!!
        val aliases = keyStore.aliases().toList()
        assertEquals(listOf("kafka"), aliases)
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not loaded")
        assertNotNull(keyStore.getKey("kafka", null), "Private key not loaded")
    }

    @Test
    @Throws(Exception::class)
    fun testPemKeyStoreFileWithKeyPassword() {
        configs[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = pemFilePath(pemAsConfigValue(ENCRYPTED_KEY, CERTCHAIN).value)
        configs[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = KEY_PASSWORD
        configs[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = DefaultSslEngineFactory.PEM_TYPE
        factory.configure(configs)
        val keyStore = factory.keystore()!!
        val aliases = keyStore.aliases().toList()
        assertEquals(listOf("kafka"), aliases)
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not found")
        assertNotNull(keyStore.getKey("kafka", KEY_PASSWORD.value.toCharArray()), "Private key not found")
    }

    @Throws(Exception::class)
    private fun pemFilePath(pem: String): String = tempFile(contents = pem).absolutePath

    private fun pemAsConfigValue(vararg pemValues: String): Password {
        val builder = StringBuilder()
        for (pem in pemValues) {
            builder.append(pem)
            builder.append("\n")
        }

        return Password(builder.toString().trim { it <= ' ' })
    }

    companion object {

        /*
         * Key and certificates were extracted using openssl from a key store file created with 100 years validity
         * using:
         *
         * openssl pkcs12 -in server.keystore.p12 -nodes -nocerts -out test.key.pem -passin pass:key-password
         * openssl pkcs12 -in server.keystore.p12 -nodes -nokeys -out test.certchain.pem  -passin pass:key-password
         * openssl pkcs12 -in server.keystore.p12 -nodes  -out test.keystore.pem -passin pass:key-password
         * openssl pkcs8 -topk8 -v1 pbeWithSHA1And3-KeyTripleDES-CBC -in test.key.pem -out test.key.encrypted.pem -passout pass:key-password
         */
        private const val CA1 =
"""-----BEGIN CERTIFICATE-----
MIIC0zCCAbugAwIBAgIEStdXHTANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU
ZXN0Q0ExMCAXDTIwMDkyODA5MDI0MFoYDzIxMjAwOTA0MDkwMjQwWjASMRAwDgYD
VQQDEwdUZXN0Q0ExMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo3Gr
WJAkjnvgcuIfjArDhNdtAlRTt094WMUXhYDibgGtd+CLcWqA+c4PEoK4oybnKZqU
6MlDfPgesIK2YiNBuSVWMtZ2doageOBnd80Iwbg8DqGtQpUsvw8X5fOmuza+4inv
/8IpiTizq8YjSMT4nYDmIjyyRCSNY4atjgMnskutJ0v6i69+ZAA520Y6nn2n4RD5
8Yc+y7yCkbZXnYS5xBOFEExmtc0Xa7S9nM157xqKws9Z+rTKZYLrryaHI9JNcXgG
kzQEH9fBePASeWfi9AGRvAyS2GMSIBOsihIDIha/mqQcJOGCEqTMtefIj2FaErO2
bL9yU7OpW53iIC8y0QIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRf
svKcoQ9ZBvjwyUSV2uMFzlkOWDANBgkqhkiG9w0BAQsFAAOCAQEAEE1ZG2MGE248
glO83ROrHbxmnVWSQHt/JZANR1i362sY1ekL83wlhkriuvGVBlHQYWezIfo/4l9y
JTHNX3Mrs9eWUkaDXADkHWj3AyLXN3nfeU307x1wA7OvI4YKpwvfb4aYS8RTPz9d
JtrfR0r8aGTgsXvCe4SgwDBKv7bckctOwD3S7D/b6y3w7X0s7JCU5+8ZjgoYfcLE
gNqQEaOwdT2LHCvxHmGn/2VGs/yatPQIYYuufe5i8yX7pp4Xbd2eD6LULYkHFs3x
uJzMRI7BukmIIWuBbAkYI0atxLQIysnVFXdL9pBgvgso2nA3FgP/XeORhkyHVvtL
REH2YTlftQ==
-----END CERTIFICATE-----
"""

        private const val CA2 =
"""-----BEGIN CERTIFICATE-----
MIIC0zCCAbugAwIBAgIEfk9e9DANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU
ZXN0Q0EyMCAXDTIwMDkyODA5MDI0MVoYDzIxMjAwOTA0MDkwMjQxWjASMRAwDgYD
VQQDEwdUZXN0Q0EyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvCh0
UO5op9eHfz7mvZ7IySK7AOCTC56QYFJcU+hD6yk1wKg2qot7naI5ozAc8n7c4pMt
LjI3D0VtC/oHC29R2HNMSWyHcxIXw8z127XeCLRkCqYWuVAl3nBuWfWVPObjKetH
TWlQANYWAfk1VbS6wfzgp9cMaK7wQ+VoGEo4x3pjlrdlyg4k4O2yubcpWmJ2TjxS
gg7TfKGizUVAvF9wUG9Q4AlCg4uuww5RN9w6vnzDKGhWJhkQ6pf/m1xB+WueFOeU
aASGhGqCTqiz3p3M3M4OZzG3KptjQ/yb67x4T5U5RxqoiN4L57E7ZJLREpa6ZZNs
ps/gQ8dR9Uo/PRyAkQIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRg
IAOVH5LeE6nZmdScEE3JO/AhvTANBgkqhkiG9w0BAQsFAAOCAQEAHkk1iybwy/Lf
iEQMVRy7XfuC008O7jfCUBMgUvE+oO2RadH5MmsXHG3YerdsDM90dui4JqQNZOUh
kF8dIWPQHE0xDsR9jiUsemZFpVMN7DcvVZ3eFhbvJA8Q50rxcNGA+tn9xT/xdQ6z
1eRq9IPoYcRexQ7s9mincM4T4lLm8GGcd7ZPHy8kw0Bp3E/enRHWaF5b8KbXezXD
I3SEYUyRL2K3px4FImT4X9XQm2EX6EONlu4GRcJpD6RPc0zC7c9dwEnSo+0NnewR
gjgO34CLzShB/kASLS9VQXcUC6bsggAVK2rWQMmy35SOEUufSuvg8kUFoyuTzfhn
hL+PVwIu7g==
-----END CERTIFICATE-----
"""

        private const val CERTCHAIN =
"""Bag Attributes
    friendlyName: server
    localKeyID: 54 69 6D 65 20 31 36 30 31 32 38 33 37 36 35 34 32 33 
subject=/CN=TestBroker
issuer=/CN=TestCA1
-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIEatBnEzANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU
ZXN0Q0ExMCAXDTIwMDkyODA5MDI0NFoYDzIxMjAwOTA0MDkwMjQ0WjAVMRMwEQYD
VQQDEwpUZXN0QnJva2VyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
pkw1AS71ej/iOMvzVgVL1dkQOYzI842NcPmx0yFFsue2umL8WVd3085NgWRb3SS1
4X676t7zxjPGzYi7jwmA8stCrDt0NAPWd/Ko6ErsCs87CUs4u1Cinf+b3o9NF5u0
UPYBQLF4Ir8T1jQ+tKiqsChGDt6urRAg1Cro5i7r10jN1uofY2tBs+r8mALhJ17c
T5LKawXeYwNOQ86c5djClbcP0RrfcPyRyj1/Cp1axo28iO0fXFyO2Zf3a4vtt+Ih
PW+A2tL+t3JTBd8g7Fl3ozzpcotAi7MDcZaYA9GiTP4DOiKUeDt6yMYQQr3VEqGa
pXp4fKY+t9slqnAmcBZ4kQIDAQABo1gwVjAfBgNVHSMEGDAWgBRfsvKcoQ9ZBvjw
yUSV2uMFzlkOWDAUBgNVHREEDTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFGWt+27P
INk/S5X+PRV/jW3WOhtaMA0GCSqGSIb3DQEBCwUAA4IBAQCLHCjFFvqa+0GcG9eq
v1QWaXDohY5t5CCwD8Z+lT9wcSruTxDPwL7LrR36h++D6xJYfiw4iaRighoA40xP
W6+0zGK/UtWV4t+ODTDzyAWgls5w+0R5ki6447qGqu5tXlW5DCHkkxWiozMnhNU2
G3P/Drh7DhmADDBjtVLsu5M1sagF/xwTP/qCLMdChlJNdeqyLnAUa9SYG1eNZS/i
wrCC8m9RUQb4+OlQuFtr0KhaaCkBXfmhigQAmh44zSyO+oa3qQDEavVFo/Mcui9o
WBYetcgVbXPNoti+hQEMqmJYBHlLbhxMnkooGn2fa70f453Bdu/Xh6Yphi5NeCHn
1I+y
-----END CERTIFICATE-----
Bag Attributes
    friendlyName: CN=TestCA1
subject=/CN=TestCA1
issuer=/CN=TestCA1
-----BEGIN CERTIFICATE-----
MIIC0zCCAbugAwIBAgIEStdXHTANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU
ZXN0Q0ExMCAXDTIwMDkyODA5MDI0MFoYDzIxMjAwOTA0MDkwMjQwWjASMRAwDgYD
VQQDEwdUZXN0Q0ExMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo3Gr
WJAkjnvgcuIfjArDhNdtAlRTt094WMUXhYDibgGtd+CLcWqA+c4PEoK4oybnKZqU
6MlDfPgesIK2YiNBuSVWMtZ2doageOBnd80Iwbg8DqGtQpUsvw8X5fOmuza+4inv
/8IpiTizq8YjSMT4nYDmIjyyRCSNY4atjgMnskutJ0v6i69+ZAA520Y6nn2n4RD5
8Yc+y7yCkbZXnYS5xBOFEExmtc0Xa7S9nM157xqKws9Z+rTKZYLrryaHI9JNcXgG
kzQEH9fBePASeWfi9AGRvAyS2GMSIBOsihIDIha/mqQcJOGCEqTMtefIj2FaErO2
bL9yU7OpW53iIC8y0QIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRf
svKcoQ9ZBvjwyUSV2uMFzlkOWDANBgkqhkiG9w0BAQsFAAOCAQEAEE1ZG2MGE248
glO83ROrHbxmnVWSQHt/JZANR1i362sY1ekL83wlhkriuvGVBlHQYWezIfo/4l9y
JTHNX3Mrs9eWUkaDXADkHWj3AyLXN3nfeU307x1wA7OvI4YKpwvfb4aYS8RTPz9d
JtrfR0r8aGTgsXvCe4SgwDBKv7bckctOwD3S7D/b6y3w7X0s7JCU5+8ZjgoYfcLE
gNqQEaOwdT2LHCvxHmGn/2VGs/yatPQIYYuufe5i8yX7pp4Xbd2eD6LULYkHFs3x
uJzMRI7BukmIIWuBbAkYI0atxLQIysnVFXdL9pBgvgso2nA3FgP/XeORhkyHVvtL
REH2YTlftQ==
-----END CERTIFICATE-----
"""

        private const val KEY =
"""Bag Attributes
    friendlyName: server
    localKeyID: 54 69 6D 65 20 31 36 30 31 32 38 33 37 36 35 34 32 33
Key Attributes: <No Attributes>
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCmTDUBLvV6P+I4
y/NWBUvV2RA5jMjzjY1w+bHTIUWy57a6YvxZV3fTzk2BZFvdJLXhfrvq3vPGM8bN
iLuPCYDyy0KsO3Q0A9Z38qjoSuwKzzsJSzi7UKKd/5vej00Xm7RQ9gFAsXgivxPW
ND60qKqwKEYO3q6tECDUKujmLuvXSM3W6h9ja0Gz6vyYAuEnXtxPksprBd5jA05D
zpzl2MKVtw/RGt9w/JHKPX8KnVrGjbyI7R9cXI7Zl/dri+234iE9b4Da0v63clMF
3yDsWXejPOlyi0CLswNxlpgD0aJM/gM6IpR4O3rIxhBCvdUSoZqlenh8pj632yWq
cCZwFniRAgMBAAECggEAOfC/XwQvf0KW3VciF0yNGZshbgvBUCp3p284J+ml0Smu
ns4yQiaZl3B/zJ9c6nYJ8OEpNDIuGVac46vKPZIAHZf4SO4GFMFpji078IN6LmH5
nclZoNn9brNKaYbgQ2N6teKgmRu8Uc7laHKXjnZd0jaWAkRP8/h0l7fDob+jaERj
oJBx4ux2Z62TTCP6W4VY3KZgSL1p6dQswqlukPVytMeI2XEwWnO+w8ED0BxCxM4F
K//dw7nUMGS9GUNkgyDcH1akYSCDzdBeymQBp2latBotVfGNK1hq9nC1iaxmRkJL
sYjwVc24n37u+txOovy3daq2ySj9trF7ySAPVYkh4QKBgQDWeN/MR6cy1TLF2j3g
eMMeM32LxXArIPsar+mft+uisKWk5LDpsKpph93sl0JjFi4x0t1mqw23h23I+B2c
JWiPAHUG3FGvvkPPcfMUvd7pODyE2XaXi+36UZAH7qc94VZGJEb+sPITckSruREE
QErWZyrbBRgvQXsmVme5B2/kRQKBgQDGf2HQH0KHl54O2r9vrhiQxWIIMSWlizJC
hjboY6DkIsAMwnXp3wn3Bk4tSgeLk8DEVlmEaE3gvGpiIp0vQnSOlME2TXfEthdM
uS3+BFXN4Vxxx/qjKL2WfZloyzdaaaF7s+LIwmXgLsFFCUSq+uLtBqfpH2Qv+paX
Xqm7LN3V3QKBgH5ssj/Q3RZx5oQKqf7wMNRUteT2dbB2uI56s9SariQwzPPuevrG
US30ETWt1ExkfsaP7kLfAi71fhnBaHLq+j+RnWp15REbrw1RtmC7q/L+W25UYjvj
GF0+RxDl9V/cvOaL6+2mkIw2B5TSet1uqK7KEdEZp6/zgYyP0oSXhbWhAoGAdnlZ
HCtMPjnUcPFHCZVTvDTTSihrW9805FfPNe0g/olvLy5xymEBRZtR1d41mq1ZhNY1
H75RnS1YIbKfNrHnd6J5n7ulHJfCWFy+grp7rCIyVwcRJYkPf17/zXhdVW1uoLLB
TSoaPDAr0tSxU4vjHa23UoEV/z0F3Nr3W2xwC1ECgYBHKjv6ekLhx7HbP797+Ai+
wkHvS2L/MqEBxuHzcQ9G6Mj3ANAeyDB8YSC8qGtDQoEyukv2dO73lpodNgbR8P+Q
PDBb6eyntAo2sSeo0jZkiXvDOfRaGuGVrxjuTfaqcVB33jC6BYfi61/3Sr5oG9Nd
tDGh1HlOIRm1jD9KQNVZ/Q==
-----END PRIVATE KEY-----
"""

        private const val ENCRYPTED_KEY =
"""-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIE6jAcBgoqhkiG9w0BDAEDMA4ECGyAEWAXlaXzAgIIAASCBMgt7QD1Bbz7MAHI
Ni0eTrwNiuAPluHirLXzsV57d1O9i4EXVp5nzRy6753cjXbGXARbBeaJD+/+jbZp
CBZTHMG8rTCfbsg5kMqxT6XuuqWlKLKc4gaq+QNgHHleKqnpwZQmOQ+awKWEK/Ow
Z0KxXqkp+b4/qJK3MqKZDsJtVdyUhO0tLVxd+BHDg9B93oExc87F16h3R0+T4rxE
Tvz2c2upBqva49AbLDxpWXLCJC8CRkxM+KHrPkYjpNx3jCjtwiiXfzJCWjuCkVrL
2F4bqvpYPIseoPtMvWaplNtoPwhpzBB/hoJ+R+URr4XHX3Y+bz6k6iQnhoCOIviy
oEEUvWtKnaEEKSauR+Wyj3MoeB64g9NWMEHv7+SQeA4WqlgV2s4txwRxFGKyKLPq
caMSpfxvYujtSh0DOv9GI3cVHPM8WsebCz9cNrbKSR8/8JufcoonTitwF/4vm1Et
AdmCuH9JIYVvmFKFVxY9SvRAvo43OQaPmJQHMUa4yDfMtpTSgmB/7HFgxtksYs++
Gbrq6F/hon+0bLx+bMz2FK635UU+iVno+qaScKWN3BFqDl+KnZprBhLSXTT3aHmp
fisQit/HWp71a0Vzq85WwI4ucMKNc8LemlwNBxWLLiJDp7sNPLb5dIl8yIwSEIgd
vC5px9KWEdt3GxTUEqtIeBmagbBhahcv+c9Dq924DLI+Slv6TJKZpIcMqUECgzvi
hb8gegyEscBEcDSzl0ojlFVz4Va5eZS/linTjNJhnkx8BKLn/QFco7FpEE6uOmQ3
0kF64M2Rv67cJbYVrhD46TgIzH3Y/FOMSi1zFHQ14nVXWMu0yAlBX+QGk7Xl+/aF
BIq+i9WcBqbttR3CwyeTnIFXkdC66iTZYhDl9HT6yMcazql2Or2TjIIWr6tfNWH/
5dWSEHYM5m8F2/wF0ANWJyR1oPr4ckcUsfl5TfOWVj5wz4QVF6EGV7FxEnQHrdx0
6rXThRKFjqxUubsNt1yUEwdlTNz2UFhobGF9MmFeB97BZ6T4v8G825de/Caq9FzO
yMFFCRcGC7gIzMXRPEjHIvBdTThm9rbNzKPXHqw0LHG478yIqzxvraCYTRw/4eWN
Q+hyOL/5T5QNXHpR8Udp/7sptw7HfRnecQ/Vz9hOKShQq3h4Sz6eQMQm7P9qGo/N
bltEAIECRVcNYLN8LuEORfeecNcV3BX+4BBniFtdD2bIRsWC0ZUsGf14Yhr4P1OA
PtMJzy99mrcq3h+o+hEW6bhIj1gA88JSMJ4iRuwTLRKE81w7EyziScDsotYKvDPu
w4+PFbQO3fr/Zga3LgYis8/DMqZoWjVCjAeVoypuOZreieZYC/BgBS8qSUAmDPKq
jK+T5pwMMchfXbkV80LTu1kqLfKWdE0AmZfGy8COE/NNZ/FeiWZPdwu2Ix6u/RoY
LTjNy4YLIBdVELFXaFJF2GfzLpnwrW5tyNPVVrGmUoiyOzgx8gMyCLGavGtduyoY
tBiUTmd05Ugscn4Rz9X30S4NbnjL/h+bWl1m6/M+9FHEe85FPxmt/GRmJPbFPMR5
q5EgQGkt4ifiaP6qvyFulwvVwx+m0bf1q6Vb/k3clIyLMcVZWFE1TqNH2Ife46AE
2I39ZnGTt0mbWskpHBA=
-----END ENCRYPTED PRIVATE KEY-----
"""

        private val KEY_PASSWORD = Password("key-password")
    }
}

