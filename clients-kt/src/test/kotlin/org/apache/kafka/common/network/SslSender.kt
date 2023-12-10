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

import java.io.OutputStream
import java.net.InetSocketAddress
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.X509TrustManager

class SslSender(
    private val tlsProtocol: String,
    private val serverAddress: InetSocketAddress,
    private val payload: ByteArray,
) : Thread() {

    private val handshaked = CountDownLatch(1)

    init {
        setDaemon(true)
        setName("SslSender - ${payload.size} bytes @ $serverAddress")
    }

    override fun run() {
        try {
            val sc = SSLContext.getInstance(tlsProtocol)
            sc.init(null, arrayOf(NaiveTrustManager()), SecureRandom())
            val socket = sc.socketFactory.createSocket(serverAddress.address, serverAddress.port) as SSLSocket
            socket.use { connection ->
                val os: OutputStream = connection.getOutputStream()
                connection.startHandshake()
                handshaked.countDown()
                os.write(payload)
                os.flush()
            }
        } catch (e: Exception) {
            e.printStackTrace(System.err)
        }
    }

    @Throws(InterruptedException::class)
    fun waitForHandshake(timeoutMillis: Long): Boolean {
        return handshaked.await(timeoutMillis, TimeUnit.MILLISECONDS)
    }

    /**
     * blindly trust any certificate presented to it
     */
    private class NaiveTrustManager : X509TrustManager {

        override fun checkClientTrusted(x509Certificates: Array<X509Certificate>, s: String) = Unit

        override fun checkServerTrusted(x509Certificates: Array<X509Certificate>, s: String) = Unit

        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
    }
}
