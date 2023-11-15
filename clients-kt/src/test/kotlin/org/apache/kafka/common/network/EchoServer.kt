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

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
import org.apache.kafka.common.security.ssl.SslFactory

/**
 * A simple server that takes size delimited byte arrays and just echos them back to the sender.
 */
class EchoServer(
    securityProtocol: SecurityProtocol,
    configs: Map<String, *>,
) : Thread() {

    val port: Int

    private var serverSocket: ServerSocket? = null

    private val threads: MutableList<Thread>

    private val sockets: MutableList<Socket>

    @Volatile
    private var closing = false

    private var sslFactory: SslFactory? = null

    private val renegotiate = AtomicBoolean()

    init {
        when (securityProtocol) {
            SecurityProtocol.SSL -> {
                val sslContext: SSLContext
                sslFactory = SslFactory(Mode.SERVER).apply {
                    configure(configs)
                    sslContext = (sslEngineFactory as DefaultSslEngineFactory).sslContext()!!
                }
                serverSocket = sslContext.serverSocketFactory.createServerSocket(0)
            }

            SecurityProtocol.PLAINTEXT -> {
                serverSocket = ServerSocket(0)
                sslFactory = null
            }

            else -> throw IllegalArgumentException("Unsupported securityProtocol $securityProtocol")
        }
        port = serverSocket!!.getLocalPort()
        threads = Collections.synchronizedList(ArrayList())
        sockets = Collections.synchronizedList(ArrayList())
    }

    fun renegotiate() {
        renegotiate.set(true)
    }

    override fun run() {
        try {
            while (!closing) {
                val socket = serverSocket!!.accept()
                synchronized(sockets) {
                    if (closing) break
                    sockets.add(socket)
                    val thread: Thread = object : Thread() {
                        override fun run() {
                            socket.use {
                                try {
                                    val input = DataInputStream(socket.getInputStream())
                                    val output = DataOutputStream(socket.getOutputStream())
                                    while (socket.isConnected && !socket.isClosed) {
                                        val size = input.readInt()
                                        if (renegotiate.get()) {
                                            renegotiate.set(false)
                                            (socket as SSLSocket).startHandshake()
                                        }
                                        val bytes = ByteArray(size)
                                        input.readFully(bytes)
                                        output.writeInt(size)
                                        output.write(bytes)
                                        output.flush()
                                    }
                                } catch (e: IOException) {
                                    // ignore
                                }
                            }
                        }
                    }
                    thread.start()
                    threads.add(thread)
                }
            }
        } catch (e: IOException) {
            // ignore
        }
    }

    @Throws(IOException::class)
    fun closeConnections() {
        synchronized(sockets) { for (socket in sockets) socket.close() }
    }

    @Throws(IOException::class, InterruptedException::class)
    fun close() {
        closing = true
        serverSocket?.close()
        closeConnections()
        for (t in threads) t.join()
        join()
    }
}
