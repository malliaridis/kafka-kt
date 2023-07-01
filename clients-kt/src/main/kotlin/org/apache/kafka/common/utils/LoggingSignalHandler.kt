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

package org.apache.kafka.common.utils

import java.lang.reflect.Constructor
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.common.utils.Utils.contextOrKafkaClassLoader
import org.slf4j.LoggerFactory

/**
 * @constructor Create an instance of this class.
 * @throws ReflectiveOperationException if the underlying API has changed in an incompatible manner.
 */
class LoggingSignalHandler {

    private val signalClass: Class<*> = Class.forName("sun.misc.Signal")

    private val signalConstructor: Constructor<*> = signalClass.getConstructor(String::class.java)

    private val signalHandlerClass: Class<*> = Class.forName("sun.misc.SignalHandler")

    private val signalHandlerHandleMethod: Method =
        signalHandlerClass.getMethod("handle", signalClass)

    private val signalHandleMethod: Method =
        signalClass.getMethod("handle", signalClass, signalHandlerClass)

    private val signalGetNameMethod: Method = signalClass.getMethod("getName")

    /**
     * Register signal handler to log termination due to SIGTERM, SIGHUP and SIGINT (control-c).
     * This method does not currently work on Windows.
     *
     * @implNote sun.misc.Signal and sun.misc.SignalHandler are described as "not encapsulated" in
     * http://openjdk.java.net/jeps/260. However, they are not available in the compile classpath if
     * the `--release` flag is used. As a workaround, we rely on reflection.
     */
    @Throws(ReflectiveOperationException::class)
    fun register() {
        val jvmSignalHandlers: MutableMap<String, Any> = ConcurrentHashMap()
        SIGNALS.forEach { signal -> register(signal, jvmSignalHandlers) }

        log.info("Registered signal handlers for " + java.lang.String.join(", ", SIGNALS))
    }

    private fun createSignalHandler(jvmSignalHandlers: Map<String, Any>): Any {

        val invocationHandler: InvocationHandler = object : InvocationHandler {

            @Throws(Throwable::class)
            private fun getName(signal: Any): String {
                return try {
                    signalGetNameMethod.invoke(signal) as String
                } catch (e: InvocationTargetException) {
                    throw e.cause!!
                }
            }

            @Throws(ReflectiveOperationException::class)
            private fun handle(signalHandler: Any, signal: Any) {
                signalHandlerHandleMethod.invoke(signalHandler, signal)
            }

            @Throws(Throwable::class)
            override fun invoke(proxy: Any, method: Method, args: Array<Any>): Any? {
                val signal = args[0]
                log.info("Terminating process due to signal {}", signal)
                val handler = jvmSignalHandlers[getName(signal)]
                handler?.let { handle(it, signal) }
                return null
            }
        }

        return Proxy.newProxyInstance(
            contextOrKafkaClassLoader,
            arrayOf(signalHandlerClass),
            invocationHandler
        )
    }

    @Throws(ReflectiveOperationException::class)
    private fun register(signalName: String, jvmSignalHandlers: MutableMap<String, Any>) {
        val signal = signalConstructor.newInstance(signalName)
        val signalHandler = createSignalHandler(jvmSignalHandlers)
        val oldHandler = signalHandleMethod.invoke(null, signal, signalHandler)

        if (oldHandler != null) jvmSignalHandlers[signalName] = oldHandler
    }

    companion object {

        private val log = LoggerFactory.getLogger(LoggingSignalHandler::class.java)

        private val SIGNALS: List<String> = mutableListOf("TERM", "INT", "HUP")
    }
}
