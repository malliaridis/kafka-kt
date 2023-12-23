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

package org.apache.kafka.trogdor.rest

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.ThreadUtils.createThreadFactory
import org.apache.kafka.trogdor.common.JsonUtil
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.CustomRequestLog
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.Slf4jRequestLogWriter
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import org.eclipse.jetty.server.handler.RequestLogHandler
import org.eclipse.jetty.server.handler.StatisticsHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Embedded server for the REST API that provides the control plane for Trogdor.
 *
 * @constructor Create a REST server for this herder using the specified configs.
 * @param port The port number to use for the REST server, or 0 to use a random port.
 */
class JsonRestServer(port: Int) {

    private val shutdownExecutor = Executors.newSingleThreadScheduledExecutor(
        createThreadFactory("JsonRestServerCleanupExecutor", false)
    )

    private val jettyServer: Server = Server()

    private val connector: ServerConnector = ServerConnector(jettyServer)

    init {
        if (port > 0) connector.port = port
        jettyServer.setConnectors(arrayOf<Connector>(connector))
    }

    /**
     * Start the JsonRestServer.
     *
     * @param resources The path handling resources to register.
     */
    fun start(vararg resources: Any?) {
        log.info("Starting REST server")
        val resourceConfig = ResourceConfig()
        resourceConfig.register(JacksonJsonProvider(JsonUtil.JSON_SERDE))
        for (resource in resources) {
            resourceConfig.register(resource)
            log.info("Registered resource {}", resource)
        }
        resourceConfig.register(RestExceptionMapper::class.java)
        val servletContainer = ServletContainer(resourceConfig)
        val servletHolder = ServletHolder(servletContainer)
        val context = ServletContextHandler(ServletContextHandler.SESSIONS)
        context.setContextPath("/")
        context.addServlet(servletHolder, "/*")

        val requestLogHandler = RequestLogHandler()
        val slf4jRequestLogWriter = Slf4jRequestLogWriter()
        slf4jRequestLogWriter.loggerName = JsonRestServer::class.java.getCanonicalName()
        val requestLog = CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %{ms}T")
        requestLogHandler.setRequestLog(requestLog)

        val handlers = HandlerCollection()
        handlers.setHandlers(arrayOf<Handler>(context, DefaultHandler(), requestLogHandler))
        val statsHandler = StatisticsHandler()
        statsHandler.setHandler(handlers)
        jettyServer.setHandler(statsHandler)
        // Needed for graceful shutdown as per `setStopTimeout` documentation
        jettyServer.stopTimeout = GRACEFUL_SHUTDOWN_TIMEOUT_MS
        jettyServer.setStopAtShutdown(true)

        try {
            jettyServer.start()
        } catch (exception: Exception) {
            throw RuntimeException("Unable to start REST server", exception)
        }
        log.info("REST server listening at " + jettyServer.getURI())
    }

    fun port(): Int = connector.localPort

    /**
     * Initiate shutdown, but do not wait for it to complete.
     */
    fun beginShutdown() {
        if (!shutdownExecutor.isShutdown) {
            shutdownExecutor.submit(Callable {
                try {
                    log.info("Stopping REST server")
                    jettyServer.stop()
                    jettyServer.join()
                    log.info("REST server stopped")
                } catch (e: Exception) {
                    log.error("Unable to stop REST server", e)
                } finally {
                    try {
                        jettyServer.destroy()
                    } catch (e: Exception) {
                        log.error("Unable to destroy REST server", e)
                    }
                    shutdownExecutor.shutdown()
                }
            })
        }
    }

    /**
     * Wait for shutdown to complete.  May be called prior to beginShutdown.
     */
    @Throws(InterruptedException::class)
    fun waitForShutdown() {
        while (!shutdownExecutor.isShutdown) {
            shutdownExecutor.awaitTermination(1, TimeUnit.DAYS)
        }
    }

    class HttpResponse<T> internal constructor(
        private val body: T,
        val error: ErrorResponse? = null,
    ) {
        @Throws(Exception::class)
        fun body(): T {
            if (error != null) throw RestExceptionMapper.toException(error.code(), error.message())
            return body
        }

        @Deprecated(
            message = "Use property instead",
            replaceWith = ReplaceWith("error"),
        )
        fun error(): ErrorResponse? = error
    }

    companion object {

        private val log = LoggerFactory.getLogger(JsonRestServer::class.java)

        private const val GRACEFUL_SHUTDOWN_TIMEOUT_MS = (10 * 1000).toLong()

        /**
         * Make an HTTP request.
         *
         * @param logger The logger to use.
         * @param url HTTP connection will be established with this url.
         * @param method HTTP method ("GET", "POST", "PUT", etc.)
         * @param requestBodyData Object to serialize as JSON and send in the request body.
         * @param responseFormat Expected format of the response to the HTTP request.
         * @param T The type of the deserialized response to the HTTP request.
         * @return The deserialized response to the HTTP request, or null if no data is expected.
         */
        @Throws(IOException::class)
        fun <T> httpRequest(
            logger: Logger,
            url: String,
            method: String,
            requestBodyData: Any?,
            responseFormat: TypeReference<T>,
        ): HttpResponse<T?> {
            var connection: HttpURLConnection? = null
            return try {
                val serializedBody =
                    if (requestBodyData == null) null
                    else JsonUtil.JSON_SERDE.writeValueAsString(requestBodyData)
                logger.debug("Sending {} with input {} to {}", method, serializedBody, url)
                connection = URL(url).openConnection() as HttpURLConnection
                connection.setRequestMethod(method)
                connection.setRequestProperty("User-Agent", "kafka")
                connection.setRequestProperty("Accept", "application/json")

                // connection.getResponseCode() implicitly calls getInputStream, so always set this to true.
                connection.setDoInput(true)
                connection.setUseCaches(false)

                if (requestBodyData != null) {
                    connection.setRequestProperty("Content-Type", "application/json")
                    connection.setDoOutput(true)
                    val os = connection.outputStream

                    os.write(serializedBody!!.toByteArray())
                    os.flush()
                    os.close()
                }
                when (val responseCode = connection.getResponseCode()) {
                    HttpURLConnection.HTTP_NO_CONTENT -> HttpResponse(
                        body = null,
                        error = ErrorResponse(responseCode, connection.getResponseMessage()),
                    )

                    in 200..299 -> {
                        val inputStream = connection.inputStream
                        val result = JsonUtil.JSON_SERDE.readValue(inputStream, responseFormat)
                        inputStream.close()
                        HttpResponse(body = result)
                    }
                    else -> {
                        // If the resposne code was not in the 200s, we assume that this is an error response.
                        // Also handle the case where HttpURLConnection#getErrorStream returns null.
                        val es = connection.errorStream ?: return HttpResponse(
                            body = null,
                            error = ErrorResponse(responseCode, ""),
                        )
                        // Try to read the error response JSON.
                        val error = JsonUtil.JSON_SERDE.readValue(es, ErrorResponse::class.java)
                        es.close()
                        HttpResponse(null, error)
                    }
                }
            } finally {
                connection?.disconnect()
            }
        }

        /**
         * Make an HTTP request with retries.
         *
         * @param url HTTP connection will be established with this url.
         * @param method HTTP method ("GET", "POST", "PUT", etc.)
         * @param requestBodyData Object to serialize as JSON and send in the request body.
         * @param responseFormat Expected format of the response to the HTTP request.
         * @param T The type of the deserialized response to the HTTP request.
         * @return The deserialized response to the HTTP request, or null if no data is expected.
         */
        @Throws(IOException::class, InterruptedException::class)
        fun <T> httpRequest(
            url: String,
            method: String,
            requestBodyData: Any?,
            responseFormat: TypeReference<T>,
            maxTries: Int,
        ): HttpResponse<T?> = httpRequest(
            logger = log,
            url = url,
            method = method,
            requestBodyData = requestBodyData,
            responseFormat = responseFormat,
            maxTries = maxTries
        )

        /**
         * Make an HTTP request with retries.
         *
         * @param logger The logger to use.
         * @param url HTTP connection will be established with this url.
         * @param method HTTP method ("GET", "POST", "PUT", etc.)
         * @param requestBodyData Object to serialize as JSON and send in the request body.
         * @param responseFormat Expected format of the response to the HTTP request.
         * @param T The type of the deserialized response to the HTTP request.
         * @return The deserialized response to the HTTP request, or null if no data is expected.
         */
        @Throws(IOException::class, InterruptedException::class)
        fun <T> httpRequest(
            logger: Logger,
            url: String,
            method: String,
            requestBodyData: Any?,
            responseFormat: TypeReference<T>,
            maxTries: Int,
        ): HttpResponse<T?> {
            lateinit var exc: IOException

            for (tries in 0..<maxTries) {
                if (tries > 0) Thread.sleep(if (tries > 1) 10 else 2)

                exc = try {
                    return httpRequest(
                        logger = logger,
                        url = url,
                        method = method,
                        requestBodyData = requestBodyData,
                        responseFormat = responseFormat,
                    )
                } catch (e: IOException) {
                    logger.info("{} {}: error: {}", method, url, e.message)
                    e
                }
            }
            throw exc
        }
    }
}
