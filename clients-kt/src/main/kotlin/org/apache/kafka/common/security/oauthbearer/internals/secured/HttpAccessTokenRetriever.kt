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
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.UnsupportedEncodingException
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.ExecutionException
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLSocketFactory
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
import org.slf4j.LoggerFactory

/**
 * `HttpAccessTokenRetriever` is an [AccessTokenRetriever] that will communicate with an OAuth/OIDC
 * provider directly via HTTP to post client credentials
 * ([OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG]/[OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG])
 * to a publicized token endpoint URL ([SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL]).
 *
 * @see AccessTokenRetriever
 * @see OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG
 * @see OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG
 * @see OAuthBearerLoginCallbackHandler.SCOPE_CONFIG
 * @see SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 */
class HttpAccessTokenRetriever(
    private val clientId: String,
    private val clientSecret: String,
    private val scope: String?,
    private val sslSocketFactory: SSLSocketFactory?,
    private val tokenEndpointUrl: String,
    private val loginRetryBackoffMs: Long,
    private val loginRetryBackoffMaxMs: Long,
    private val loginConnectTimeoutMs: Int? = null,
    private val loginReadTimeoutMs: Int? = null
) : AccessTokenRetriever {

    /**
     * Retrieves a JWT access token in its serialized three-part form. The implementation is free to
     * determine how it should be retrieved but should not perform validation on the result.
     *
     * **Note**: This is a blocking function and callers should be aware that the implementation
     * communicates over a network. The facility in the [javax.security.auth.spi.LoginModule] from
     * which this is ultimately called does not provide an asynchronous approach.
     *
     * @return JWT access token string
     * @throws IOException Thrown on errors related to IO during retrieval
     */
    @Throws(IOException::class)
    override fun retrieve(): String {
        val authorizationHeader = formatAuthorizationHeader(clientId, clientSecret)
        val requestBody = formatRequestBody(scope)
        val retry: Retry<String> = Retry(
            retryBackoffMs = loginRetryBackoffMs,
            retryBackoffMaxMs = loginRetryBackoffMaxMs,
        )

        val headers = mapOf(AUTHORIZATION_HEADER to authorizationHeader)
        val responseBody: String
        try {
            responseBody = retry.execute {
                var connection: HttpURLConnection? = null
                try {
                    connection = URL(tokenEndpointUrl).openConnection() as HttpURLConnection
                    if (sslSocketFactory != null && connection is HttpsURLConnection)
                        connection.sslSocketFactory = sslSocketFactory

                    return@execute post(
                        connection = connection,
                        headers = headers,
                        requestBody = requestBody,
                        connectTimeoutMs = loginConnectTimeoutMs,
                        readTimeoutMs = loginReadTimeoutMs,
                    )
                } catch (e: IOException) {
                    throw ExecutionException(e)
                } finally {
                    connection?.disconnect()
                }
            }
        } catch (e: ExecutionException) {
            throw e.cause as? IOException ?: KafkaException(cause = e.cause)
        }

        return parseAccessToken(responseBody)
    }

    companion object {

        private val log = LoggerFactory.getLogger(HttpAccessTokenRetriever::class.java)

        // This does not have to be an exhaustive list. There are other HTTP codes that are defined
        // in different RFCs (e.g. https://datatracker.ietf.org/doc/html/rfc6585) that we won't
        // worry about yet. The worst case if a status code is missing from this set is that the
        // request will be retried.
        private val UNRETRYABLE_HTTP_CODES = setOf(
            HttpURLConnection.HTTP_BAD_REQUEST,
            HttpURLConnection.HTTP_UNAUTHORIZED,
            HttpURLConnection.HTTP_PAYMENT_REQUIRED,
            HttpURLConnection.HTTP_FORBIDDEN,
            HttpURLConnection.HTTP_NOT_FOUND,
            HttpURLConnection.HTTP_BAD_METHOD,
            HttpURLConnection.HTTP_NOT_ACCEPTABLE,
            HttpURLConnection.HTTP_PROXY_AUTH,
            HttpURLConnection.HTTP_CONFLICT,
            HttpURLConnection.HTTP_GONE,
            HttpURLConnection.HTTP_LENGTH_REQUIRED,
            HttpURLConnection.HTTP_PRECON_FAILED,
            HttpURLConnection.HTTP_ENTITY_TOO_LARGE,
            HttpURLConnection.HTTP_REQ_TOO_LONG,
            HttpURLConnection.HTTP_UNSUPPORTED_TYPE,
            HttpURLConnection.HTTP_NOT_IMPLEMENTED,
            HttpURLConnection.HTTP_VERSION,
        )

        private const val MAX_RESPONSE_BODY_LENGTH = 1000

        const val AUTHORIZATION_HEADER = "Authorization"

        @Throws(IOException::class, UnretryableException::class)
        fun post(
            connection: HttpURLConnection,
            headers: Map<String, String>?,
            requestBody: String?,
            connectTimeoutMs: Int?,
            readTimeoutMs: Int?
        ): String {
            handleInput(
                connection = connection,
                headers = headers,
                requestBody = requestBody,
                connectTimeoutMs = connectTimeoutMs,
                readTimeoutMs = readTimeoutMs,
            )
            return handleOutput(connection)
        }

        @Throws(IOException::class, UnretryableException::class)
        private fun handleInput(
            connection: HttpURLConnection,
            headers: Map<String, String>?,
            requestBody: String?,
            connectTimeoutMs: Int?,
            readTimeoutMs: Int?
        ) {
            log.debug("handleInput - starting post for {}", connection.url)

            connection.apply {
                requestMethod = "POST"
                setRequestProperty("Accept", "application/json")
                headers?.forEach { (key, value) -> setRequestProperty(key, value) }
                setRequestProperty("Cache-Control", "no-cache")
                requestBody?.let {
                    setRequestProperty("Content-Length", it.length.toString())
                    doOutput = true
                }
                useCaches = false
                connectTimeoutMs?.let { connectTimeout = it }
                readTimeoutMs?.let { readTimeout = it }
            }

            log.debug("handleInput - preparing to connect to {}", connection.url)
            connection.connect()

            if (requestBody != null) {
                connection.outputStream.use { output ->
                    val input =
                        ByteArrayInputStream(requestBody.toByteArray(StandardCharsets.UTF_8))

                    log.debug("handleInput - preparing to write request body to {}", connection.url)
                    copy(input, output)
                }
            }
        }

        @Throws(IOException::class)
        fun handleOutput(connection: HttpURLConnection): String {
            val responseCode = connection.responseCode
            log.debug("handleOutput - responseCode: {}", responseCode)

            // NOTE: the contents of the response should not be logged so that we don't leak any
            // sensitive data.
            var responseBody: String? = null

            // NOTE: It is OK to log the error response body and/or its formatted version as per the
            // OAuth spec, it doesn't include sensitive information.
            // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2
            var errorResponseBody: String? = null
            try {
                connection.inputStream.use { input ->
                    val output = ByteArrayOutputStream()
                    log.debug(
                        "handleOutput - preparing to read response body from {}",
                        connection.url
                    )
                    copy(input, output)
                    responseBody = output.toString(StandardCharsets.UTF_8.name())
                }
            } catch (e: Exception) {
                // there still can be useful error response from the servers, lets get it
                try {
                    connection.errorStream.use { input ->
                        val output = ByteArrayOutputStream()
                        log.debug(
                            "handleOutput - preparing to read error response body from {}",
                            connection.url
                        )

                        copy(input, output)
                        errorResponseBody = output.toString(StandardCharsets.UTF_8.name())
                    }
                } catch (e2: Exception) {
                    log.warn("handleOutput - error retrieving error information", e2)
                }
                log.warn("handleOutput - error retrieving data", e)
            }
            return when (responseCode) {
                HttpURLConnection.HTTP_OK,
                HttpURLConnection.HTTP_CREATED -> {
                    log.debug(
                        "handleOutput - responseCode: {}, error response: {}",
                        responseCode,
                        errorResponseBody
                    )
                    if (responseBody.isNullOrEmpty()) throw IOException(
                        String.format(
                            "The token endpoint response was unexpectedly empty despite response " +
                                    "code %s from %s and error message %s",
                            responseCode,
                            connection.url,
                            formatErrorMessage(errorResponseBody),
                        )
                    )
                    responseBody as String
                }
                else -> {
                    log.warn(
                        "handleOutput - error response code: {}, error response body: {}",
                        responseCode,
                        formatErrorMessage(errorResponseBody),
                    )
                    if (UNRETRYABLE_HTTP_CODES.contains(responseCode)) {
                        // We know that this is a non-transient error, so let's not keep retrying
                        // the request unnecessarily.
                        throw UnretryableException(
                            cause = IOException(
                                String.format(
                                    "The response code %s and error response %s was encountered " +
                                            "reading the token endpoint response; will not " +
                                            "attempt further retries",
                                    responseCode,
                                    formatErrorMessage(errorResponseBody),
                                )
                            )
                        )
                    // We don't know if this is a transient (retryable) error or not, so let's
                    // assume it is.
                    } else throw IOException(
                        String.format(
                            "The unexpected response code %s and error message %s was " +
                                    "encountered reading the token endpoint response",
                            responseCode,
                            formatErrorMessage(errorResponseBody)
                        )
                    )
                }
            }
        }

        @Throws(IOException::class)
        fun copy(input: InputStream, output: OutputStream) {
            val buffer = ByteArray(4096)
            var b: Int
            while (input.read(buffer).also { b = it } != -1) output.write(buffer, 0, b)
        }

        fun formatErrorMessage(errorResponseBody: String?): String {
            // See https://www.ietf.org/rfc/rfc6749.txt, section 5.2 for the format of this error
            // message.
            if (errorResponseBody == null || errorResponseBody.trim { it <= ' ' } == "") return "{}"

            val mapper = ObjectMapper()
            try {
                val rootNode = mapper.readTree(errorResponseBody)
                return if (!rootNode.at("/error").isMissingNode) {
                    String.format(
                        "{%s - %s}",
                        rootNode.at("/error"),
                        rootNode.at("/error_description")
                    )
                } else if (!rootNode.at("/errorCode").isMissingNode) {
                    String.format(
                        "{%s - %s}",
                        rootNode.at("/errorCode"),
                        rootNode.at("/errorSummary")
                    )
                } else errorResponseBody
            } catch (e: Exception) {
                log.warn("Error parsing error response", e)
            }
            return String.format("{%s}", errorResponseBody)
        }

        @Throws(IOException::class)
        fun parseAccessToken(responseBody: String): String {
            val mapper = ObjectMapper()
            val rootNode = mapper.readTree(responseBody)

            val accessTokenNode = rootNode.at("/access_token")
            if (accessTokenNode == null) {
                // Only grab the first N characters so that if the response body is huge, we don't
                // blow up.
                var snippet = responseBody
                if (snippet.length > MAX_RESPONSE_BODY_LENGTH) {
                    val actualLength = responseBody.length
                    val s = responseBody.substring(0, MAX_RESPONSE_BODY_LENGTH)
                    snippet = String.format(
                        "%s (trimmed to first %s characters out of %s total)",
                        s,
                        MAX_RESPONSE_BODY_LENGTH,
                        actualLength
                    )
                }
                throw IOException(
                    String.format(
                        "The token endpoint response did not contain an access_token value. " +
                                "Response: (%s)",
                        snippet
                    )
                )
            }
            return sanitizeString(
                "the token endpoint response's access_token JSON attribute",
                accessTokenNode.textValue(),
            )
        }

        fun formatAuthorizationHeader(clientId: String, clientSecret: String): String {
            val sanitizedClientId = sanitizeString(
                name = "the token endpoint request client ID parameter",
                value = clientId,
            )

            val sanitizedClientSecret = sanitizeString(
                name = "the token endpoint request client secret parameter",
                value = clientSecret,
            )

            val idAndSecret = String.format("%s:%s", sanitizedClientId, sanitizedClientSecret)

            // Per RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
            val encoded = Base64.getEncoder().encodeToString(idAndSecret.toByteArray())
            return String.format("Basic %s", encoded)
        }

        @Throws(IOException::class)
        fun formatRequestBody(scope: String?): String {
            val trimmed = scope?.trim { it <= ' ' }

            return try {
                val requestParameters = StringBuilder()
                requestParameters.append("grant_type=client_credentials")
                if (!trimmed.isNullOrEmpty()) {
                    val encodedScope = URLEncoder.encode(trimmed, StandardCharsets.UTF_8.name())
                    requestParameters.append("&scope=").append(encodedScope)
                }
                requestParameters.toString()
            } catch (e: UnsupportedEncodingException) {
                // The world has gone crazy!
                throw IOException(
                    String.format("Encoding %s not supported", StandardCharsets.UTF_8.name())
                )
            }
        }

        private fun sanitizeString(name: String, value: String): String {
            require(value.isNotEmpty()) {
                String.format("The value for %s must be non-empty", name)
            }

            val sanitized = value.trim { it <= ' ' }
            require(sanitized.isNotEmpty()) {
                String.format("The value for %s must not contain only whitespace", name)
            }

            return sanitized
        }
    }
}
