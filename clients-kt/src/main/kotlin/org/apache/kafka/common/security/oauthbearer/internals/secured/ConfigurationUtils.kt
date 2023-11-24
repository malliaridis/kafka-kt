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

import java.io.File
import java.net.MalformedURLException
import java.net.URISyntaxException
import java.net.URL
import java.nio.file.Path
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.ListenerName

/**
 * `ConfigurationUtils` is a utility class to perform basic configuration-related logic and is
 * separated out here for easier, more direct testing.
 */
class ConfigurationUtils(
    private val configs: Map<String, *>,
    saslMechanism: String? = null,
) {
    private var prefix: String? = null

    init {
        val trimmed = saslMechanism?.trim { it <= ' ' }
        prefix =
            if (!trimmed.isNullOrEmpty()) ListenerName.saslMechanismPrefix(trimmed)
            else null
    }

    /**
     * Validates that, if a value is supplied, is a file that:
     *
     * - exists
     * - has read permission
     * - points to a file
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus.
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */
    fun validateFile(name: String): Path {
        val url = validateUrl(name)

        val file: File = try {
            File(url.toURI().rawPath).absoluteFile
        } catch (e: URISyntaxException) {
            throw ConfigException(
                name = name,
                value = url.toString(),
                message = "The OAuth configuration option $name contains a URL ($url) that is malformed: ${e.message}"
            )
        }

        if (!file.exists()) throw ConfigException(
            name = name,
            value = file,
            message = "The OAuth configuration option $name contains a file ($file) that doesn't exist"
        )

        if (!file.canRead()) throw ConfigException(
            name = name,
            value = file,
            message = String.format(
                "The OAuth configuration option %s contains a file (%s) that doesn't have read " +
                        "permission",
                name,
                file
            )
        )

        if (file.isDirectory) throw ConfigException(
            name = name,
            value = file,
            message = "The OAuth configuration option $name references a directory ($file), not a file"
        )
        return file.toPath()
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * - is an Integer
     * - has a value that is not less than the provided minimum value
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */
    @Deprecated("Use validateRequiredInt or validateOptionalInt instead")
    fun validateInteger(name: String, isRequired: Boolean): Int? {
        return get<Int>(name)
            ?: if (isRequired) throw ConfigException(
                name = name,
                message = "The OAuth configuration option $name must be non-null"
            ) else null
    }

    /**
     * Validates that value:
     *
     * - is supplied
     * - is an Int
     * - has a value that is not less than the provided minimum value (if provided)
     *
     * @param name The name / key of the value to get.
     * @param min The minimum the value is allowed to be
     * @return The value that is equal to or greater than [min].
     * @throws ConfigException If any of the conditions are not met.
     */
    fun validateRequiredInt(name: String, min: Int? = null): Int {
        val value = get<Int>(name) ?: throw ConfigException(
            name = name,
            message = "The OAuth configuration option $name must be non-null"
        )

        if (min != null && value < min) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name value must be at least $min"
        )
        return value
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * - is an Int
     * - has a value that is not less than the provided minimum value (if provided)
     *
     * @param name The name / key of the value to get.
     * @param min The minimum the value is allowed to be
     * @return The value that is equal to or greater than [min], or `null` if the value is not
     * supplied.
     * @return The value that is equal to or greater than [min], or `null` if the value is not
     * supplied. If the value is `null` or an empty string, it is assumed to be an "empty" value and
     * thus ignored. Any whitespace is trimmed off of the beginning and end.
     * @throws ConfigException If any of the conditions are not met.
     */
    fun validateOptionalInt(name: String, min: Int? = null): Int? {
        val value = get<Int>(name) ?: return null

        if (min != null && value < min) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name value must be at least $min"
        )
        return value
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * - is an Integer
     * - has a value that is not less than the provided minimum value
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */
    @Deprecated("Use validateRequiredLong or validateOptionalLong instead")
    fun validateLong(name: String, isRequired: Boolean = true, min: Long? = null): Long? {
        val value = get<Long>(name)
            ?: return if (isRequired) throw ConfigException(
                name = name,
                message = "The OAuth configuration option $name must be non-null"
            ) else null

        if (min != null && value < min) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name value must be at least $min"
        )
        return value
    }

    /**
     * Validates that value:
     *
     * - is supplied
     * - is a Long
     * - has a value that is not less than the provided minimum value (if provided)
     *
     * @param name The name / key of the value to get.
     * @param min The minimum the value is allowed to be
     * @return The value that is equal to or greater than [min].
     * @throws ConfigException If any of the conditions are not met.
     */
    fun validateRequiredLong(name: String, min: Long? = null): Long {
        val value = get<Long>(name) ?: throw ConfigException(
            name = name,
            message = "The OAuth configuration option $name must be non-null"
        )

        if (min != null && value < min) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name value must be at least $min"
        )
        return value
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * - is a Long
     * - has a value that is not less than the provided minimum value (if provided)
     *
     * @param name The name / key of the value to get.
     * @param min The minimum the value is allowed to be
     * @return The value that is equal to or greater than [min], or `null` if the value is not
     * supplied.
     * @return The value that is equal to or greater than [min], or `null` if the value is not
     * supplied. If the value is `null` or an empty string, it is assumed to be an "empty" value and
     * thus ignored. Any whitespace is trimmed off of the beginning and end.
     * @throws ConfigException If any of the conditions are not met.
     */
    fun validateOptionalLong(name: String, min: Long? = null): Long? {
        val value = get<Long>(name) ?: return null

        if (min != null && value < min) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name value must be at least $min"
        )
        return value
    }

    /**
     * Validates that the configured URL that:
     *
     * - is well-formed
     * - contains a scheme
     * - uses either HTTP, HTTPS, or file protocols
     *
     * No effort is made to connect to the URL in the validation step.
     */
    fun validateUrl(name: String): URL {
        val value = validateString(name)
        val url: URL = try {
            URL(value)
        } catch (e: MalformedURLException) {
            throw ConfigException(
                name = name,
                value = value,
                message = "The OAuth configuration option $name contains a URL ($value) that is malformed: ${e.message}"
            )
        }

        var protocol = url.protocol
        if (protocol == null || protocol.trim { it <= ' ' }.isEmpty()) throw ConfigException(
            name = name,
            value = value,
            message = "The OAuth configuration option $name contains a URL ($value) that is missing the protocol"
        )

        protocol = protocol.lowercase()
        if (!(protocol == "http" || protocol == "https" || protocol == "file")) throw ConfigException(
            name = name,
            value = value,
            message = String.format(
                "The OAuth configuration option %s contains a URL (%s) that contains an invalid " +
                        "protocol (%s); only \"http\", \"https\", and \"file\" protocol are supported",
                name,
                value,
                protocol
            )
        )
        return url
    }

    @Throws(ValidateException::class)
    @Deprecated("Use validateRequiredString or validateOptionalString instead")
    fun validateString(name: String, isRequired: Boolean = true): String? {
        var value = get<String>(name)
            ?: return if (isRequired) throw ConfigException(
                message = String.format(
                    "The OAuth configuration option %s value must be non-null",
                    name
                )
            ) else null

        value = value.trim { it <= ' ' }
        return value.ifEmpty {
            if (isRequired) throw ConfigException(
                message = String.format(
                    "The OAuth configuration option %s value must not contain only whitespace",
                    name
                )
            ) else null
        }
    }

    @Throws(ValidateException::class)
    fun validateRequiredString(name: String): String {
        var value = get<String>(name) ?: throw ConfigException(
            String.format("The OAuth configuration option %s value must be non-null", name)
        )

        value = value.trim { it <= ' ' }
        return value.ifEmpty {
            throw ConfigException(
                message = String.format(
                    "The OAuth configuration option %s value must not contain only whitespace",
                    name
                )
            )
        }
    }

    @Throws(ValidateException::class)
    fun validateOptionalString(name: String): String? {
        return get<String>(name)
            ?.trim { it <= ' ' }
            ?.ifEmpty { null }
    }

    operator fun <T> get(name: String): T? {
        val value = configs[prefix + name] as? T
        return value ?: configs[name] as? T
    }
}
