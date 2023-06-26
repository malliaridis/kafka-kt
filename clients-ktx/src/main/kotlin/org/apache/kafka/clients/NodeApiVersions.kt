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

package org.apache.kafka.clients

import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.feature.SupportedVersionRange
import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.common.utils.Utils.min
import java.util.*

/**
 * An internal class which represents the API versions supported by a particular node.
 */
class NodeApiVersions(
    nodeApiVersions: Collection<ApiVersionsResponseData.ApiVersion>,
    nodeSupportedFeatures: Collection<SupportedFeatureKey>
) {
    // A map of the usable versions of each API, keyed by the ApiKeys instance
    val supportedVersions: MutableMap<ApiKeys, ApiVersionsResponseData.ApiVersion> =
        EnumMap(ApiKeys::class.java)

    // List of APIs which the broker supports, but which are unknown to the client
    private val unknownApis: MutableList<ApiVersionsResponseData.ApiVersion> = ArrayList()

    val supportedFeatures: Map<String, SupportedVersionRange>

    init {
        for (nodeApiVersion in nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey().toInt())) {
                val nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey().toInt())
                supportedVersions[nodeApiKey] = nodeApiVersion
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(nodeApiVersion)
            }
        }
        supportedFeatures = nodeSupportedFeatures.associate { supportedFeature ->
            supportedFeature.name() to SupportedVersionRange(
                minVersion = supportedFeature.minVersion(),
                maxVersion = supportedFeature.maxVersion(),
            )
        }
    }

    /**
     * Get the latest version supported by the broker within an allowed range of versions, defaults
     * to the most recent version supported by both the node and the local software if versions
     * not provided.
     */
    fun latestUsableVersion(
        apiKey: ApiKeys,
        oldestAllowedVersion: Short = apiKey.oldestVersion(),
        latestAllowedVersion: Short = apiKey.latestVersion(),
    ): Short {
        if (!supportedVersions.containsKey(apiKey))
            throw UnsupportedVersionException("The broker does not support $apiKey")

        val supportedVersion = supportedVersions[apiKey]
        val intersectVersion: ApiVersionsResponseData.ApiVersion? =
            ApiVersionsResponse.intersect(
                supportedVersion,
                ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion(oldestAllowedVersion)
                    .setMaxVersion(latestAllowedVersion)
            )
        return intersectVersion?.maxVersion()
            ?: throw UnsupportedVersionException(
                "The broker does not support $apiKey with version in range " +
                        "[$oldestAllowedVersion,$latestAllowedVersion]. " +
                        "The supported range is [${supportedVersion!!.minVersion()}," +
                        "${supportedVersion.maxVersion()}]."
            )
    }

    /**
     * Convert the object to a string with no linebreaks.
     *
     * This toString method is relatively expensive, so avoid calling it unless debug logging is
     * turned on.
     */
    override fun toString(): String = toString(false)

    /**
     * Convert the object to a string.
     *
     * @param lineBreaks True if we should add a linebreak after each api.
     */
    fun toString(lineBreaks: Boolean): String {
        // The apiVersion collection may not be in sorted order. We put it into a TreeMap before
        // printing it out to ensure that we always print in ascending order.
        val apiKeysText = (supportedVersions.values.associate { supportedVersion ->
            supportedVersion.apiKey() to apiVersionToText(supportedVersion)
        } + unknownApis.associate { apiVersion ->
            apiVersion.apiKey() to apiVersionToText(apiVersion)
        }).toSortedMap()

        // Also handle the case where some apiKey types are not specified at all in the given
        // ApiVersions, which may happen when the remote is too old.
        for (apiKey in ApiKeys.zkBrokerApis()) {
            if (!apiKeysText.containsKey(apiKey.id)) {
                val bld = StringBuilder()
                bld.append(apiKey.name)
                    .append("(")
                    .append(apiKey.id.toInt())
                    .append("): ")
                    .append("UNSUPPORTED")

                apiKeysText[apiKey.id] = bld.toString()
            }
        }
        val separator = if (lineBreaks) ",\n\t" else ", "
        val bld = StringBuilder()
        bld.append("(")
        if (lineBreaks) bld.append("\n\t")
        bld.append(apiKeysText.values.joinToString(separator))
        if (lineBreaks) bld.append("\n")
        bld.append(")")

        return bld.toString()
    }

    private fun apiVersionToText(apiVersion: ApiVersionsResponseData.ApiVersion): String {
        val bld = StringBuilder()
        var apiKey: ApiKeys? = null
        if (ApiKeys.hasId(apiVersion.apiKey().toInt())) {
            apiKey = ApiKeys.forId(apiVersion.apiKey().toInt())
            bld.append(apiKey.name)
                .append("(")
                .append(apiKey.id.toInt())
                .append("): ")
        } else bld.append("UNKNOWN(")
            .append(apiVersion.apiKey().toInt())
            .append("): ")

        if (apiVersion.minVersion() == apiVersion.maxVersion())
            bld.append(apiVersion.minVersion().toInt())
        else bld.append(apiVersion.minVersion().toInt())
            .append(" to ")
            .append(apiVersion.maxVersion().toInt())

        if (apiKey != null) {
            val supportedVersion = supportedVersions[apiKey]
            if (apiKey.latestVersion() < supportedVersion!!.minVersion())
                bld.append(" [unusable: node too new]")
            else if (supportedVersion.maxVersion() < apiKey.oldestVersion())
                bld.append(" [unusable: node too old]")
            else {
                val latestUsableVersion = minOf(
                    apiKey.latestVersion().toInt(),
                    supportedVersion.maxVersion().toInt(),
                )
                bld.append(" [usable: ")
                    .append(latestUsableVersion)
                    .append("]")
            }
        }
        return bld.toString()
    }

    /**
     * Get the version information for a given API.
     *
     * @param apiKey The api key to lookup
     * @return The api version information from the broker or null if it is unsupported
     */
    fun apiVersion(apiKey: ApiKeys): ApiVersionsResponseData.ApiVersion? = supportedVersions[apiKey]

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("supportedVersions"),
    )
    fun allSupportedApiVersions(): Map<ApiKeys, ApiVersionsResponseData.ApiVersion> =
        supportedVersions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("supportedFeatures"),
    )
    fun supportedFeatures(): Map<String, SupportedVersionRange> = supportedFeatures

    companion object {

        /**
         * Create a NodeApiVersions object with the current ApiVersions.
         *
         * @param overrides API versions to override. Any ApiVersion not specified here will be set
         * to the current client value.
         * @return A new NodeApiVersions object.
         */
        fun create(
            overrides: Collection<ApiVersionsResponseData.ApiVersion> = emptyList()
        ): NodeApiVersions {
            val apiVersions = LinkedList(overrides)
            for (apiKey in ApiKeys.zkBrokerApis()) {
                var exists = false
                for (apiVersion in apiVersions) {
                    if (apiVersion.apiKey() == apiKey.id) {
                        exists = true
                        break
                    }
                }
                if (!exists) apiVersions.add(ApiVersionsResponse.toApiVersion(apiKey))
            }
            return NodeApiVersions(apiVersions, emptyList())
        }

        /**
         * Create a NodeApiVersions object with a single ApiKey. It is mainly used in tests.
         *
         * @param apiKey ApiKey's id.
         * @param minVersion ApiKey's minimum version.
         * @param maxVersion ApiKey's maximum version.
         * @return A new NodeApiVersions object.
         */
        fun create(apiKey: Short, minVersion: Short, maxVersion: Short): NodeApiVersions = create(
            setOf(
                ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey)
                    .setMinVersion(minVersion)
                    .setMaxVersion(maxVersion)
            )
        )
    }
}
