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

package org.apache.kafka.message

import java.io.BufferedWriter
import java.io.IOException
import java.util.*
import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import org.apache.kafka.message.MessageGenerator.stripSuffix
import org.apache.kafka.message.MessageGenerator.toSnakeCase

class ApiMessageTypeGenerator(packageName: String) : TypeClassGenerator {

    private val headerGenerator: HeaderGenerator = HeaderGenerator(packageName)

    private val buffer: CodeBuffer = CodeBuffer()

    private val apis: TreeMap<Short, ApiData> = TreeMap()

    private val apisByListener = EnumMap<RequestListenerType, MutableList<ApiData?>>(
        RequestListenerType::class.java
    )

    private class ApiData(var apiKey: Short) {

        var requestSpec: MessageSpec? = null

        var responseSpec: MessageSpec? = null

        fun name(): String {
            return requestSpec?.let {
                stripSuffix(it.name, MessageGenerator.REQUEST_SUFFIX)
            } ?: responseSpec?.let {
                stripSuffix(it.name, MessageGenerator.RESPONSE_SUFFIX)
            } ?: throw RuntimeException(
                "Neither requestSpec nor responseSpec is defined for API key $apiKey"
            )
        }

        fun requestSchema(): String = requestSpec?.let { "${it.name}Data.SCHEMAS" }.toString()

        fun responseSchema(): String = responseSpec?.let { "${it.name}Data.SCHEMAS" }.toString()
    }

    override fun outputName(): String = MessageGenerator.API_MESSAGE_TYPE_KOTLIN

    override fun registerMessageType(spec: MessageSpec) {
        when (spec.type) {
            MessageSpecType.REQUEST -> {
                val apiKey = spec.apiKey!!
                val data = apis.computeIfAbsent(apiKey) { ApiData(apiKey) }
                if (data.requestSpec != null) throw RuntimeException(
                    "Found more than one request with API key ${spec.apiKey}"
                )
                data.requestSpec = spec
                val listeners = spec.listeners
                if (listeners != null) {
                    for (listener in listeners) {
                        apisByListener.putIfAbsent(listener, ArrayList())
                        apisByListener[listener]!!.add(data)
                    }
                }
            }

            MessageSpecType.RESPONSE -> {
                val apiKey = spec.apiKey!!
                val data = apis.computeIfAbsent(apiKey) { ApiData(apiKey) }
                if (data.responseSpec != null) throw RuntimeException(
                    "Found more than one response with API key ${spec.apiKey}"
                )
                data.responseSpec = spec
            }

            else -> Unit
        }
    }

    @Throws(IOException::class)
    override fun generateAndWrite(writer: BufferedWriter) {
        generate()
        write(writer)
    }

    private fun generate() {
        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS)
        headerGenerator.addImport(MessageGenerator.ENUM_SET_CLASS)
        generateEnumClassWithProperties()
        buffer.incrementIndent()
        generateEnumValues()
        buffer.printf("%n")
        generateFromApiKey()
        buffer.printf("%n")
        generateNewApiMessageMethod("request")
        buffer.printf("%n")
        generateNewApiMessageMethod("response")
        buffer.printf("%n")
        generateAccessor(name = "lowestSupportedVersion", type = "Short")
        buffer.printf("%n")
        generateHighestSupportedVersion()
        buffer.printf("%n")
        generateAccessor(name = "highestSupportedVersion", type = "Short")
        buffer.printf("%n")
        generateAccessor(name = "listeners", type = "EnumSet<ListenerType>")
        buffer.printf("%n")
        generateAccessor(name = "latestVersionUnstable", type = "Boolean")
        buffer.printf("%n")
        generateAccessor(name = "apiKey", type = "Short")
        buffer.printf("%n")
        generateAccessor(name = "requestSchemas", type = "Array<Schema?>")
        buffer.printf("%n")
        generateAccessor(name = "responseSchemas", type = "Array<Schema?>")
        buffer.printf("%n")
        generateToString()
        buffer.printf("%n")
        generateHeaderVersion("request")
        buffer.printf("%n")
        generateHeaderVersion("response")
        buffer.printf("%n")
        generateListenerTypesEnum()
        buffer.printf("%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        headerGenerator.generate()
    }

    private fun generateEnumClassWithProperties() {
        buffer.printf(
            """
            enum class ApiMessageType(
                private val altName: String,
                private val apiKey: Short,
                private val requestSchemas: Array<Schema?>,
                private val responseSchemas: Array<Schema?>,
                private val lowestSupportedVersion: Short,
                private val highestSupportedVersion: Short,
                private val listeners: EnumSet<ListenerType>,
                private val latestVersionUnstable: Boolean,
            ) {%n
        """.trimIndent(),
        )
    }

    private fun generateListenerTypeEnumSet(values: Collection<String>): String {
        if (values.isEmpty()) return "EnumSet.noneOf(ListenerType::class.java)"

        val bldr = StringBuilder("EnumSet.of(")
        val iter = values.iterator()
        while (iter.hasNext()) {
            bldr.append("ListenerType.")
            bldr.append(iter.next())
            if (iter.hasNext()) bldr.append(", ")
        }
        bldr.append(")")
        return bldr.toString()
    }

    private fun generateEnumValues() {
        var numProcessed = 0
        for ((key, apiData) in apis) {
            val name = apiData.name()
            numProcessed++
            val requestSpec = apiData.requestSpec!!
            val listeners = requestSpec.listeners?.map(RequestListenerType::name) ?: emptyList()

            buffer.printf(
                "%s(\"%s\", %d.toShort(), %s, %s, %d.toShort(), %d.toShort(), %s, %s)%s%n",
                toSnakeCase(name).uppercase(),
                capitalizeFirst(name),
                key,
                apiData.requestSchema(),
                apiData.responseSchema(),
                requestSpec.struct.versions.lowest,
                requestSpec.struct.versions.highest,
                generateListenerTypeEnumSet(listeners),
                requestSpec.latestVersionUnstable,
                if (numProcessed != apis.size) "," else ";"
            )
        }
    }

    private fun generateFromApiKey() {
        buffer.printf("companion object {%n")
        buffer.incrementIndent()
        buffer.printf("fun fromApiKey(apiKey: Short): ApiMessageType {%n")
        buffer.incrementIndent()
        buffer.printf("return when (apiKey.toInt()) {%n")
        buffer.incrementIndent()
        for ((key, apiData) in apis)
            buffer.printf("%d -> %s%n", key, toSnakeCase(apiData.name()).uppercase())

        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("else -> throw UnsupportedVersionException(\"Unsupported API key \$apiKey\")%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")

        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateNewApiMessageMethod(type: String) {
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        buffer.printf("fun new%s(): ApiMessage {%n", capitalizeFirst(type))
        buffer.incrementIndent()
        buffer.printf("return when (apiKey.toInt()) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis.entries)
            buffer.printf(
                "%d -> %s%sData()%n",
                key,
                value.name(),
                capitalizeFirst(type),
            )

        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "else -> throw UnsupportedVersionException(\"Unsupported %s API key \$apiKey\")%n",
            type,
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateAccessor(name: String, type: String) {
        buffer.printf("fun %s(): %s {%n", name, type)
        buffer.incrementIndent()
        buffer.printf("return this.%s%n", name)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateToString() {
        buffer.printf("override fun toString(): String {%n")
        buffer.incrementIndent()
        buffer.printf("return this.altName%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHeaderVersion(type: String) {
        buffer.printf("fun %sHeaderVersion(version: Short): Short {%n", type)
        buffer.incrementIndent()
        buffer.printf("return when (apiKey.toInt()) {%n")
        buffer.incrementIndent()
        for ((apiKey, apiData) in apis) {
            val name = apiData.name()
            buffer.printf("%d -> // %s%n", apiKey, capitalizeFirst(name))
            buffer.incrementIndent()
            if (type == "response" && apiKey.toInt() == 18) {
                buffer.printf("// ApiVersionsResponse always includes a v0 header.%n")
                buffer.printf("// See KIP-511 for details.%n")
                buffer.printf("0%n")
                buffer.decrementIndent()
                continue
            }
            if (type == "request" && apiKey.toInt() == 7) {
                buffer.printf("// Version 0 of ControlledShutdownRequest has a non-standard request header%n")
                buffer.printf("// which does not include clientId. Version 1 of ControlledShutdownRequest%n")
                buffer.printf("// and later use the standard request header.%n")
                buffer.printf("if (version == 0.toShort()) 0%n")
                buffer.printf("else {%n")
                buffer.incrementIndent()
            }
            val spec = when (type) {
                "request" -> apiData.requestSpec
                "response" -> apiData.responseSpec
                else -> throw RuntimeException("Invalid type $type for generateHeaderVersion")
            } ?: throw RuntimeException("failed to find $type for API key $apiKey")

            VersionConditional.forVersions(
                containingVersions = spec.flexibleVersions,
                possibleVersions = spec.validVersions,
            ).ifMember {
                if (type == "request") buffer.printf("2%n")
                else buffer.printf("1%n")
            }.ifNotMember {
                if ((type == "request")) buffer.printf("1%n")
                else buffer.printf("0%n")
            }.generate(buffer)

            if (type == "request" && apiKey.toInt() == 7) {
                buffer.decrementIndent()
                buffer.printf("}%n")
            }
            buffer.decrementIndent()
        }
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("else -> throw UnsupportedVersionException(\"Unsupported API key \$apiKey\")%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateListenerTypesEnum() {
        buffer.printf("enum class ListenerType {%n")
        buffer.incrementIndent()
        val listenerIter = RequestListenerType.values().iterator()
        while (listenerIter.hasNext()) {
            val scope = listenerIter.next()
            buffer.printf("%s%s%n", scope.name, if (listenerIter.hasNext()) "," else "")
        }
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHighestSupportedVersion() {
        buffer.printf("fun highestSupportedVersion(enableUnstableLastVersion: Boolean): Short {%n")
        buffer.incrementIndent()
        buffer.printf("if (!this.latestVersionUnstable() || enableUnstableLastVersion) {%n")
        buffer.incrementIndent()
        buffer.printf("return this.highestSupportedVersion%n")
        buffer.decrementIndent()
        buffer.printf("} else {%n")
        buffer.incrementIndent()
        buffer.printf("// A negative value means that the API has no enabled versions.%n")
        buffer.printf("return (this.highestSupportedVersion - 1).toShort()%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    @Throws(IOException::class)
    private fun write(writer: BufferedWriter) {
        headerGenerator.buffer.write(writer)
        buffer.write(writer)
    }
}
