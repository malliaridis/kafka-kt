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
import java.util.TreeMap
import org.apache.kafka.message.MessageGenerator.capitalizeFirst

class MetadataJsonConvertersGenerator(packageName: String) : TypeClassGenerator {

    private val headerGenerator = HeaderGenerator(packageName)

    private val buffer = CodeBuffer()

    private val apis = TreeMap<Short, MessageSpec>()

    override fun outputName(): String = MessageGenerator.METADATA_JSON_CONVERTERS_KOTLIN

    override fun registerMessageType(spec: MessageSpec) {
        if (spec.type != MessageSpecType.METADATA) return

        val id: Short = spec.apiKey!!
        val prevSpec = apis.put(id, spec)
        if (prevSpec != null) throw RuntimeException(
            "Duplicate metadata record entry for type $id. Original claimant: ${prevSpec.name}. " +
                    "New claimant: ${spec.name}"
        )
    }

    @Throws(IOException::class)
    override fun generateAndWrite(writer: BufferedWriter) {
        buffer.printf("object MetadataJsonConverters {%n")
        buffer.incrementIndent()
        generateWriteJson()
        buffer.printf("%n")
        generateReadJson()
        buffer.printf("%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        headerGenerator.generate()
        headerGenerator.buffer.write(writer)
        buffer.write(writer)
    }

    private fun generateWriteJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        buffer.printf("@JvmStatic%n")
        buffer.printf("fun writeJson(apiMessage: ApiMessage, apiVersion: Short): JsonNode {%n")
        buffer.incrementIndent()
        buffer.printf("return when (apiMessage.apiKey().toInt()) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis) {
            val apiMessageClassName = capitalizeFirst(value.name)
            buffer.printf(
                "%d -> %sJsonConverter.write(apiMessage as %s, apiVersion)%n",
                key,
                apiMessageClassName,
                apiMessageClassName,
            )
        }
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("else -> throw UnsupportedVersionException(\"Unknown metadata id \${apiMessage.apiKey()}\")%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateReadJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        buffer.printf("@JvmStatic%n")
        buffer.printf(
            "fun readJson(json: JsonNode, apiKey: Short, apiVersion: Short): ApiMessage {%n"
        )
        buffer.incrementIndent()
        buffer.printf("return when (apiKey.toInt()) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis.entries)
            buffer.printf(
                "%d -> %sJsonConverter.read(json, apiVersion)%n",
                key,
                capitalizeFirst(value.name),
            )
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("else -> throw UnsupportedVersionException(\"Unknown metadata id \$apiKey\")%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }
}
