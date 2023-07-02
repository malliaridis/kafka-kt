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

import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import java.io.BufferedWriter
import java.io.IOException
import java.util.*

class MetadataJsonConvertersGenerator(packageName: String) : TypeClassGenerator {

    private val headerGenerator = HeaderGenerator(packageName)

    private val buffer = CodeBuffer()

    private val apis = TreeMap<Short, MessageSpec>()

    override fun outputName(): String = MessageGenerator.METADATA_JSON_CONVERTERS_JAVA

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
        buffer.printf("public class MetadataJsonConverters {%n")
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
        buffer.printf("public static JsonNode writeJson(ApiMessage apiMessage, short apiVersion) {%n")
        buffer.incrementIndent()
        buffer.printf("switch (apiMessage.apiKey()) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis) {
            val apiMessageClassName = capitalizeFirst(value.name)
            buffer.printf("case %d:%n", key)
            buffer.incrementIndent()
            buffer.printf(
                "return %sJsonConverter.write((%s) apiMessage, apiVersion);%n",
                apiMessageClassName,
                apiMessageClassName,
            )
            buffer.decrementIndent()
        }
        buffer.printf("default:%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw new UnsupportedVersionException(\"Unknown metadata id \"" +
                    " + apiMessage.apiKey());%n"
        )
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateReadJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        buffer.printf(
            "public static ApiMessage readJson(JsonNode json, short apiKey, short apiVersion) {%n"
        )
        buffer.incrementIndent()
        buffer.printf("switch (apiKey) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis.entries) {
            val apiMessageClassName = capitalizeFirst(value.name)
            buffer.printf("case %d:%n", key)
            buffer.incrementIndent()
            buffer.printf("return %sJsonConverter.read(json, apiVersion);%n", apiMessageClassName)
            buffer.decrementIndent()
        }
        buffer.printf("default:%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw new UnsupportedVersionException(\"Unknown metadata id \" + apiKey);%n"
        )
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }
}
