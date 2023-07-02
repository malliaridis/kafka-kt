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
import org.apache.kafka.message.MessageGenerator.toSnakeCase
import java.io.BufferedWriter
import java.io.IOException
import java.util.*

class MetadataRecordTypeGenerator(packageName: String) : TypeClassGenerator {

    private val headerGenerator = HeaderGenerator(packageName)

    private val buffer = CodeBuffer()

    private val apis = TreeMap<Short, MessageSpec>()

    override fun outputName(): String = MessageGenerator.METADATA_RECORD_TYPE_JAVA

    override fun registerMessageType(spec: MessageSpec) {
        if (spec.type != MessageSpecType.METADATA) return

        val id: Short = spec.apiKey!!
        val prevSpec = apis.put(id, spec)
        if (prevSpec != null) throw RuntimeException(
            "Duplicate metadata record entry for type $id. Original claimant: ${prevSpec.name}. "+
                    "New claimant: ${spec.name}"
        )
    }

    @Throws(IOException::class)
    override fun generateAndWrite(writer: BufferedWriter) {
        generate()
        write(writer)
    }

    private fun generate() {
        buffer.printf("public enum MetadataRecordType {%n")
        buffer.incrementIndent()
        generateEnumValues()
        buffer.printf("%n")
        generateInstanceVariables()
        buffer.printf("%n")
        generateEnumConstructor()
        buffer.printf("%n")
        generateFromApiKey()
        buffer.printf("%n")
        generateNewMetadataRecord()
        buffer.printf("%n")
        generateAccessor("id", "short")
        buffer.printf("%n")
        generateAccessor("lowestSupportedVersion", "short")
        buffer.printf("%n")
        generateAccessor("highestSupportedVersion", "short")
        buffer.printf("%n")
        generateToString()
        buffer.decrementIndent()
        buffer.printf("}%n")
        headerGenerator.generate()
    }

    private fun generateEnumValues() {
        var numProcessed = 0
        for ((key, spec) in apis) {
            numProcessed++
            buffer.printf(
                "%s(\"%s\", (short) %d, (short) %d, (short) %d)%s%n",
                toSnakeCase(spec.name).uppercase(),
                capitalizeFirst(spec.name),
                key,
                spec.validVersions.lowest,
                spec.validVersions.highest,
                if ((numProcessed == apis.size)) ";" else ","
            )
        }
    }

    private fun generateInstanceVariables() {
        buffer.printf("private final String name;%n")
        buffer.printf("private final short id;%n")
        buffer.printf("private final short lowestSupportedVersion;%n")
        buffer.printf("private final short highestSupportedVersion;%n")
    }

    private fun generateEnumConstructor() {
        buffer.printf(
            "MetadataRecordType(String name, short id, short lowestSupportedVersion, " +
                    "short highestSupportedVersion) {%n",
        )
        buffer.incrementIndent()
        buffer.printf("this.name = name;%n")
        buffer.printf("this.id = id;%n")
        buffer.printf("this.lowestSupportedVersion = lowestSupportedVersion;%n")
        buffer.printf("this.highestSupportedVersion = highestSupportedVersion;%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFromApiKey() {
        buffer.printf("public static MetadataRecordType fromId(short id) {%n")
        buffer.incrementIndent()
        buffer.printf("switch (id) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis) {
            buffer.printf("case %d:%n", key)
            buffer.incrementIndent()
            buffer.printf("return %s;%n", toSnakeCase(value.name).uppercase())
            buffer.decrementIndent()
        }
        buffer.printf("default:%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("throw new UnsupportedVersionException(\"Unknown metadata id \" + id);%n")
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateNewMetadataRecord() {
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        buffer.printf("public ApiMessage newMetadataRecord() {%n")
        buffer.incrementIndent()
        buffer.printf("switch (id) {%n")
        buffer.incrementIndent()
        for ((key, value) in apis) {
            buffer.printf("case %d:%n", key)
            buffer.incrementIndent()
            buffer.printf("return new %s();%n", capitalizeFirst(value.name))
            buffer.decrementIndent()
        }
        buffer.printf("default:%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf("throw new UnsupportedVersionException(\"Unknown metadata id \" + id);%n")
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateAccessor(name: String, type: String) {
        buffer.printf("public %s %s() {%n", type, name)
        buffer.incrementIndent()
        buffer.printf("return this.%s;%n", name)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateToString() {
        buffer.printf("@Override%n")
        buffer.printf("public String toString() {%n")
        buffer.incrementIndent()
        buffer.printf("return this.name();%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    @Throws(IOException::class)
    private fun write(writer: BufferedWriter) {
        headerGenerator.buffer.write(writer)
        buffer.write(writer)
    }
}
