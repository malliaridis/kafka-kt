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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The Kafka message generator.
 */
object MessageGenerator {

    const val JSON_SUFFIX = ".json"

    val JSON_GLOB = "*" + JSON_SUFFIX

    const val KOTLIN_SUFFIX = ".kt"

    const val API_MESSAGE_TYPE_KOTLIN = "ApiMessageType.kt"

    const val API_SCOPE_KOTLIN = "ApiScope.kt"

    const val METADATA_RECORD_TYPE_KOTLIN = "MetadataRecordType.kt"

    const val METADATA_JSON_CONVERTERS_KOTLIN = "MetadataJsonConverters.kt"

    const val API_MESSAGE_CLASS = "org.apache.kafka.common.protocol.ApiMessage"

    const val MESSAGE_CLASS = "org.apache.kafka.common.protocol.Message"

    const val MESSAGE_UTIL_CLASS = "org.apache.kafka.common.protocol.MessageUtil"

    const val READABLE_CLASS = "org.apache.kafka.common.protocol.Readable"

    const val WRITABLE_CLASS = "org.apache.kafka.common.protocol.Writable"

    @Deprecated("Use kotlin.collections.listOf()")
    const val ARRAYS_CLASS = "java.util.Arrays"

    @Deprecated("Class kotlin.Any might be more suitable")
    const val OBJECTS_CLASS = "java.util.Objects"

    const val ANY_CLASS = "kotlin.Any"

    const val LIST_CLASS = "kotlin.collections.List"

    const val ARRAYLIST_CLASS = "java.util.ArrayList"

    const val IMPLICIT_LINKED_HASH_COLLECTION_CLASS =
        "org.apache.kafka.common.utils.ImplicitLinkedHashCollection"

    const val IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS =
        "org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection"

    const val UNSUPPORTED_VERSION_EXCEPTION_CLASS =
        "org.apache.kafka.common.errors.UnsupportedVersionException"

    const val ITERATOR_CLASS = "kotlin.collections.Iterator"

    const val MUTABLE_ITERATOR_CLASS = "kotlin.collections.MutableIterator"

    const val ENUM_SET_CLASS = "java.util.EnumSet"

    const val TYPE_CLASS = "org.apache.kafka.common.protocol.types.Type"

    const val FIELD_CLASS = "org.apache.kafka.common.protocol.types.Field"

    const val SCHEMA_CLASS = "org.apache.kafka.common.protocol.types.Schema"

    const val ARRAYOF_CLASS = "org.apache.kafka.common.protocol.types.ArrayOf"

    const val COMPACT_ARRAYOF_CLASS = "org.apache.kafka.common.protocol.types.CompactArrayOf"

    const val BYTES_CLASS = "org.apache.kafka.common.utils.Bytes"

    const val UUID_CLASS = "org.apache.kafka.common.Uuid"

    const val BASE_RECORDS_CLASS = "org.apache.kafka.common.record.BaseRecords"

    const val MEMORY_RECORDS_CLASS = "org.apache.kafka.common.record.MemoryRecords"

    const val REQUEST_SUFFIX = "Request"

    const val RESPONSE_SUFFIX = "Response"

    const val BYTE_UTILS_CLASS = "org.apache.kafka.common.utils.ByteUtils"

    @Deprecated(
        message  = "Use CHARSETS instead",
        replaceWith = ReplaceWith("CHARSETS"),
    )
    const val STANDARD_CHARSETS = "java.nio.charset.StandardCharsets"

    const val CHARSETS = "kotlin.text.Charsets"

    const val TAGGED_FIELDS_SECTION_CLASS =
        "org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection"

    const val OBJECT_SERIALIZATION_CACHE_CLASS =
        "org.apache.kafka.common.protocol.ObjectSerializationCache"

    const val MESSAGE_SIZE_ACCUMULATOR_CLASS =
        "org.apache.kafka.common.protocol.MessageSizeAccumulator"

    const val RAW_TAGGED_FIELD_CLASS = "org.apache.kafka.common.protocol.types.RawTaggedField"

    const val RAW_TAGGED_FIELD_WRITER_CLASS =
        "org.apache.kafka.common.protocol.types.RawTaggedFieldWriter"

    const val TREE_MAP_CLASS = "java.util.TreeMap"

    const val BYTE_BUFFER_CLASS = "java.nio.ByteBuffer"

    const val NAVIGABLE_MAP_CLASS = "java.util.NavigableMap"

    const val MAP_ENTRY_CLASS = "java.util.Map.Entry"

    const val JSON_NODE_CLASS = "com.fasterxml.jackson.databind.JsonNode"

    const val OBJECT_NODE_CLASS = "com.fasterxml.jackson.databind.node.ObjectNode"

    const val JSON_NODE_FACTORY_CLASS = "com.fasterxml.jackson.databind.node.JsonNodeFactory"

    const val BOOLEAN_NODE_CLASS = "com.fasterxml.jackson.databind.node.BooleanNode"

    const val SHORT_NODE_CLASS = "com.fasterxml.jackson.databind.node.ShortNode"

    const val INT_NODE_CLASS = "com.fasterxml.jackson.databind.node.IntNode"

    const val LONG_NODE_CLASS = "com.fasterxml.jackson.databind.node.LongNode"

    const val TEXT_NODE_CLASS = "com.fasterxml.jackson.databind.node.TextNode"

    const val BINARY_NODE_CLASS = "com.fasterxml.jackson.databind.node.BinaryNode"

    const val NULL_NODE_CLASS = "com.fasterxml.jackson.databind.node.NullNode"

    const val ARRAY_NODE_CLASS = "com.fasterxml.jackson.databind.node.ArrayNode"

    const val FLOAT_NODE_CLASS = "com.fasterxml.jackson.databind.node.FLoatNode"

    const val DOUBLE_NODE_CLASS = "com.fasterxml.jackson.databind.node.DoubleNode"

    @Deprecated("Use UShort.MAX_VALUE instead")
    const val UNSIGNED_SHORT_MAX = UShort.MAX_VALUE

    @Deprecated("Use UInt.MAX_VALUE instead")
    const val UNSIGNED_INT_MAX = UInt.MAX_VALUE

    /**
     * The Jackson serializer we use for JSON objects.
     */
    val JSON_SERDE: ObjectMapper = ObjectMapper().apply {
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true)
        configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    }

    private fun createTypeClassGenerators(
        packageName: String,
        types: List<String>?,
    ): List<TypeClassGenerator> {
        if (types == null) return emptyList()
        val generators: MutableList<TypeClassGenerator> = ArrayList()
        for (type in types) {
            when (type) {
                "ApiMessageTypeGenerator" -> generators.add(ApiMessageTypeGenerator(packageName))
                "MetadataRecordTypeGenerator" ->
                    generators.add(MetadataRecordTypeGenerator(packageName))

                "MetadataJsonConvertersGenerator" ->
                    generators.add(MetadataJsonConvertersGenerator(packageName))

                else -> throw RuntimeException("Unknown type class generator type '$type'")
            }
        }
        return generators
    }

    private fun createMessageClassGenerators(
        packageName: String,
        types: List<String>?,
    ): List<MessageClassGenerator> {
        if (types == null) return emptyList()
        val generators: MutableList<MessageClassGenerator> = ArrayList()
        for (type in types) {
            when (type) {
                "MessageDataGenerator" -> generators.add(MessageDataGenerator(packageName))
                "JsonConverterGenerator" -> generators.add(JsonConverterGenerator(packageName))
                else -> throw RuntimeException("Unknown message class generator type '$type'")
            }
        }
        return generators
    }

    @Throws(Exception::class)
    fun processDirectories(
        packageName: String,
        outputDir: String,
        inputDir: String,
        typeClassGeneratorTypes: List<String>?,
        messageClassGeneratorTypes: List<String>?,
    ) {
        Files.createDirectories(Paths.get(outputDir))
        var numProcessed = 0
        val typeClassGenerators = createTypeClassGenerators(packageName, typeClassGeneratorTypes)
        val outputFileNames = HashSet<String>()
        Files.newDirectoryStream(Paths.get(inputDir), JSON_GLOB).use { directoryStream ->
            for (inputPath in directoryStream) try {
                val spec = JSON_SERDE.readValue(inputPath.toFile(), MessageSpec::class.java)
                val generators =
                    createMessageClassGenerators(packageName, messageClassGeneratorTypes)

                for (generator in generators) {
                    val name = generator.outputName(spec) + KOTLIN_SUFFIX
                    outputFileNames.add(name)
                    val outputPath = Paths.get(outputDir, name)
                    Files.newBufferedWriter(outputPath).use { writer ->
                        generator.generateAndWrite(spec, writer)
                    }
                }
                numProcessed++
                typeClassGenerators.forEach { generator ->
                    generator.registerMessageType(spec)
                }
            } catch (e: Exception) {
                throw RuntimeException("Exception while processing $inputPath", e)
            }
        }
        for (typeClassGenerator in typeClassGenerators) {
            outputFileNames.add(typeClassGenerator.outputName())
            val factoryOutputPath = Paths.get(outputDir, typeClassGenerator.outputName())
            Files.newBufferedWriter(factoryOutputPath).use { writer ->
                typeClassGenerator.generateAndWrite(writer)
            }
        }

        Files.newDirectoryStream(Paths.get(outputDir)).use { directoryStream ->
            for (outputPath: Path in directoryStream) {
                val fileName = outputPath.fileName ?: continue
                if (!outputFileNames.contains(fileName.toString()))
                    Files.delete(outputPath)
            }
        }

        System.out.printf(
            "MessageGenerator: processed %d Kafka message JSON files(s).%n",
            numProcessed,
        )
    }

    fun capitalizeFirst(string: String): String {
        return if (string.isEmpty()) string
        else string.substring(0, 1).uppercase() + string.substring(1)
    }

    fun lowerCaseFirst(string: String): String {
        return if (string.isEmpty()) string
        else string.substring(0, 1).lowercase() + string.substring(1)
    }

    fun firstIsCapitalized(string: String): Boolean {
        return if (string.isEmpty()) false
        else Character.isUpperCase(string[0])
    }

    fun toSnakeCase(string: String): String {
        val bld = StringBuilder()
        var prevWasCapitalized = true

        for (element in string) {
            prevWasCapitalized = if (Character.isUpperCase(element)) {
                if (!prevWasCapitalized) bld.append('_')
                bld.append(element.lowercaseChar())
                true
            } else {
                bld.append(element)
                false
            }
        }
        return bld.toString()
    }

    fun stripSuffix(str: String, suffix: String): String {
        return if (str.endsWith(suffix)) str.substring(0, str.length - suffix.length)
        else throw RuntimeException("String $str does not end with the expected suffix $suffix")
    }

    /**
     * Return the number of bytes needed to encode an integer in unsigned variable-length format.
     */
    @Deprecated("Directly use unsigned data types")
    fun sizeOfUnsignedVarint(value: Int): Int {
        var value = value
        var bytes = 1
        while ((value and -0x80).toLong() != 0L) {
            bytes += 1
            value = value ushr 7
        }
        return bytes
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val parser = ArgumentParsers
            .newArgumentParser("message-generator")
            .defaultHelp(true)
            .description("The Kafka message generator")

        parser.addArgument("--package", "-p")
            .action(Arguments.store())
            .required(true)
            .metavar("PACKAGE")
            .help("The kotlin package to use in generated files.")

        parser.addArgument("--output", "-o")
            .action(Arguments.store())
            .required(true)
            .metavar("OUTPUT")
            .help("The output directory to create.")

        parser.addArgument("--input", "-i")
            .action(Arguments.store())
            .required(true)
            .metavar("INPUT")
            .help("The input directory to use.")

        parser.addArgument("--typeclass-generators", "-t")
            .nargs("+")
            .action(Arguments.store())
            .metavar("TYPECLASS_GENERATORS")
            .help("The type class generators to use, if any.")

        parser.addArgument("--message-class-generators", "-m")
            .nargs("+")
            .action(Arguments.store())
            .metavar("MESSAGE_CLASS_GENERATORS")
            .help("The message class generators to use.")

        val res = parser.parseArgsOrFail(args)
        processDirectories(
            packageName = res.getString("package"),
            outputDir = res.getString("output"),
            inputDir = res.getString("input"),
            typeClassGeneratorTypes = res.getList("typeclass_generators"),
            messageClassGeneratorTypes = res.getList("message_class_generators"),
        )
    }
}
