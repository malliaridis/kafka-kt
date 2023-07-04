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

import org.apache.kafka.message.FieldType.BoolFieldType
import org.apache.kafka.message.FieldType.Float32FieldType
import org.apache.kafka.message.FieldType.Float64FieldType
import org.apache.kafka.message.FieldType.Int16FieldType
import org.apache.kafka.message.FieldType.Int32FieldType
import org.apache.kafka.message.FieldType.Int64FieldType
import org.apache.kafka.message.FieldType.Int8FieldType
import org.apache.kafka.message.FieldType.UUIDFieldType
import org.apache.kafka.message.FieldType.Uint16FieldType
import org.apache.kafka.message.FieldType.Uint32FieldType
import org.apache.kafka.message.FieldType.Uint64FieldType
import org.apache.kafka.message.FieldType.Uint8FieldType
import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import java.io.BufferedWriter

/**
 * Generates Kafka MessageData classes.
 */
class JsonConverterGenerator internal constructor(
    private val packageName: String,
) : MessageClassGenerator {

    private val structRegistry: StructRegistry = StructRegistry()

    private val headerGenerator: HeaderGenerator = HeaderGenerator(packageName)

    private val buffer: CodeBuffer = CodeBuffer()

    override fun outputName(spec: MessageSpec): String = spec.dataClassName() + SUFFIX

    @Throws(Exception::class)
    override fun generateAndWrite(spec: MessageSpec, writer: BufferedWriter) {
        structRegistry.register(spec)
        headerGenerator.addImport(String.format("%s.%s.*", packageName, spec.dataClassName()))
        buffer.printf("%n")
        buffer.printf("object %s {%n", capitalizeFirst(outputName(spec)))
        buffer.incrementIndent()
        buffer.printf("%n")
        generateConverters(
            spec.dataClassName(), spec.struct,
            spec.validVersions
        )
        val iter = structRegistry.structs()
        while (iter.hasNext()) {
            val (structSpec, parentVersions) = iter.next()
            buffer.printf("%n")
            buffer.printf(
                "object %s {%n",
                capitalizeFirst(structSpec.name + SUFFIX),
            )
            buffer.incrementIndent()
            buffer.printf("%n")
            generateConverters(
                name = capitalizeFirst(structSpec.name),
                spec = structSpec,
                parentVersions = parentVersions,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
        buffer.decrementIndent()
        buffer.printf("}%n")
        headerGenerator.generate()
        headerGenerator.buffer.write(writer)
        buffer.write(writer)
    }

    private fun generateConverters(
        name: String,
        spec: StructSpec,
        parentVersions: Versions,
    ) {
        generateRead(name, spec, parentVersions)
        buffer.printf("%n")
        generateWrite(name, spec, parentVersions)
        buffer.printf("%n")
        generateOverloadWrite(name)
    }

    private fun generateRead(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
        buffer.printf(
            "fun read(_node: JsonNode, _version: Short): %s {%n",
            className,
        )
        buffer.incrementIndent()
        buffer.printf("val _object = %s()%n", className)
        VersionConditional.forVersions(struct.versions, parentVersions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw UnsupportedVersionException(\"Can't read version \$_version of %s\")%n",
                    className,
                )
            }
            .generate(buffer)
        val curVersions = parentVersions.intersect(struct.versions)
        for (field in struct.fields) {
            val sourceVariable = String.format("_%sNode", field.camelCaseName())
            buffer.printf(
                "val %s: JsonNode? = _node.get(\"%s\")%n",
                sourceVariable,
                field.camelCaseName(),
            )
            buffer.printf("if (%s == null) {%n", sourceVariable)
            buffer.incrementIndent()
            val mandatoryVersions = field.versions.subtract(field.taggedVersions)!!
            VersionConditional.forVersions(mandatoryVersions, curVersions)
                .ifMember {
                    buffer.printf(
                        "throw RuntimeException(\"%s: unable to locate field \'%s\', which is mandatory in version \$_version\")%n",
                        className,
                        field.camelCaseName()
                    )
                }
                .ifNotMember {
                    buffer.printf(
                        "_object.%s = %s%n",
                        field.camelCaseName(),
                        field.fieldDefault(headerGenerator, structRegistry),
                    )
                }
                .generate(buffer)
            buffer.decrementIndent()
            buffer.printf("} else {%n")
            buffer.incrementIndent()
            VersionConditional.forVersions(struct.versions, curVersions)
                .ifMember {
                    generateTargetFromJson(
                        Target(field, sourceVariable, className) { input ->
                            "_object.${field.camelCaseName()} = $input"
                        },
                        curVersions
                    )
                }
                .ifNotMember {
                    buffer.printf(
                        "throw RuntimeException(\"%s: field \'%s\' is not supported in version \$_version\")%n",
                        className,
                        field.camelCaseName(),
                    )
                }
                .generate(buffer)
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
        buffer.printf("return _object%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateTargetFromJson(target: Target, curVersions: Versions) {
        when (target.field.type) {
            is BoolFieldType -> {
                buffer.printf("if (!%s.isBoolean()) {%n", target.sourceVariable)
                buffer.incrementIndent()
                buffer.printf(
                    "throw RuntimeException(\"%s expected Boolean type, but got \${_node.getNodeType()}\")%n",
                    target.humanReadableName,
                )
                buffer.decrementIndent()
                buffer.printf("}%n")
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("${target.sourceVariable}.asBoolean()"),
                )
            }

            is Int8FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToByte(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Uint8FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToUnsignedByte(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Int16FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToShort(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Uint16FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToUnsignedShort(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Int32FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s;%n", target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToInt(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Uint32FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n", target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToUnsignedInt(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Int64FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s;%n", target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToLong(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Uint64FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n", target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToUnsignedLong(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is UUIDFieldType -> {
                buffer.printf("if (!%s.isTextual()) {%n", target.sourceVariable)
                buffer.incrementIndent()
                buffer.printf(
                    "throw RuntimeException(\"%s expected a JSON string type, but got \${_node.getNodeType()}\")%n",
                    target.humanReadableName,
                )
                buffer.decrementIndent()
                buffer.printf("}%n")
                headerGenerator.addImport(MessageGenerator.UUID_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "Uuid.fromString(%s.asText())", target.sourceVariable
                        )
                    )
                )
            }

            is Float32FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToFloat(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            is Float64FieldType -> {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToDouble(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }

            else -> {
                // Handle the variable length types.  All of them are potentially
                // nullable, so handle that here.
                IsNullConditional.forName(target.sourceVariable)
                    .nullableVersions(target.field.nullableVersions)
                    .possibleVersions(curVersions)
                    .conditionalGenerator { name, negated ->
                        String.format("%s%s.isNull()", if (negated) "!" else "", name)
                    }
                    .ifNull { buffer.printf("%s%n", target.assignmentStatement("null")) }
                    .ifShouldNotBeNull { generateVariableLengthTargetFromJson(target, curVersions) }
                    .generate(buffer)
            }
        }
    }

    private fun generateVariableLengthTargetFromJson(target: Target, curVersions: Versions) {
        if (target.field.type.isString) {
            buffer.printf("if (!%s.isTextual()) {%n", target.sourceVariable)
            buffer.incrementIndent()
            buffer.printf(
                "throw RuntimeException(\"%s expected a string type, but got \${_node.getNodeType()}\")%n",
                target.humanReadableName,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
            buffer.printf(
                "%s%n",
                target.assignmentStatement(String.format("%s.asText()", target.sourceVariable))
            )
        } else if (target.field.type.isBytes) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            if (target.field.zeroCopy) {
                headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS)
                buffer.printf(
                    "%s%n", target.assignmentStatement(
                        String.format(
                            "ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(%s, \"%s\"))",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            } else {
                buffer.printf(
                    "%s%n", target.assignmentStatement(
                        String.format(
                            "MessageUtil.jsonNodeToBinary(%s, \"%s\")",
                            target.sourceVariable,
                            target.humanReadableName,
                        )
                    )
                )
            }
        } else if (target.field.type.isRecords) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS)
            headerGenerator.addImport(MessageGenerator.MEMORY_RECORDS_CLASS)
            buffer.printf(
                "%s%n", target.assignmentStatement(
                    String.format(
                        "MemoryRecords.readableRecords(ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(%s, \"%s\")))",
                        target.sourceVariable,
                        target.humanReadableName,
                    )
                )
            )
        } else if (target.field.type.isArray) {
            buffer.printf("if (!%s.isArray()) {%n", target.sourceVariable)
            buffer.incrementIndent()
            buffer.printf(
                "throw RuntimeException(\"%s expected a JSON array, but got \${_node.getNodeType()}\")%n",
                target.humanReadableName,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
            val type = target.field.concreteKotlinType(headerGenerator, structRegistry)
            buffer.printf(
                "val _collection: %s = %s(%s.size)%n",
                type,
                type,
                target.sourceVariable,
            )
            buffer.printf("%s%n", target.assignmentStatement("_collection"))
            headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
            buffer.printf("for (_element in %s) {%n", target.sourceVariable)
            buffer.incrementIndent()
            generateTargetFromJson(
                target.arrayElementTarget { input -> String.format("_collection.add(%s)", input) },
                curVersions,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        } else if (target.field.type.isStruct) {
            buffer.printf(
                "%s%n", target.assignmentStatement(
                    String.format(
                        "%s%s.read(%s, _version)",
                        target.field.type.toString(),
                        SUFFIX,
                        target.sourceVariable,
                    )
                )
            )
        } else {
            throw RuntimeException("Unexpected type " + target.field.type)
        }
    }

    private fun generateOverloadWrite(className: String) {
        buffer.printf(
            "fun write(_object: %s, _version: Short): JsonNode {%n",
            className
        )
        buffer.incrementIndent()
        buffer.printf("return write(_object, _version, true)%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateWrite(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS)
        buffer.printf(
            "fun write(_object: %s, _version: Short, _serializeRecords: Boolean): JsonNode {%n",
            className
        )
        buffer.incrementIndent()
        VersionConditional.forVersions(struct.versions, parentVersions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw UnsupportedVersionException(\"Can't write version \$_version of %s\")%n",
                    className,
                )
            }
            .generate(buffer)
        val curVersions = parentVersions.intersect(struct.versions)
        headerGenerator.addImport(MessageGenerator.OBJECT_NODE_CLASS)
        headerGenerator.addImport(MessageGenerator.JSON_NODE_FACTORY_CLASS)
        buffer.printf("val _node = ObjectNode(JsonNodeFactory.instance)%n")
        for (field in struct.fields) {
            val target = Target(
                field = field,
                sourceVariable = String.format("_object.%s", field.camelCaseName()),
                humanReadableName = field.camelCaseName()
            ) { input ->
                String.format(
                    "_node.set(\"%s\", %s)", // TODO find a solution for this
                    field.camelCaseName(),
                    input,
                )
            }
            val cond = VersionConditional.forVersions(field.versions, curVersions)
                .ifMember { presentVersions ->
                    VersionConditional.forVersions(field.taggedVersions, presentVersions)
                        .ifMember { presentAndTaggedVersions: Versions ->
                            field.generateNonDefaultValueCheck(
                                headerGenerator,
                                structRegistry, buffer, "_object.", field.nullableVersions
                            )
                            buffer.incrementIndent()
                            // If the default was null, and we already checked that this field was not
                            // the default, we can omit further null checks.
                            if (field.fieldDefault == "null") generateTargetToJson(
                                target.nonNullableCopy(),
                                presentAndTaggedVersions,
                            ) else generateTargetToJson(target, presentAndTaggedVersions)

                            buffer.decrementIndent()
                            buffer.printf("}%n")
                        }
                        .ifNotMember { presentAndNotTaggedVersions ->
                            generateTargetToJson(target, presentAndNotTaggedVersions)
                        }
                        .generate(buffer)
                }
            if (!field.ignorable) {
                cond.ifNotMember {
                    field.generateNonIgnorableFieldCheck(
                        headerGenerator,
                        structRegistry, "_object.", buffer
                    )
                }
            }
            cond.generate(buffer)
        }
        buffer.printf("return _node%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateTargetToJson(target: Target, versions: Versions) {
        when (target.field.type) {
            is BoolFieldType -> {
                headerGenerator.addImport(MessageGenerator.BOOLEAN_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("BooleanNode.valueOf(${target.sourceVariable})"),
                )
            }

            is Int8FieldType,
            is Uint8FieldType, // TODO See if this entry will work property
            is Int16FieldType -> {
                headerGenerator.addImport(MessageGenerator.SHORT_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("ShortNode(${target.sourceVariable})"),
                )
            }
            // TODO The new data types (except float) are failing here (fasterxml does not support unsigned fields)
            is Int32FieldType,
            is Uint16FieldType, -> {
                headerGenerator.addImport(MessageGenerator.INT_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("IntNode(${target.sourceVariable})"),
                )
            }

            is Int64FieldType,
            is Uint32FieldType, -> {
                headerGenerator.addImport(MessageGenerator.LONG_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("LongNode(${target.sourceVariable})"),
                )
            }

            is UUIDFieldType -> {
                headerGenerator.addImport(MessageGenerator.TEXT_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("TextNode(${target.sourceVariable}.toString())"),
                )
            }

            is Float32FieldType -> {
                headerGenerator.addImport(MessageGenerator.FLOAT_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("FloatNode(${target.sourceVariable})"),
                )
            }

            is Float64FieldType -> {
                headerGenerator.addImport(MessageGenerator.DOUBLE_NODE_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement("DoubleNode(${target.sourceVariable})"),
                )
            }

            else -> {
                // Handle the variable length types.  All of them are potentially
                // nullable, so handle that here.
                IsNullConditional.forName(target.sourceVariable)
                    .nullableVersions(target.field.nullableVersions)
                    .possibleVersions(versions)
                    .conditionalGenerator { name, negated ->
                        "$name ${if (negated) "!" else "="}= null"
                    }
                    .ifNull {
                        headerGenerator.addImport(MessageGenerator.NULL_NODE_CLASS)
                        buffer.printf("%s%n", target.assignmentStatement("NullNode.instance"))
                    }
                    .ifShouldNotBeNull { generateVariableLengthTargetToJson(target, versions) }
                    .generate(buffer)
            }
        }
    }

    private fun generateVariableLengthTargetToJson(target: Target, versions: Versions) {
        if (target.field.type.isString) {
            headerGenerator.addImport(MessageGenerator.TEXT_NODE_CLASS)
            buffer.printf(
                "%s%n",
                target.assignmentStatement("TextNode(${target.sourceVariable})")
            )
        } else if (target.field.type.isBytes) {
            headerGenerator.addImport(MessageGenerator.BINARY_NODE_CLASS)
            if (target.field.zeroCopy) {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                buffer.printf(
                    "%s%n",
                    target.assignmentStatement(
                        String.format(
                            "BinaryNode(MessageUtil.byteBufferToArray(%s))",
                            target.sourceVariable
                        )
                    ),
                )
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS)
                buffer.printf(
                    "%s%n", target.assignmentStatement(
                        String.format(
                            "BinaryNode(Arrays.copyOf(%s, %s.length))",
                            target.sourceVariable,
                            target.sourceVariable,
                        )
                    )
                )
            }
        } else if (target.field.type.isRecords) {
            headerGenerator.addImport(MessageGenerator.BINARY_NODE_CLASS)
            headerGenerator.addImport(MessageGenerator.INT_NODE_CLASS)
            // KIP-673: When logging requests/responses, we do not serialize the record, instead we
            // output its sizeInBytes, because outputting the bytes is not very useful and can be
            // quite expensive. Otherwise, we will serialize the record.
            buffer.printf("if (_serializeRecords) {%n")
            buffer.incrementIndent()
            buffer.printf(
                "%s%n",
                target.assignmentStatement("BinaryNode(ByteArray(0))"),
            )
            buffer.decrementIndent()
            buffer.printf("} else {%n")
            buffer.incrementIndent()
            buffer.printf(
                "_node.set<IntNode>(\"%sSizeInBytes\", IntNode(%s.sizeInBytes()))%n",
                target.field.camelCaseName(),
                target.sourceVariable
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        } else if (target.field.type.isArray) {
            headerGenerator.addImport(MessageGenerator.ARRAY_NODE_CLASS)
            headerGenerator.addImport(MessageGenerator.JSON_NODE_FACTORY_CLASS)
            val arrayType = target.field.type as FieldType.ArrayType
            val elementType = arrayType.elementType
            val arrayInstanceName = String.format(
                "_%sArray",
                target.field.camelCaseName()
            )
            buffer.printf(
                "val %s: ArrayNode = ArrayNode(JsonNodeFactory.instance)%n",
                arrayInstanceName,
            )
            buffer.printf(
                "for (_element in %s) {%n",
                target.sourceVariable,
            )
            buffer.incrementIndent()
            generateTargetToJson(
                target.arrayElementTarget { input ->
                    String.format("%s.add(%s)", arrayInstanceName, input)
                },
                versions
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
            buffer.printf("%s%n", target.assignmentStatement(arrayInstanceName))
        } else if (target.field.type.isStruct) buffer.printf(
            "%s%n", target.assignmentStatement(
                String.format(
                    "%sJsonConverter.write(%s, _version, _serializeRecords)",
                    target.field.type.toString(),
                    target.sourceVariable
                )
            )
        ) else throw RuntimeException("unknown type " + target.field.type)
    }

    private fun getTargetClass(target: Target, versions: Versions): String {
        return if (target.field.type.isString) {
            headerGenerator.addImport(MessageGenerator.TEXT_NODE_CLASS)
            "TextNode"
        } else if (target.field.type.isBytes) {
            headerGenerator.addImport(MessageGenerator.BINARY_NODE_CLASS)
            "BinaryNode"
        } else if (target.field.type.isRecords) ""
        else if (target.field.type.isStruct) ""
        else throw RuntimeException("unknown type " + target.field.type)

    }

    companion object {
        private const val SUFFIX = "JsonConverter"
    }
}
