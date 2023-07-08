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
import org.apache.kafka.message.FieldType.BytesFieldType
import org.apache.kafka.message.FieldType.Float32FieldType
import org.apache.kafka.message.FieldType.Float64FieldType
import org.apache.kafka.message.FieldType.Int16FieldType
import org.apache.kafka.message.FieldType.Int32FieldType
import org.apache.kafka.message.FieldType.Int64FieldType
import org.apache.kafka.message.FieldType.Int8FieldType
import org.apache.kafka.message.FieldType.StringFieldType
import org.apache.kafka.message.FieldType.StructType
import org.apache.kafka.message.FieldType.UUIDFieldType
import org.apache.kafka.message.FieldType.Uint16FieldType
import org.apache.kafka.message.FieldType.Uint32FieldType
import org.apache.kafka.message.FieldType.Uint64FieldType
import org.apache.kafka.message.FieldType.Uint8FieldType
import org.apache.kafka.message.MessageGenerator.sizeOfUnsignedVarint
import java.io.BufferedWriter
import java.util.*
import kotlin.collections.LinkedHashSet

/**
 * Generates Kafka MessageData classes.
 */
class MessageDataGenerator internal constructor(
    packageName: String,
) : MessageClassGenerator {

    private val structRegistry: StructRegistry = StructRegistry()

    private val headerGenerator: HeaderGenerator = HeaderGenerator(packageName)

    private val schemaGenerator: SchemaGenerator = SchemaGenerator(headerGenerator, structRegistry)

    private val buffer: CodeBuffer = CodeBuffer()

    private lateinit var messageFlexibleVersions: Versions

    override fun outputName(spec: MessageSpec): String = spec.dataClassName()

    @Throws(Exception::class)
    override fun generateAndWrite(spec: MessageSpec, writer: BufferedWriter) {
        generate(spec)
        write(writer)
    }

    @Throws(Exception::class)
    fun generate(message: MessageSpec) {
        if (message.struct.versions.contains(Short.MAX_VALUE))
            throw RuntimeException("Message ${message.name} does not specify a maximum version.")

        structRegistry.register(message)
        schemaGenerator.generateSchemas(message)
        messageFlexibleVersions = message.flexibleVersions
        generateClass(
            topLevelMessageSpec = message,
            className = message.dataClassName(),
            struct = message.struct,
            parentVersions = message.struct.versions,
        )
        headerGenerator.generate()
    }

    @Throws(Exception::class)
    fun write(writer: BufferedWriter) {
        headerGenerator.buffer.write(writer)
        buffer.write((writer))
    }

    @Throws(Exception::class)
    private fun generateClass(
        topLevelMessageSpec: MessageSpec? = null,
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        buffer.printf("%n")
        val isTopLevel = topLevelMessageSpec != null
        val isSetElement = struct.hasKeys // Check if the class is inside a set.
        if (isTopLevel && isSetElement)
            throw RuntimeException("Cannot set mapKey on top level fields.")

        generateClassHeader(className, isTopLevel, isSetElement)
        buffer.incrementIndent()
        generateFieldDeclarations(struct, isSetElement)
        buffer.printf("%n")
        schemaGenerator.writeSchema(className, buffer)
        buffer.printf("%n")
        generateClassConstructors(className, struct, isSetElement)
        if (topLevelMessageSpec != null) {
            buffer.printf("%n")
            generateShortAccessor(
                name = "apiKey",
                value = topLevelMessageSpec.apiKey ?: (-1).toShort(),
            )
        }
        buffer.printf("%n")
        generateShortAccessor(name = "lowestSupportedVersion", value = parentVersions.lowest)
        buffer.printf("%n")
        generateShortAccessor(name = "highestSupportedVersion", value = parentVersions.highest)
        buffer.printf("%n")
        generateClassReader(className, struct, parentVersions)
        buffer.printf("%n")
        generateClassWriter(className, struct, parentVersions)
        buffer.printf("%n")
        generateClassMessageSize(className, struct, parentVersions)
        if (isSetElement) {
            buffer.printf("%n")
            generateClassEquals(className, struct, true)
        }
        buffer.printf("%n")
        generateClassEquals(className, struct, false)
        buffer.printf("%n")
        generateClassHashCode(struct, isSetElement)
        buffer.printf("%n")
        generateClassDuplicate(className, struct)
        buffer.printf("%n")
        generateClassToString(className, struct)
        generateFieldAccessors(struct, isSetElement)
        buffer.printf("%n")
        generateUnknownTaggedFieldsAccessor()
        generateFieldMutators(struct, className, isSetElement)
        if (!isTopLevel) {
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
        generateSubclasses(className, struct, parentVersions, isSetElement)
        if (isTopLevel) {
            val iter = structRegistry.commonStructs()
            while (iter.hasNext()) {
                val commonStruct = iter.next()
                generateClass(
                    className = commonStruct.name,
                    struct = commonStruct,
                    parentVersions = commonStruct.versions
                )
            }
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    private fun generateClassHeader(
        className: String,
        isTopLevel: Boolean,
        isSetElement: Boolean,
    ) {
        val implementedInterfaces: MutableSet<String> = HashSet()
        if (isTopLevel) {
            implementedInterfaces.add("ApiMessage")
            headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS)
        } else {
            implementedInterfaces.add("Message")
            headerGenerator.addImport(MessageGenerator.MESSAGE_CLASS)
        }
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_COLLECTION_CLASS)
            implementedInterfaces.add("ImplicitLinkedHashCollection.Element")
        }
        val classModifiers: MutableSet<String> = LinkedHashSet()
        // TODO Addd data class modifier once properties added to constructor
        // classModifiers.add("data")
        // if (!isTopLevel) classModifiers.add("inner")

        buffer.printf(
            "%sclass %s : %s {%n",
            classModifiers.joinToString(" "),
            className,
            implementedInterfaces.joinToString(", "),
        )
    }

    @Throws(Exception::class)
    private fun generateSubclasses(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
        isSetElement: Boolean,
    ) {
        for (field in struct.fields) {
            if (field.type.isStructArray) {
                val arrayType = field.type as FieldType.ArrayType
                if (!structRegistry.commonStructNames().contains(arrayType.elementName)) {
                    generateClass(
                        className = arrayType.elementType.toString(),
                        struct = structRegistry.findStruct(field),
                        parentVersions = parentVersions.intersect(struct.versions),
                    )
                }
            } else if (field.type.isStruct) {
                if (!structRegistry.commonStructNames().contains(field.typeString)) {
                    generateClass(
                        className = field.typeString,
                        struct = structRegistry.findStruct(field),
                        parentVersions = parentVersions.intersect(struct.versions),
                    )
                }
            }
        }
        if (isSetElement) generateHashSet(className, struct)
    }

    private fun generateHashSet(className: String, struct: StructSpec) {
        buffer.printf("%n")
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
        buffer.printf(
            // TODO Consider using LinkedHashSet or ArrayList instead
            "class %s : ImplicitLinkedHashMultiCollection<%s> {%n",
            FieldSpec.collectionType(className),
            className,
        )
        buffer.incrementIndent()
        generateHashSetZeroArgConstructor(className)
        buffer.printf("%n")
        generateHashSetSizeArgConstructor(className)
        buffer.printf("%n")
        generateHashSetIteratorConstructor(className)
        buffer.printf("%n")
        generateHashSetFindMethod(className, struct)
        buffer.printf("%n")
        generateHashSetFindAllMethod(className, struct)
        buffer.printf("%n")
        generateCollectionDuplicateMethod(className)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetZeroArgConstructor(className: String) {
        buffer.printf("constructor() : super()%n")
    }

    private fun generateHashSetSizeArgConstructor(className: String) {
        buffer.printf("constructor(expectedNumElements: Int) : super(expectedNumElements)%n",)
    }

    private fun generateHashSetIteratorConstructor(className: String) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS)
        buffer.printf("constructor(iterator: Iterator<%s>) : super(iterator)%n", className)
    }

    private fun generateHashSetFindMethod(className: String, struct: StructSpec) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf(
            "fun find(%s): %s? {%n",
            commaSeparatedHashSetFieldAndTypes(struct),
            className,
        )
        buffer.incrementIndent()
        generateKeyElement(className, struct)
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
        buffer.printf("return find(key)%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetFindAllMethod(className: String, struct: StructSpec) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf(
            "fun findAll(%s): List<%s> {%n",
            commaSeparatedHashSetFieldAndTypes(struct),
            className,
        )
        buffer.incrementIndent()
        generateKeyElement(className, struct)
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
        buffer.printf("return findAll(key)%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateKeyElement(className: String, struct: StructSpec) {
        buffer.printf("val key = %s()%n", className, className)
        for (field: FieldSpec in struct.fields) {
            if (field.mapKey) {
                buffer.printf(
                    "key.set%s(%s)%n",
                    field.capitalizedCamelCaseName(),
                    field.camelCaseName(),
                )
            }
        }
    }

    private fun commaSeparatedHashSetFieldAndTypes(struct: StructSpec): String {
        return struct.fields.filter { f -> f.mapKey }.joinToString(", ") { spec ->
            String.format(
                "%s: %s",
                spec.camelCaseName(),
                spec.concreteKotlinType(headerGenerator, structRegistry),
            )
        }
    }

    // TODO Can be removed if the class is a data class (since data classes have copy)
    private fun generateCollectionDuplicateMethod(className: String) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf("fun duplicate(): %s {%n", FieldSpec.collectionType(className))
        buffer.incrementIndent()
        buffer.printf(
            "val duplicate = %s(size)%n",
            FieldSpec.collectionType(className),
        )
        buffer.printf("for (element in this) {%n")
        buffer.incrementIndent()
        buffer.printf("duplicate.add(element.duplicate())%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("return duplicate%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldDeclarations(struct: StructSpec, isSetElement: Boolean) {
        // for (field in struct.fields) generateFieldDeclaration(field)
        for (field in struct.fields) generateFieldDeclarationWithDefaults(field)

        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS)
        buffer.printf("private var unknownTaggedFields: List<RawTaggedField>? = null%n")
        generateFieldEpilogue(isSetElement)
    }

    private fun generateFieldDeclarationWithDefaults(field: FieldSpec) {
        buffer.printf(
            "var %s: %s = %s%n",
            field.camelCaseName(),
            field.fieldAbstractKotlinType(headerGenerator, structRegistry),
            field.fieldDefault(headerGenerator, structRegistry),
        )
    }

    private fun generateFieldAccessors(struct: StructSpec, isSetElement: Boolean) {
        for (field in struct.fields) generateFieldAccessor(field)

        if (isSetElement) {
            buffer.printf("%n")
            generateAccessor(
                kotlinType = "Int",
                functionName = "next",
                memberName = "next",
                isOverride = true,
            )
            buffer.printf("%n")
            generateAccessor(
                kotlinType = "Int",
                functionName = "prev",
                memberName = "prev",
                isOverride = true,
            )
        }
    }

    private fun generateUnknownTaggedFieldsAccessor() {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS)
        buffer.printf("override fun unknownTaggedFields(): List<RawTaggedField> {%n")
        buffer.incrementIndent()
        // Optimize unknownTaggedFields by not creating a new list object
        // unless we need it.
        buffer.printf("if (unknownTaggedFields == null) {%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
        buffer.printf("unknownTaggedFields = ArrayList()%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("return unknownTaggedFields!!%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldMutators(
        struct: StructSpec,
        className: String,
        isSetElement: Boolean,
    ) {
        for (field in struct.fields) generateFieldMutator(className, field)

        if (isSetElement) {
            buffer.printf("%n")
            generateSetter(
                kotlinType = "Int",
                functionName = "setNext",
                memberName = "next",
                isOverride = true,
            )
            buffer.printf("%n")
            generateSetter(
                kotlinType = "Int",
                functionName = "setPrev",
                memberName = "prev",
                isOverride = true,
            )
        }
    }

    private fun generateClassConstructors(
        className: String,
        struct: StructSpec,
        isSetElement: Boolean,
    ) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS)
        buffer.printf("constructor(readable: Readable, version: Short) {%n", className)
        buffer.incrementIndent()
        buffer.printf("read(readable, version)%n")
        generateConstructorEpilogue(isSetElement)
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("%n")
        buffer.printf("constructor()%n")
    }

    private fun generateConstructorEpilogue(isSetElement: Boolean) {
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_COLLECTION_CLASS)
            buffer.printf("this.prev = ImplicitLinkedHashCollection.INVALID_INDEX%n")
            buffer.printf("this.next = ImplicitLinkedHashCollection.INVALID_INDEX%n")
        }
    }

    private fun generateFieldEpilogue(isSetElement: Boolean) {
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_COLLECTION_CLASS)
            buffer.printf("private var next: Int = ImplicitLinkedHashCollection.INVALID_INDEX%n")
            buffer.printf("private var prev: Int = ImplicitLinkedHashCollection.INVALID_INDEX%n")
        }
    }

    private fun generateShortAccessor(name: String, value: Short) {
        buffer.printf("override fun %s(): Short {%n", name)
        buffer.incrementIndent()
        buffer.printf("return %d%n", value)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateClassReader(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS)
        buffer.printf("override fun read(readable: Readable, version: Short) {%n")
        buffer.incrementIndent()
        VersionConditional.forVersions(parentVersions, struct.versions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw UnsupportedVersionException(\"Can't read version \$version of %s\")%n",
                    className,
                )
            }
            .generate(buffer)
        val curVersions = parentVersions.intersect(struct.versions)
        for (field in struct.fields) {
            val fieldFlexibleVersions = fieldFlexibleVersions(field)
            if (field.taggedVersions.intersect(fieldFlexibleVersions) != field.taggedVersions) {
                throw RuntimeException(
                    "Field ${field.name} specifies tagged versions ${field.taggedVersions} that " +
                            "are not a subset of the flexible versions $fieldFlexibleVersions"
                )
            }
            val mandatoryVersions = field.versions.subtract(field.taggedVersions)!!
            VersionConditional.forVersions(mandatoryVersions, curVersions)
                .alwaysEmitBlockScope(field.type.isVariableLength)
                .ifNotMember {
                    // If the field is not present, or is tagged, set it to its default here.
                    buffer.printf(
                        "%s = %s%n",
                        field.prefixedCamelCaseName(),
                        field.fieldDefault(headerGenerator, structRegistry),
                    )
                }
                .ifMember { presentAndUntaggedVersions ->
                    if (field.type.isVariableLength && !field.type.isStruct) {
                        val callGenerateVariableLengthReader = ClauseGenerator { versions ->
                            generateVariableLengthReader(
                                fieldFlexibleVersions = fieldFlexibleVersions(field),
                                name = field.camelCaseName(),
                                type = field.type,
                                possibleVersions = versions,
                                nullableVersions = field.nullableVersions,
                                assignmentPrefix = String.format("%s = ", field.prefixedCamelCaseName()),
                                assignmentSuffix = String.format("%n"),
                                isStructArrayWithKeys = structRegistry.isStructArrayWithKeys(field),
                                zeroCopy = field.zeroCopy,
                            )
                        }
                        // For arrays where the field type needs to be serialized differently in
                        // flexible versions, lift the flexible version check outside of the array.
                        // This may mean generating two separate 'for' loops-- one for flexible
                        // versions, and one for regular versions.
                        if (
                            field.type.isArray
                            && (field.type as FieldType.ArrayType).elementType
                                .serializationIsDifferentInFlexibleVersions()
                        ) VersionConditional.forVersions(
                            fieldFlexibleVersions(field),
                            presentAndUntaggedVersions
                        )
                            .ifMember(callGenerateVariableLengthReader)
                            .ifNotMember(callGenerateVariableLengthReader)
                            .generate(buffer)
                        else callGenerateVariableLengthReader.generate(presentAndUntaggedVersions)

                    } else buffer.printf(
                        "%s = %s%n",
                        field.prefixedCamelCaseName(),
                        primitiveReadExpression(field.type),
                    )
                }
                .generate(buffer)
        }
        // TODO Unknown tagged fields may be null
        buffer.printf("this.unknownTaggedFields = null%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions)
            .ifMember { curFlexibleVersions ->

                buffer.printf("val numTaggedFields: Int = readable.readUnsignedVarint()%n")
                buffer.printf("for (i in 0 until numTaggedFields) {%n")
                buffer.incrementIndent()
                buffer.printf("val tag: Int = readable.readUnsignedVarint()%n")
                buffer.printf("val size: Int = readable.readUnsignedVarint()%n")
                buffer.printf("when (tag) {%n")
                buffer.incrementIndent()
                for (field: FieldSpec in struct.fields) {
                    val validTaggedVersions = field.versions.intersect(field.taggedVersions)
                    if (!validTaggedVersions.isEmpty) {
                        if (field.tag == null) throw RuntimeException(
                            "Field ${field.name} has tagged versions, but no tag."
                        )
                        buffer.printf("%d -> {%n", field.tag)
                        buffer.incrementIndent()
                        VersionConditional.forVersions(validTaggedVersions, curFlexibleVersions)
                            .ifMember { presentAndTaggedVersions ->
                                // All tagged fields are serialized using the new-style
                                // flexible versions serialization.
                                if (field.type.isVariableLength && !field.type.isStruct)
                                    generateVariableLengthReader(
                                        fieldFlexibleVersions = fieldFlexibleVersions(field),
                                        name = field.camelCaseName(),
                                        type = field.type,
                                        possibleVersions = presentAndTaggedVersions,
                                        nullableVersions = field.nullableVersions,
                                        assignmentPrefix =
                                        String.format("%s = ", field.prefixedCamelCaseName()),
                                        assignmentSuffix = String.format("%n"),
                                        isStructArrayWithKeys =
                                        structRegistry.isStructArrayWithKeys(field),
                                        zeroCopy = field.zeroCopy
                                    )
                                else buffer.printf(
                                    "%s = %s%n",
                                    field.prefixedCamelCaseName(),
                                    primitiveReadExpression(field.type)
                                )
                            }
                            .ifNotMember {
                                buffer.printf(
                                    "throw RuntimeException(\"Tag %d is not valid for version \$version\")%n",
                                    field.tag,
                                )
                            }
                            .generate(buffer)
                        buffer.decrementIndent()
                        buffer.printf("}%n")
                    }
                }
                buffer.printf("else -> %n")
                buffer.incrementIndent()
                buffer.printf(
                    "this.unknownTaggedFields = readable.readUnknownTaggedField(this.unknownTaggedFields?.toMutableList(), tag, size)%n"
                )
                // TODO Test this indentation
                buffer.decrementIndent()
                buffer.decrementIndent()
                buffer.printf("}%n")
                buffer.decrementIndent()
                buffer.printf("}%n")
            }.generate(buffer)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun primitiveReadExpression(type: FieldType): String = when (type) {
        is BoolFieldType -> "readable.readByte().toInt() != 0"
        is Int8FieldType -> "readable.readByte()"
        is Uint8FieldType -> "readable.readUnsignedByte()"
        is Int16FieldType -> "readable.readShort()"
        is Uint16FieldType -> "readable.readUnsignedShort().toUShort()"
        is Int32FieldType -> "readable.readInt()"
        is Uint32FieldType -> "readable.readUnsignedInt().toUInt()"
        is Int64FieldType -> "readable.readLong()"
        is Uint64FieldType -> "readable.readUnsignedLong().toULong()"
        is Float32FieldType -> "readable.readFloat()"
        is Float64FieldType -> "readable.readDouble()"
        is UUIDFieldType -> "readable.readUuid()"
        else -> if (type.isStruct) String.format("%s(readable, version)", type.toString())
        else throw RuntimeException("Unsupported field type $type")
    }

    private fun generateVariableLengthReader(
        fieldFlexibleVersions: Versions,
        name: String,
        type: FieldType,
        possibleVersions: Versions,
        nullableVersions: Versions,
        assignmentPrefix: String,
        assignmentSuffix: String,
        isStructArrayWithKeys: Boolean,
        zeroCopy: Boolean,
    ) {
        // TODO See if array need to be different
        val lengthVar = if (type.isArray) "arrayLength" else "length"
        buffer.printf("val %s: Int%n", lengthVar)
        VersionConditional.forVersions(fieldFlexibleVersions, possibleVersions).ifMember {
            buffer.printf("%s = readable.readUnsignedVarint() - 1%n", lengthVar)
        }.ifNotMember {
            if (type.isString) buffer.printf("%s = readable.readInt()%n", lengthVar)
            else if (type.isBytes || type.isArray || type.isRecords)
                buffer.printf("%s = readable.readInt()%n", lengthVar)
            else throw RuntimeException("Can't handle variable length type $type")
        }.generate(buffer)
        buffer.printf("if (%s < 0) {%n", lengthVar)
        buffer.incrementIndent()
        VersionConditional.forVersions(nullableVersions, possibleVersions).ifNotMember {
            buffer.printf(
                "throw RuntimeException(\"non-nullable field %s was serialized as null\")%n",
                name,
            )
        }.ifMember { buffer.printf("%snull%s", assignmentPrefix, assignmentSuffix) }
            .generate(buffer)
        buffer.decrementIndent()
        if (type.isString) {
            buffer.printf("} else if (%s > 0x7fff) {%n", lengthVar)
            buffer.incrementIndent()
            buffer.printf(
                "throw RuntimeException(\"string field %s had invalid length \$%s\")%n",
                name,
                lengthVar,
            )
            buffer.decrementIndent()
        }
        buffer.printf("} else {%n")
        buffer.incrementIndent()
        if (type.isString) buffer.printf(
            "%sreadable.readString(%s)%s",
            assignmentPrefix,
            lengthVar,
            assignmentSuffix,
        )
        else if (type.isBytes) {
            if (zeroCopy) buffer.printf(
                "%sreadable.readByteBuffer(%s)%s",
                assignmentPrefix,
                lengthVar,
                assignmentSuffix
            )
            else {
                buffer.printf("var newBytes: ByteArray = readable.readArray(%s)%n", lengthVar)
                buffer.printf("%snewBytes%s", assignmentPrefix, assignmentSuffix)
            }
        } else if (type.isRecords) buffer.printf(
            "%sreadable.readRecords(%s)%s",
            assignmentPrefix,
            lengthVar,
            assignmentSuffix,
        )
        else if (type.isArray) {
            val arrayType = type as FieldType.ArrayType
            buffer.printf("if (%s > readable.remaining()) {%n", lengthVar)
            buffer.incrementIndent()
            buffer.printf(
                "throw RuntimeException(\"Tried to allocate a collection of size \$%s, but there are only \${readable.remaining()} bytes remaining.\")%n",
                lengthVar,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
            if (isStructArrayWithKeys) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
                buffer.printf(
                    "var newCollection: %s = %s(%s)%n",
                    FieldSpec.collectionType(arrayType.elementType.toString()),
                    FieldSpec.collectionType(arrayType.elementType.toString()),
                    lengthVar,
                )
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
                val boxedArrayType = arrayType.elementType.getBoxedKotlinType(headerGenerator)
                buffer.printf(
                    "var newCollection: ArrayList<%s> = ArrayList(%s)%n",
                    boxedArrayType,
                    lengthVar,
                )
            }
            buffer.printf("for (i in 0 until %s) {%n", lengthVar)
            buffer.incrementIndent()
            if (arrayType.elementType.isArray) throw RuntimeException(
                "Nested arrays are not supported. Use an array of structures containing another array."
            )
            else if (arrayType.elementType.isBytes || arrayType.elementType.isString) {
                generateVariableLengthReader(
                    fieldFlexibleVersions = fieldFlexibleVersions,
                    name = "$name element",
                    type = arrayType.elementType,
                    possibleVersions = possibleVersions,
                    nullableVersions = Versions.NONE,
                    assignmentPrefix = "newCollection.add(",
                    assignmentSuffix = String.format(")%n"),
                    isStructArrayWithKeys = false,
                    zeroCopy = false,
                )
            } else buffer.printf(
                "newCollection.add(%s)%n",
                primitiveReadExpression(arrayType.elementType)
            )

            buffer.decrementIndent()
            buffer.printf("}%n")
            buffer.printf("%snewCollection%s", assignmentPrefix, assignmentSuffix)
        } else {
            throw RuntimeException("Can't handle variable length type $type")
        }
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateClassWriter(
        className: String, struct: StructSpec,
        parentVersions: Versions
    ) {
        headerGenerator.addImport(MessageGenerator.WRITABLE_CLASS)
        headerGenerator.addImport(MessageGenerator.OBJECT_SERIALIZATION_CACHE_CLASS)
        buffer.printf(
            "override fun write(writable: Writable, cache: ObjectSerializationCache, version: Short) {%n"
        )
        buffer.incrementIndent()
        VersionConditional.forVersions(struct.versions, parentVersions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw UnsupportedVersionException(\"Can't write version \$version of %s\")%n",
                    className,
                )
            }
            .generate(buffer)
        buffer.printf("var numTaggedFields = 0%n")

        val curVersions = parentVersions.intersect(struct.versions)
        val taggedFields = TreeMap<Int, FieldSpec?>()
        for (field in struct.fields) {
            val cond = VersionConditional.forVersions(field.versions, curVersions)
                .ifMember { presentVersions ->
                    VersionConditional.forVersions(field.taggedVersions, presentVersions)
                        .ifNotMember { presentAndUntaggedVersions ->
                            if (field.type.isVariableLength && !field.type.isStruct) {
                                val callGenerateVariableLengthWriter = ClauseGenerator { versions ->
                                    generateVariableLengthWriter(
                                        fieldFlexibleVersions = fieldFlexibleVersions(field),
                                        name = field.camelCaseName(),
                                        type = field.type,
                                        possibleVersions = versions,
                                        nullableVersions = field.nullableVersions,
                                        zeroCopy = field.zeroCopy
                                    )
                                }
                                // For arrays where the field type needs to be serialized
                                // differently in flexible versions, lift the flexible version check
                                // outside of the array. This may mean generating two separate 'for'
                                // loops-- one for flexible versions, and one for regular versions.
                                if (
                                    field.type.isArray
                                    && (field.type as FieldType.ArrayType).elementType
                                        .serializationIsDifferentInFlexibleVersions()
                                ) {
                                    VersionConditional.forVersions(
                                        fieldFlexibleVersions(field),
                                        presentAndUntaggedVersions,
                                    )
                                        .ifMember(callGenerateVariableLengthWriter)
                                        .ifNotMember(callGenerateVariableLengthWriter)
                                        .generate(buffer)
                                } else callGenerateVariableLengthWriter.generate(
                                    presentAndUntaggedVersions
                                )
                            } else buffer.printf(
                                "%s%n",
                                primitiveWriteExpression(field.type, field.camelCaseName()),
                            )
                        }
                        .ifMember {
                            field.generateNonDefaultValueCheck(
                                headerGenerator = headerGenerator,
                                structRegistry = structRegistry,
                                buffer = buffer,
                                fieldPrefix = "this.",
                                nullableVersions = field.nullableVersions,
                            )
                            buffer.incrementIndent()
                            buffer.printf("numTaggedFields++%n")
                            buffer.decrementIndent()
                            buffer.printf("}%n")
                            if (taggedFields.put(field.tag!!, field) != null)
                                throw RuntimeException(
                                    "Field ${field.name} has tag ${field.tag}, but another field " +
                                            "already used that tag."
                                )
                        }
                        .generate(buffer)
                }
            if (!field.ignorable) {
                cond.ifNotMember {
                    field.generateNonIgnorableFieldCheck(
                        headerGenerator = headerGenerator,
                        structRegistry = structRegistry,
                        fieldPrefix = "this.",
                        buffer = buffer,
                    )
                }
            }
            cond.generate(buffer)
        }
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_WRITER_CLASS)
        buffer.printf("val rawWriter: RawTaggedFieldWriter = RawTaggedFieldWriter.forFields(unknownTaggedFields)%n")
        buffer.printf("numTaggedFields += rawWriter.numFields()%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).ifNotMember {
            generateCheckForUnsupportedNumTaggedFields("numTaggedFields > 0")
        }.ifMember { flexibleVersions ->
            buffer.printf("writable.writeUnsignedVarint(numTaggedFields)%n")
            var prevTag: Int = -1
            for (field in taggedFields.values) {
                if (prevTag + 1 != field!!.tag)
                    buffer.printf("rawWriter.writeRawTags(writable, %d)%n", field.tag)

                VersionConditional.forVersions(
                    field.taggedVersions.intersect(field.versions),
                    flexibleVersions,
                ).allowMembershipCheckAlwaysFalse(false).ifMember { presentAndTaggedVersions ->
                    val cond = IsNullConditional.forName(field.camelCaseName())
                        .nullableVersions(field.nullableVersions)
                        .possibleVersions(presentAndTaggedVersions)
                        .alwaysEmitBlockScope(true)
                        .ifShouldNotBeNull {
                            if (field.fieldDefault != "null") {
                                field.generateNonDefaultValueCheck(
                                    headerGenerator = headerGenerator,
                                    structRegistry = structRegistry,
                                    buffer = buffer,
                                    fieldPrefix = "this.",
                                    nullableVersions = Versions.NONE,
                                )
                                buffer.incrementIndent()
                            }
                            buffer.printf("writable.writeUnsignedVarint(%d)%n", field.tag)
                            if (field.type.isString) {
                                buffer.printf(
                                    "val stringBytes: ByteArray = cache.getSerializedValue(%s)%n",
                                    field.prefixedCamelCaseName(),
                                )
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "writable.writeUnsignedVarint(stringBytes.size + ByteUtils.sizeOfUnsignedVarint(stringBytes.size + 1))%n"
                                )
                                buffer.printf("writable.writeUnsignedVarint(stringBytes.size + 1)%n")
                                buffer.printf("writable.writeByteArray(stringBytes)%n")
                            } else if (field.type.isBytes) {
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "writable.writeUnsignedVarint(%s.length + ByteUtils.sizeOfUnsignedVarint(%s.length + 1))%n",
                                    field.prefixedCamelCaseName(),
                                    field.prefixedCamelCaseName(),
                                )
                                buffer.printf(
                                    "writable.writeUnsignedVarint(%s.length + 1)%n",
                                    field.prefixedCamelCaseName(),
                                )
                                buffer.printf(
                                    "writable.writeByteArray(%s)%n",
                                    field.prefixedCamelCaseName(),
                                )
                            } else if (field.type.isArray) {
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "writable.writeUnsignedVarint(cache.getArraySizeInBytes(%s))%n",
                                    field.prefixedCamelCaseName()
                                )
                                generateVariableLengthWriter(
                                    fieldFlexibleVersions = fieldFlexibleVersions(field),
                                    name = field.camelCaseName(),
                                    type = field.type,
                                    possibleVersions = presentAndTaggedVersions,
                                    nullableVersions = Versions.NONE,
                                    zeroCopy = field.zeroCopy,
                                )
                            } else if (field.type.isStruct) {
                                buffer.printf(
                                    "writable.writeUnsignedVarint(%s.size(cache, version))%n",
                                    field.prefixedCamelCaseName(),
                                )
                                buffer.printf(
                                    "%s%n",
                                    primitiveWriteExpression(field.type, field.camelCaseName()),
                                )
                            } else if (field.type.isRecords) throw RuntimeException(
                                "Unsupported attempt to declare field `${field.name}` with " +
                                        "`records` type as a tagged field."
                            )
                            else {
                                buffer.printf(
                                    "writable.writeUnsignedVarint(%d)%n",
                                    field.type.fixedLength()
                                )
                                buffer.printf(
                                    "%s%n",
                                    primitiveWriteExpression(field.type, field.camelCaseName()),
                                )
                            }
                            if (field.fieldDefault != "null") {
                                buffer.decrementIndent()
                                buffer.printf("}%n")
                            }
                        }
                    if (field.fieldDefault != "null") {
                        cond.ifNull {
                            buffer.printf("writable.writeUnsignedVarint(%d)%n", field.tag)
                            buffer.printf("writable.writeUnsignedVarint(1)%n")
                            buffer.printf("writable.writeUnsignedVarint(0)%n")
                        }
                    }
                    cond.generate(buffer)
                }.generate(buffer)
                prevTag = field.tag!!
            }
            if (prevTag < Int.MAX_VALUE)
                buffer.printf("rawWriter.writeRawTags(writable, Integer.MAX_VALUE)%n")
        }.generate(buffer)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateCheckForUnsupportedNumTaggedFields(conditional: String) {
        buffer.printf("if (%s) {%n", conditional)
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw UnsupportedVersionException(\"Tagged fields were set, but version \$version of this message does not support them.\")%n"
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun primitiveWriteExpression(type: FieldType, name: String): String = when (type) {
        is BoolFieldType -> String.format("writable.writeByte(if (%s) 1.toByte() else 0.toByte())", name)
        is Int8FieldType -> String.format("writable.writeByte(%s)", name)
        is Uint8FieldType -> String.format("writable.writeUnsignedByte(%s.toShort())", name) // Not supported, eventually through writeUnsignedVarint
        is Int16FieldType -> String.format("writable.writeShort(%s)", name)
        is Uint16FieldType -> String.format("writable.writeUnsignedShort(%s.toInt())", name)
        is Int32FieldType -> String.format("writable.writeInt(%s)", name)
        is Uint32FieldType -> String.format("writable.writeUnsignedInt(%s.toLong())", name)
        is Int64FieldType -> String.format("writable.writeLong(%s)", name)
        is Uint64FieldType -> String.format("writable.writeUnsignedLong(%s)", name) // Not supported
        is UUIDFieldType -> String.format("writable.writeUuid(%s)", name)
        is Float32FieldType -> String.format("writable.writeFloat(%s)", name)
        is Float64FieldType -> String.format("writable.writeDouble(%s)", name)
        else -> if (type is StructType)
            String.format("%s.write(writable, cache, version)", name)
        else throw RuntimeException("Unsupported field type $type")
    }

    private fun generateVariableLengthWriter(
        fieldFlexibleVersions: Versions,
        name: String,
        type: FieldType,
        possibleVersions: Versions,
        nullableVersions: Versions,
        zeroCopy: Boolean,
    ) {
        IsNullConditional.forName(name, "this.")
            .possibleVersions(possibleVersions)
            .nullableVersions(nullableVersions)
            .alwaysEmitBlockScope(type.isString)
            .ifNull {
                VersionConditional.forVersions(nullableVersions, possibleVersions)
                    .ifMember { presentVersions ->
                        VersionConditional.forVersions(fieldFlexibleVersions, presentVersions)
                            .ifMember { buffer.printf("writable.writeUnsignedVarint(0)%n") }
                            .ifNotMember {
                                if (type.isString)
                                    buffer.printf("writable.writeShort((-1).toShort())%n")
                                else buffer.printf("writable.writeInt(-1)%n")
                            }
                            .generate(buffer)
                    }.ifNotMember { buffer.printf("throw NullPointerException()%n") }
                    .generate(buffer)
            }
            .ifShouldNotBeNull {
                val lengthExpression: String = if (type.isString) {
                    buffer.printf("val stringBytes: ByteArray = cache.getSerializedValue(%s)!!%n", name)
                    "stringBytes.size"
                } else if (type.isBytes) {
                    if (type.isNullable) {
                        if (zeroCopy) "$name.remaining()"
                        else "$name.size"
                    } else {
                        if (zeroCopy) "this.$name.remaining()"
                        else "this.$name.size"
                    }
                } else if (type.isRecords) "this.$name.sizeInBytes()"
                else if (type.isArray) {
                    if (type.isNullable) "$name.size"
                    else "this.$name.size"
                }
                else throw RuntimeException("Unhandled type $type")

                // Check whether we're dealing with a flexible version or not. In a flexible
                // version, the length is serialized differently.
                //
                // Note: for arrays, each branch of the if contains the loop for writing out
                // the elements. This allows us to lift the version check out of the loop.
                // This is helpful for things like arrays of strings, where each element
                // will be serialized differently based on whether the version is flexible.
                VersionConditional.forVersions(fieldFlexibleVersions, possibleVersions)
                    .ifMember {
                        buffer.printf("writable.writeUnsignedVarint(%s + 1)%n", lengthExpression)
                    }.ifNotMember {
                        if (type.isString)
                            buffer.printf("writable.writeShort((%s).toShort() )%n", lengthExpression)
                        else buffer.printf("writable.writeInt(%s)%n", lengthExpression)
                    }.generate(buffer)
                if (type.isString) buffer.printf("writable.writeByteArray(stringBytes)%n")
                else if (type.isBytes) {
                    if (zeroCopy) buffer.printf("writable.writeByteBuffer(%s)%n", name)
                    else buffer.printf("writable.writeByteArray(this.%s)%n", name)
                } else if (type.isRecords) buffer.printf("writable.writeRecords(this.%s)%n", name)
                else if (type.isArray) {
                    val arrayType = type as FieldType.ArrayType
                    val elementType = arrayType.elementType
                    val elementName = String.format("%sElement", name)
                    buffer.printf(
                        "for (%s in %s) {%n",
                        elementName,
                        name,
                    )
                    buffer.incrementIndent()
                    if (elementType.isArray) throw RuntimeException(
                        "Nested arrays are not supported. Use an array of structures containing " +
                                "another array.",
                    )
                    else if (elementType.isBytes || elementType.isString)
                        generateVariableLengthWriter(
                            fieldFlexibleVersions = fieldFlexibleVersions,
                            name = elementName,
                            type = elementType,
                            possibleVersions = possibleVersions,
                            nullableVersions = Versions.NONE,
                            zeroCopy = false,
                        )
                    else buffer.printf("%s%n", primitiveWriteExpression(elementType, elementName))

                    buffer.decrementIndent()
                    buffer.printf("}%n")
                }
            }
            .generate(buffer)
    }

    private fun generateClassMessageSize(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        headerGenerator.addImport(MessageGenerator.OBJECT_SERIALIZATION_CACHE_CLASS)
        headerGenerator.addImport(MessageGenerator.MESSAGE_SIZE_ACCUMULATOR_CLASS)
        buffer.printf(
            "override fun addSize(size: MessageSizeAccumulator, cache: ObjectSerializationCache, version: Short) {%n",
        )
        buffer.incrementIndent()
        buffer.printf("var numTaggedFields = 0%n")
        VersionConditional.forVersions(parentVersions, struct.versions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw UnsupportedVersionException(\"Can't size version \$version of %s\")%n",
                    className,
                )
            }
            .generate(buffer)
        val curVersions = parentVersions.intersect(struct.versions)
        for (field in struct.fields) {
            VersionConditional.forVersions(field.versions, curVersions)
                .ifMember { presentVersions ->
                    VersionConditional.forVersions(field.taggedVersions, presentVersions)
                        .ifMember { presentAndTaggedVersions ->
                            generateFieldSize(
                                field = field,
                                possibleVersions = presentAndTaggedVersions,
                                tagged = true,
                            )
                        }
                        .ifNotMember { presentAndUntaggedVersions ->
                            generateFieldSize(
                                field = field,
                                possibleVersions = presentAndUntaggedVersions,
                                tagged = false,
                            )
                        }
                        .generate(buffer)
                }
                .generate(buffer)
        }
        buffer.printf("val unknownTaggedFields = unknownTaggedFields%n")
        buffer.printf("if (unknownTaggedFields != null) {%n")
        buffer.incrementIndent()
        buffer.printf("numTaggedFields += unknownTaggedFields.size%n")
        buffer.printf("for (field in unknownTaggedFields) {%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
        buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(field.tag()))%n")
        buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(field.size))%n")
        buffer.printf("size.addBytes(field.size)%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).ifNotMember {
            generateCheckForUnsupportedNumTaggedFields("numTaggedFields > 0")
        }.ifMember {
            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
            buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(numTaggedFields))%n")
        }.generate(buffer)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    /**
     * Generate the size calculator for a variable-length array element.
     * Array elements cannot be null.
     */
    private fun generateVariableLengthArrayElementSize(
        flexibleVersions: Versions,
        fieldName: String,
        type: FieldType,
        versions: Versions,
    ) {
        when (type) {
            is StringFieldType -> {
                generateStringToBytes(fieldName)
                VersionConditional.forVersions(flexibleVersions, versions)
                    .ifNotMember { buffer.printf("size.addBytes(stringBytes.size + 2)%n") }
                    .ifMember {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf(
                            "size.addBytes(stringBytes.size + ByteUtils.sizeOfUnsignedVarint(stringBytes.size + 1))%n",
                        )
                    }
                    .generate(buffer)
            }

            is BytesFieldType -> {
                buffer.printf("size.addBytes(%s.size)%n", fieldName)
                VersionConditional.forVersions(flexibleVersions, versions)
                    .ifNotMember { buffer.printf("size.addBytes(4)%n") }
                    .ifMember {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf(
                            "size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.size + 1))%n",
                            fieldName,
                        )
                    }
                    .generate(buffer)
            }

            is StructType -> buffer.printf("%s.addSize(size, cache, version)%n", fieldName)
            else -> throw RuntimeException("Unsupported type $type")
        }
    }

    private fun generateFieldSize(
        field: FieldSpec,
        possibleVersions: Versions,
        tagged: Boolean,
    ) {
        if (field.type.fixedLength() != null) generateFixedLengthFieldSize(field, tagged)
        else generateVariableLengthFieldSize(field, possibleVersions, tagged)
    }

    private fun generateFixedLengthFieldSize(
        field: FieldSpec,
        tagged: Boolean,
    ) {
        if (tagged) {
            // Check to see that the field is not set to the default value.
            // If it is, then we don't need to serialize it.
            field.generateNonDefaultValueCheck(
                headerGenerator = headerGenerator,
                structRegistry = structRegistry,
                buffer = buffer,
                fieldPrefix = "this.",
                nullableVersions = field.nullableVersions,
            )
            buffer.incrementIndent()
            buffer.printf("numTaggedFields++%n")
            buffer.printf("size.addBytes(%d)%n", sizeOfUnsignedVarint(field.tag!!))
            // Account for the tagged field prefix length.
            buffer.printf("size.addBytes(%d)%n", sizeOfUnsignedVarint(field.type.fixedLength()!!))
            buffer.printf("size.addBytes(%d)%n", field.type.fixedLength())
            buffer.decrementIndent()
            buffer.printf("}%n")
        } else buffer.printf("size.addBytes(%d)%n", field.type.fixedLength())
    }

    private fun generateVariableLengthFieldSize(
        field: FieldSpec,
        possibleVersions: Versions,
        tagged: Boolean
    ) {
        IsNullConditional.forField(field, "this.")
            .alwaysEmitBlockScope(true)
            .possibleVersions(possibleVersions)
            .nullableVersions(field.nullableVersions)
            .ifNull {
                if (!tagged || field.fieldDefault != "null") {
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            if (tagged) {
                                buffer.printf("numTaggedFields++%n")
                                buffer.printf(
                                    "size.addBytes(%d)%n",
                                    sizeOfUnsignedVarint(field.tag!!),
                                )
                                buffer.printf(
                                    "size.addBytes(%d)%n",
                                    sizeOfUnsignedVarint(sizeOfUnsignedVarint(0)),
                                )
                            }
                            buffer.printf("size.addBytes(%d)%n", sizeOfUnsignedVarint(0))
                        }
                        .ifNotMember {
                            if (tagged) throw RuntimeException(
                                "Tagged field ${field.name} should not be present in " +
                                        "non-flexible versions.",
                            )
                            if (field.type.isString) buffer.printf("size.addBytes(2)%n")
                            else buffer.printf("size.addBytes(4)%n")
                        }
                        .generate(buffer)
                }
            }
            .ifShouldNotBeNull {
                if (tagged) {
                    if (field.fieldDefault != "null") {
                        field.generateNonDefaultValueCheck(
                            headerGenerator = headerGenerator,
                            structRegistry = structRegistry,
                            buffer = buffer,
                            fieldPrefix = "this.",
                            nullableVersions = Versions.NONE,
                        )
                        buffer.incrementIndent()
                    }
                    buffer.printf("numTaggedFields++%n")
                    buffer.printf("size.addBytes(%d)%n", sizeOfUnsignedVarint(field.tag!!))
                }
                if (field.type.isString) {
                    generateStringToBytes(field.camelCaseName())
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            if (tagged) {
                                buffer.printf(
                                    "val stringPrefixSize = ByteUtils.sizeOfUnsignedVarint(stringBytes.size + 1)%n"
                                )
                                buffer.printf(
                                    "size.addBytes(stringBytes.size + stringPrefixSize + ByteUtils.sizeOfUnsignedVarint(stringPrefixSize + stringBytes.size))%n"
                                )
                            } else buffer.printf(
                                "size.addBytes(stringBytes.size + ByteUtils.sizeOfUnsignedVarint(stringBytes.size + 1))%n"
                            )
                        }
                        .ifNotMember {
                            if (tagged) throw RuntimeException(
                                "Tagged field ${field.name} should not be present in " +
                                        "non-flexible versions.",
                            )
                            buffer.printf("size.addBytes(stringBytes.size + 2)%n")
                        }
                        .generate(buffer)
                } else if (field.type.isArray) {
                    if (tagged) buffer.printf("val sizeBeforeArray = size.totalSize()%n")
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            buffer.printf(
                                "size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.size + 1))%n",
                                field.camelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("size.addBytes(4)%n") }
                        .generate(buffer)

                    val arrayType = field.type as FieldType.ArrayType
                    val elementType = arrayType.elementType

                    if (elementType.fixedLength() != null) buffer.printf(
                        "size.addBytes(%s.size * %d)%n",
                        if (field.type.isNullable) field.camelCaseName()
                        else field.prefixedCamelCaseName(),
                        elementType.fixedLength(),
                    ) else if (elementType is FieldType.ArrayType)
                        throw RuntimeException("Arrays of arrays are not supported (use a struct).")
                    else {
                        buffer.printf(
                            "for (%sElement in %s) {%n",
                            field.camelCaseName(),
                            field.camelCaseName(),
                        )
                        buffer.incrementIndent()
                        generateVariableLengthArrayElementSize(
                            flexibleVersions = fieldFlexibleVersions(field),
                            fieldName = String.format("%sElement", field.camelCaseName()),
                            type = elementType,
                            versions = possibleVersions,
                        )
                        buffer.decrementIndent()
                        buffer.printf("}%n")
                    }
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf("val arraySize = size.totalSize() - sizeBeforeArray%n")
                        buffer.printf(
                            "cache.setArraySizeInBytes(%s, arraySize)%n",
                            field.camelCaseName(),
                        )
                        buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(arraySize))%n")
                    }
                } else if (field.type.isBytes) {
                    if (tagged) buffer.printf("var sizeBeforeBytes: Int = size.totalSize()%n")
                    if (field.zeroCopy) buffer.printf(
                        "size.addZeroCopyBytes(%s.remaining())%n",
                        field.camelCaseName()
                    )
                    else buffer.printf("size.addBytes(%s.size)%n", field.camelCaseName())

                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            if (field.zeroCopy) buffer.printf(
                                "size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.remaining() + 1))%n",
                                field.camelCaseName(),
                            )
                            else buffer.printf(
                                "size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.size + 1))%n",
                                field.camelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("size.addBytes(4)%n") }
                        .generate(buffer)
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf("val bytesSize= size.totalSize() - sizeBeforeBytes%n")
                        buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(bytesSize))%n")
                    }
                } else if (field.type.isRecords) {
                    buffer.printf(
                        "size.addZeroCopyBytes(%s.sizeInBytes())%n",
                        field.camelCaseName()
                    )
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            buffer.printf(
                                "size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.sizeInBytes() + 1))%n",
                                field.prefixedCamelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("size.addBytes(4)%n") }
                        .generate(buffer)
                } else if (field.type.isStruct) {
                    buffer.printf("val sizeBeforeStruct = size.totalSize()%n",)
                    buffer.printf(
                        "%s.addSize(size, cache, version)%n",
                        field.prefixedCamelCaseName(),
                    )
                    buffer.printf("val structSize = size.totalSize() - sizeBeforeStruct%n",)
                    if (tagged)
                        buffer.printf("size.addBytes(ByteUtils.sizeOfUnsignedVarint(structSize))%n")
                } else throw RuntimeException("unhandled type " + field.type)
                if (tagged && field.fieldDefault != "null") {
                    buffer.decrementIndent()
                    buffer.printf("}%n")
                }
            }
            .generate(buffer)
    }

    private fun generateStringToBytes(name: String) {
        buffer.printf("val stringBytes = %s.toByteArray(Charsets.UTF_8)%n", name)
        buffer.printf("if (stringBytes.size > 0x7fff) {%n")
        buffer.incrementIndent()
        buffer.printf(
            "throw RuntimeException(\"'%s' field is too long to be serialized\")%n",
            name,
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("cache.cacheSerializedValue(%s, stringBytes)%n", name)
    }

    private fun generateClassEquals(
        className: String,
        struct: StructSpec,
        elementKeysAreEqual: Boolean,
    ) {
        buffer.printf(
            "override fun %s(other: Any?): Boolean {%n",
            if (elementKeysAreEqual) "elementKeysAreEqual" else "equals",
        )
        buffer.incrementIndent()
        buffer.printf("if (this === other) return true%n")
        buffer.printf("if (javaClass != other?.javaClass) return false%n")
        buffer.printf("other as %s%n", className)
        if (struct.fields.isNotEmpty()) {
            for (field in struct.fields)
                if (!elementKeysAreEqual || field.mapKey) generateFieldEquals(field)
        }
        if (elementKeysAreEqual) buffer.printf("return true%n")
        else {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            buffer.printf(
                "return MessageUtil.compareRawTaggedFields(unknownTaggedFields, " +
                        "other.unknownTaggedFields)%n",
            )
        }
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldEquals(field: FieldSpec) {
        when {
            field.type is BoolFieldType
            || field.type is Int8FieldType
            || field.type is Uint8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Int32FieldType
            || field.type is Uint32FieldType
            || field.type is Int64FieldType
            || field.type is Uint64FieldType
            || field.type is Float32FieldType
            || field.type is Float64FieldType
            || field.type is UUIDFieldType
            || field.type is StringFieldType
            || field.type.isStructArray -> buffer.printf(
                "if (%s != other.%s) return false%n",
                field.prefixedCamelCaseName(),
                field.camelCaseName(),
            )
            else -> {
                if (field.type.isArray) {
                    if (!field.type.isNullable) buffer.printf(
                        // not nullable array field
                        "if (%s != other.%s) return false%n", // TODO Differentiate between primitive entityType and non-primitive entityType
                        field.camelCaseName(),
                        field.camelCaseName(),
                    )
                    else {
                        // Nullable array field
                        buffer.printf("if (%s != null) {%n", field.prefixedCamelCaseName())
                        buffer.incrementIndent()
                        buffer.printf(
                            "if (other.%s == null) return false%n",
                            field.camelCaseName(),
                        )
                        buffer.printf(
                            "if (%s != other.%s) return false%n",
                            field.prefixedCamelCaseName(),
                            field.camelCaseName(),
                        )
                        buffer.decrementIndent()
                        buffer.printf(
                            "} else if (other.%s != null) return false%n",
                            field.camelCaseName(),
                        )
                    }
                } else buffer.printf(
                    // similar as numbers, includes field.type.isBytes, field.type.isStruct, field.type.isRecords
                    "if (%s != other.%s) return false%n",
                    field.camelCaseName(),
                    field.camelCaseName(),
                )
            }
        }
    }

    private fun generateClassHashCode(struct: StructSpec, onlyMapKeys: Boolean) {
        buffer.printf("override fun hashCode(): Int {%n")
        buffer.incrementIndent()
        buffer.printf("var hashCode = 0%n")
        for (field: FieldSpec in struct.fields)
            if ((!onlyMapKeys) || field.mapKey) generateFieldHashCode(field)

        buffer.printf("return hashCode%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    // TODO hashcode and equals may be skipped for data classes that do not contain ByteArray
    private fun generateFieldHashCode(field: FieldSpec) {
        when (field.type) {
            is BoolFieldType,
            is Uint8FieldType,
            is Uint16FieldType,
            is Uint32FieldType,
            is Int64FieldType,
            is Uint64FieldType,
            is Float32FieldType,
            is Float64FieldType,
            is UUIDFieldType ->
                buffer.printf("hashCode = 31 * hashCode + %s.hashCode()%n", field.prefixedCamelCaseName())

            is Int8FieldType,
            is Int16FieldType,
            is Int32FieldType ->
                buffer.printf("hashCode = 31 * hashCode + %s%n", field.prefixedCamelCaseName())

            else -> {
                if (field.type.isBytes) buffer.printf(
                    "hashCode = 31 * hashCode + %s.hashCode()%n",
                    field.prefixedCamelCaseName(),
                )
                else if (field.type.isRecords) buffer.printf(
                    // TODO Validate if records can use hashCode()
                    "hashCode = 31 * hashCode + %s.hashCode()%n",
                    field.prefixedCamelCaseName(),
                )
                else if (field.type.isStruct || field.type.isArray || field.type.isString)
                // TODO Consider moving nullable logic to all hashcode and equals calls
                    buffer.printf(
                        "hashCode = 31 * hashCode + %s%s%s.hashCode()%s%n",
                        if (field.type.isNullable) "(" else "",
                        field.prefixedCamelCaseName(),
                        if (field.type.isNullable) "?" else "",
                        if (field.type.isNullable) " ?: 0)" else "",
                    )
                else throw RuntimeException("Unsupported field type " + field.type)
            }
        }
    }

    // TODO Consider using data class copy instead
    private fun generateClassDuplicate(className: String, struct: StructSpec) {
        buffer.printf("override fun duplicate(): %s {%n", className)
        buffer.incrementIndent()
        buffer.printf("val duplicate = %s()%n", className, className)

        for (field: FieldSpec in struct.fields) generateFieldDuplicate(
            Target(
                field = field,
                sourceVariable = field.camelCaseName(),
                humanReadableName = field.camelCaseName(),
            ) { input -> String.format("duplicate.%s = %s", field.camelCaseName(), input) }
        )

        buffer.printf("return duplicate%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldDuplicate(target: Target) {
        val field = target.field
        if (
            field.type is BoolFieldType
            || field.type is Int8FieldType
            || field.type is Uint8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Int32FieldType
            || field.type is Uint32FieldType
            || field.type is Int64FieldType
            || field.type is Uint64FieldType
            || field.type is Float32FieldType
            || field.type is Float64FieldType
            || field.type is UUIDFieldType
        ) buffer.printf("%s%n", target.assignmentStatement(target.sourceVariable))
        else {
            val cond = IsNullConditional.forName(target.sourceVariable, "this.")
                .nullableVersions(target.field.nullableVersions)
                .ifNull { buffer.printf("%s%n", target.assignmentStatement("null")) }
            if (field.type.isBytes) {
                if (field.zeroCopy) {
                    cond.ifShouldNotBeNull {
                        buffer.printf(
                            "%s%n",
                            target.assignmentStatement(
                                // TODO Use copy instead
                                String.format("%s.duplicate()", target.sourceVariable)
                            ),
                        )
                    }
                } else cond.ifShouldNotBeNull {
                    headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                    buffer.printf(
                        "%s%n",
                        target.assignmentStatement(
                            // TODO Use copy instead
                            String.format("MessageUtil.duplicate(%s)", target.sourceVariable)
                        ),
                    )
                }
            } else if (field.type.isRecords) {
                cond.ifShouldNotBeNull {
                    headerGenerator.addImport(MessageGenerator.MEMORY_RECORDS_CLASS)
                    buffer.printf(
                        "%s%n",
                        target.assignmentStatement(
                            // TODO Use copy instead
                            "MemoryRecords.readableRecords((${target.sourceVariable} as MemoryRecords).buffer().duplicate())"
                        )
                    )
                }
            } else if (field.type.isStruct) cond.ifShouldNotBeNull {
                buffer.printf(
                    "%s%n",
                    // TODO Use copy instead
                    target.assignmentStatement("${target.sourceVariable}.duplicate()")
                )
            } else if (field.type.isString) {
                // Strings are immutable, so we don't need to duplicate them.
                cond.ifShouldNotBeNull {
                    // TODO Use copy instead
                    buffer.printf("%s%n", target.assignmentStatement(target.sourceVariable))
                }
            } else if (field.type.isArray) {
                cond.ifShouldNotBeNull {
                    val newArrayName = "new${field.capitalizedCamelCaseName()}"
                    val type = field.concreteKotlinType(headerGenerator, structRegistry)
                    buffer.printf(
                        "val %s = %s(%s.size)%n",
                        newArrayName,
                        type,
                        target.sourceVariable,
                    )
                    buffer.printf("for (element in %s) {%n", target.sourceVariable)
                    buffer.incrementIndent()
                    generateFieldDuplicate(
                        target.arrayElementTarget { input ->
                            // TODO optimize with mapping function instead
                            String.format("%s.add(%s)", newArrayName, input)
                        }
                    )
                    buffer.decrementIndent()
                    buffer.printf("}%n")
                    buffer.printf(
                        "%s%n",
                        target.assignmentStatement("new${field.capitalizedCamelCaseName()}"),
                    )
                }
            } else throw RuntimeException("Unhandled field type " + field.type)
            cond.generate(buffer)
        }
    }

    // TODO toString function can be removed if data class is used
    private fun generateClassToString(className: String, struct: StructSpec) {
        buffer.printf("override fun toString(): String {%n")
        buffer.incrementIndent()
        buffer.printf("return \"%s(\" +%n", className)
        buffer.incrementIndent()
        var prefix = ""
        for (field in struct.fields) {
            generateFieldToString(prefix, field)
            prefix = ", "
        }
        buffer.printf("\")\"%n")
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldToString(prefix: String, field: FieldSpec) {
        if (
            field.type is BoolFieldType
            || field.type is Int8FieldType
            || field.type is Uint8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Int32FieldType
            || field.type is Uint32FieldType
            || field.type is Int64FieldType
            || field.type is Uint64FieldType
            || field.type is Float32FieldType
            || field.type is Float64FieldType
        ) {
            buffer.printf(
                "\"%s%s=\" + %s +%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName()
            )
        } else if (field.type.isString) buffer.printf(
            "\"%s%s=\" + if (%s == null) \"null\" else \"'\$%s'\" +%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
            field.camelCaseName(),
        ) else if (field.type.isBytes) {
            if (field.zeroCopy) buffer.printf(
                "\"%s%s=\" + %s +%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
            else {
                buffer.printf(
                    "\"%s%s=\" + %s.toString() +%n",
                    prefix,
                    field.camelCaseName(),
                    field.camelCaseName(),
                )
            }
        } else if (field.type.isRecords) buffer.printf(
            "\"%s%s=\" + %s +%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
        )
        else if (field.type is UUIDFieldType || field.type.isStruct) buffer.printf(
            "\"%s%s=\" + %s.toString() +%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
        )
        else if (field.type.isArray) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            if (field.nullableVersions.isEmpty) buffer.printf(
                "\"%s%s=\" + MessageUtil.deepToString(%s.iterator()) +%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
            else buffer.printf(
                "\"%s%s=\" + %s?.iterator()?.let { MessageUtil.deepToString(it) } +%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else throw RuntimeException("Unsupported field type " + field.type)
    }

    private fun generateFieldAccessor(field: FieldSpec) {
        buffer.printf("%n")
        generateAccessor(
            kotlinType = field.fieldAbstractKotlinType(headerGenerator, structRegistry),
            functionName = field.camelCaseName(),
            memberName = field.camelCaseName(),
        )
    }

    // TODO Accessors may be removed if data class and public val properties
    private fun generateAccessor(
        kotlinType: String,
        functionName: String,
        memberName: String,
        isOverride: Boolean = false,
    ) {
        // TODO rename e.g. next() to getNext() (see generateFieldMutator or generateSetter)
        buffer.printf("${if (isOverride) "override " else ""}fun %s(): %s {%n", functionName, kotlinType)
        buffer.incrementIndent()
        buffer.printf("return this.%s%n", memberName)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldMutator(className: String, field: FieldSpec) {
        buffer.printf("%n")
        buffer.printf(
            "fun set%s(v: %s): %s {%n",
            field.capitalizedCamelCaseName(),
            field.fieldAbstractKotlinType(headerGenerator, structRegistry),
            className,
        )
        buffer.incrementIndent()
        // Note: Unsigned fields do not need validation if proper field type is used
        buffer.printf("%s = v%n", field.prefixedCamelCaseName())
        buffer.printf("return this%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateSetter(
        kotlinType: String,
        functionName: String,
        memberName: String,
        isOverride: Boolean = false,
    ) {
        buffer.printf(
            "%sfun %s(%s: %s) {%n",
            if (isOverride) "override " else "",
            functionName,
            memberName,
            kotlinType,
        )
        buffer.incrementIndent()
        buffer.printf("this.%s = %s%n", memberName, memberName)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun fieldFlexibleVersions(field: FieldSpec): Versions {
        val flexibleVersions = field.flexibleVersions
        return if (flexibleVersions != null) {
            if (messageFlexibleVersions.intersect(flexibleVersions) != flexibleVersions)
                throw RuntimeException(
                    "The flexible versions for field ${field.name} are $flexibleVersions, which " +
                            "are not a subset of the flexible versions for the " +
                            "message as a whole, which are $messageFlexibleVersions"
                )
            flexibleVersions
        } else messageFlexibleVersions
    }
}
