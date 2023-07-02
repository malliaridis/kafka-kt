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
import org.apache.kafka.message.MessageGenerator.sizeOfUnsignedVarint
import java.io.BufferedWriter
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors

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
            Optional.of(message),
            message.dataClassName(),
            message.struct,
            message.struct.versions
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
        topLevelMessageSpec: Optional<MessageSpec>,
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        buffer.printf("%n")
        val isTopLevel = topLevelMessageSpec.isPresent
        val isSetElement = struct.hasKeys // Check if the class is inside a set.
        if (isTopLevel && isSetElement)
            throw RuntimeException("Cannot set mapKey on top level fields.")

        generateClassHeader(className, isTopLevel, isSetElement)
        buffer.incrementIndent()
        generateFieldDeclarations(struct, isSetElement)
        buffer.printf("%n")
        schemaGenerator.writeSchema(className, buffer)
        generateClassConstructors(className, struct, isSetElement)
        buffer.printf("%n")
        if (isTopLevel) {
            generateShortAccessor(
                "apiKey",
                topLevelMessageSpec.get().apiKey ?: (-1).toShort(),
            )
        }
        buffer.printf("%n")
        generateShortAccessor("lowestSupportedVersion", parentVersions.lowest)
        buffer.printf("%n")
        generateShortAccessor("highestSupportedVersion", parentVersions.highest)
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
                    Optional.empty(),
                    commonStruct.name,
                    commonStruct,
                    commonStruct.versions
                )
            }
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    private fun generateClassHeader(
        className: String, isTopLevel: Boolean,
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
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
            implementedInterfaces.add("ImplicitLinkedHashMultiCollection.Element")
        }
        val classModifiers: MutableSet<String> = LinkedHashSet()
        classModifiers.add("public")
        if (!isTopLevel) classModifiers.add("static")

        buffer.printf(
            "%s class %s implements %s {%n",
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
                if (!structRegistry.commonStructNames().contains(arrayType.elementName())) {
                    generateClass(
                        topLevelMessageSpec = Optional.empty(),
                        className = arrayType.elementType().toString(),
                        struct = structRegistry.findStruct(field),
                        parentVersions = parentVersions.intersect(struct.versions),
                    )
                }
            } else if (field.type.isStruct) {
                if (!structRegistry.commonStructNames().contains(field.typeString)) {
                    generateClass(
                        topLevelMessageSpec = Optional.empty(),
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
            "public static class %s extends ImplicitLinkedHashMultiCollection<%s> {%n",
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
        generateHashSetFindMethod(FieldSpec.toString(), struct)
        buffer.printf("%n")
        generateHashSetFindAllMethod(className, struct)
        buffer.printf("%n")
        generateCollectionDuplicateMethod(className)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetZeroArgConstructor(className: String) {
        buffer.printf("public %s() {%n", FieldSpec.collectionType(className))
        buffer.incrementIndent()
        buffer.printf("super();%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetSizeArgConstructor(className: String) {
        buffer.printf(
            "public %s(int expectedNumElements) {%n",
            FieldSpec.collectionType(className),
        )
        buffer.incrementIndent()
        buffer.printf("super(expectedNumElements);%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetIteratorConstructor(className: String) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS)
        buffer.printf(
            "public %s(Iterator<%s> iterator) {%n",
            FieldSpec.collectionType(className),
            className,
        )
        buffer.incrementIndent()
        buffer.printf("super(iterator);%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetFindMethod(className: String, struct: StructSpec) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf(
            "public %s find(%s) {%n",
            className,
            commaSeparatedHashSetFieldAndTypes(struct),
        )
        buffer.incrementIndent()
        generateKeyElement(className, struct)
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
        buffer.printf("return find(_key);%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateHashSetFindAllMethod(className: String, struct: StructSpec) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf(
            "public List<%s> findAll(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct),
        )
        buffer.incrementIndent()
        generateKeyElement(className, struct)
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
        buffer.printf("return findAll(_key);%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateKeyElement(className: String, struct: StructSpec) {
        buffer.printf("%s _key = new %s();%n", className, className)
        for (field: FieldSpec in struct.fields) {
            if (field.mapKey) {
                buffer.printf(
                    "_key.set%s(%s);%n",
                    field.capitalizedCamelCaseName(),
                    field.camelCaseName(),
                )
            }
        }
    }

    private fun commaSeparatedHashSetFieldAndTypes(struct: StructSpec): String {
        return struct.fields.filter { f -> f.mapKey }.joinToString(", ") { spec ->
            String.format(
                "%s %s",
                spec.concreteJavaType(headerGenerator, structRegistry),
                spec.camelCaseName(),
            )
        }
    }

    private fun generateCollectionDuplicateMethod(className: String) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        buffer.printf("public %s duplicate() {%n", FieldSpec.collectionType(className))
        buffer.incrementIndent()
        buffer.printf(
            "%s _duplicate = new %s(size());%n",
            FieldSpec.collectionType(className),
            FieldSpec.collectionType(className),
        )
        buffer.printf("for (%s _element : this) {%n", className)
        buffer.incrementIndent()
        buffer.printf("_duplicate.add(_element.duplicate());%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("return _duplicate;%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldDeclarations(struct: StructSpec, isSetElement: Boolean) {
        for (field in struct.fields) generateFieldDeclaration(field)

        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS)
        buffer.printf("private List<RawTaggedField> _unknownTaggedFields;%n")
        if (isSetElement) {
            buffer.printf("private int next;%n")
            buffer.printf("private int prev;%n")
        }
    }

    private fun generateFieldDeclaration(field: FieldSpec) {
        buffer.printf(
            "%s %s;%n",
            field.fieldAbstractJavaType(headerGenerator, structRegistry),
            field.camelCaseName(),
        )
    }

    private fun generateFieldAccessors(struct: StructSpec, isSetElement: Boolean) {
        for (field in struct.fields) generateFieldAccessor(field)

        if (isSetElement) {
            buffer.printf("%n")
            buffer.printf("@Override%n")
            generateAccessor("int", "next", "next")
            buffer.printf("%n")
            buffer.printf("@Override%n")
            generateAccessor("int", "prev", "prev")
        }
    }

    private fun generateUnknownTaggedFieldsAccessor() {
        buffer.printf("@Override%n")
        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS)
        buffer.printf("public List<RawTaggedField> unknownTaggedFields() {%n")
        buffer.incrementIndent()
        // Optimize _unknownTaggedFields by not creating a new list object
        // unless we need it.
        buffer.printf("if (_unknownTaggedFields == null) {%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
        buffer.printf("_unknownTaggedFields = new ArrayList<>(0);%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("return _unknownTaggedFields;%n")
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
            buffer.printf("@Override%n")
            generateSetter("int", "setNext", "next")
            buffer.printf("%n")
            buffer.printf("@Override%n")
            generateSetter("int", "setPrev", "prev")
        }
    }

    private fun generateClassConstructors(
        className: String,
        struct: StructSpec,
        isSetElement: Boolean,
    ) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS)
        buffer.printf("public %s(Readable _readable, short _version) {%n", className)
        buffer.incrementIndent()
        buffer.printf("read(_readable, _version);%n")
        generateConstructorEpilogue(isSetElement)
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("%n")
        buffer.printf("public %s() {%n", className)
        buffer.incrementIndent()
        for (field in struct.fields) buffer.printf(
            "this.%s = %s;%n",
            field.camelCaseName(),
            field.fieldDefault(headerGenerator, structRegistry)
        )
        generateConstructorEpilogue(isSetElement)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateConstructorEpilogue(isSetElement: Boolean) {
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_COLLECTION_CLASS)
            buffer.printf("this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;%n")
            buffer.printf("this.next = ImplicitLinkedHashCollection.INVALID_INDEX;%n")
        }
    }

    private fun generateShortAccessor(name: String, `val`: Short) {
        buffer.printf("@Override%n")
        buffer.printf("public short %s() {%n", name)
        buffer.incrementIndent()
        buffer.printf("return %d;%n", `val`)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateClassReader(
        className: String,
        struct: StructSpec,
        parentVersions: Versions,
    ) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS)
        buffer.printf("@Override%n")
        buffer.printf("public void read(Readable _readable, short _version) {%n")
        buffer.incrementIndent()
        VersionConditional.forVersions(parentVersions, struct.versions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw new UnsupportedVersionException(\"Can't read " +
                            "version \" + _version + \" of %s\");%n", className
                )
            }
            .generate(buffer)
        val curVersions = parentVersions.intersect(struct.versions)
        for (field: FieldSpec in struct.fields) {
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
                        "this.%s = %s;%n",
                        field.camelCaseName(),
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
                                assignmentPrefix = "this.${field.camelCaseName()} = ",
                                assignmentSuffix = String.format(";%n"),
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
                            && (field.type as FieldType.ArrayType).elementType()
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
                        "this.%s = %s;%n",
                        field.camelCaseName(),
                        primitiveReadExpression(field.type),
                    )
                }
                .generate(buffer)
        }
        buffer.printf("this._unknownTaggedFields = null;%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions)
            .ifMember { curFlexibleVersions ->
                buffer.printf("int _numTaggedFields = _readable.readUnsignedVarint();%n")
                buffer.printf("for (int _i = 0; _i < _numTaggedFields; _i++) {%n")
                buffer.incrementIndent()
                buffer.printf("int _tag = _readable.readUnsignedVarint();%n")
                buffer.printf("int _size = _readable.readUnsignedVarint();%n")
                buffer.printf("switch (_tag) {%n")
                buffer.incrementIndent()
                for (field: FieldSpec in struct.fields) {
                    val validTaggedVersions = field.versions.intersect(field.taggedVersions)
                    if (!validTaggedVersions.empty()) {
                        if (field.tag == null) throw RuntimeException(
                            "Field ${field.name} has tagged versions, but no tag."
                        )
                        buffer.printf("case %d: {%n", field.tag)
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
                                        String.format("this.%s = ", field.camelCaseName()),
                                        assignmentSuffix = String.format(";%n"),
                                        isStructArrayWithKeys =
                                        structRegistry.isStructArrayWithKeys(field),
                                        zeroCopy = field.zeroCopy
                                    )
                                else buffer.printf(
                                    "this.%s = %s;%n", field.camelCaseName(),
                                    primitiveReadExpression(field.type)
                                )
                                buffer.printf("break;%n")
                            }
                            .ifNotMember {
                                buffer.printf(
                                    "throw new RuntimeException(\"Tag %d is not " +
                                            "valid for version \" + _version);%n",
                                    field.tag,
                                )
                            }
                            .generate(buffer)
                        buffer.decrementIndent()
                        buffer.printf("}%n")
                    }
                }
                buffer.printf("default:%n")
                buffer.incrementIndent()
                buffer.printf(
                    "this._unknownTaggedFields = _readable.readUnknownTaggedField(" +
                            "this._unknownTaggedFields, _tag, _size);%n"
                )
                buffer.printf("break;%n")
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
        is BoolFieldType -> "_readable.readByte() != 0"
        is Int8FieldType -> "_readable.readByte()"
        is Int16FieldType -> "_readable.readShort()"
        is Uint16FieldType -> "_readable.readUnsignedShort()"
        is Uint32FieldType -> "_readable.readUnsignedInt()"
        is Int32FieldType -> "_readable.readInt()"
        is Int64FieldType -> "_readable.readLong()"
        is UUIDFieldType -> "_readable.readUuid()"
        is Float64FieldType -> "_readable.readDouble()"
        else -> if (type.isStruct) String.format("new %s(_readable, _version)", type.toString())
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
        val lengthVar = if (type.isArray) "arrayLength" else "length"
        buffer.printf("int %s;%n", lengthVar)
        VersionConditional.forVersions(fieldFlexibleVersions, possibleVersions).ifMember {
            buffer.printf("%s = _readable.readUnsignedVarint() - 1;%n", lengthVar)
        }.ifNotMember {
            if (type.isString) buffer.printf("%s = _readable.readShort();%n", lengthVar)
            else if (type.isBytes || type.isArray || type.isRecords)
                buffer.printf("%s = _readable.readInt();%n", lengthVar)
            else throw RuntimeException("Can't handle variable length type " + type)
        }.generate(buffer)
        buffer.printf("if (%s < 0) {%n", lengthVar)
        buffer.incrementIndent()
        VersionConditional.forVersions(nullableVersions, possibleVersions).ifNotMember {
            buffer.printf(
                "throw new RuntimeException(\"non-nullable field %s was serialized as null\");%n",
                name,
            )
        }.ifMember {
            buffer.printf("%snull%s", assignmentPrefix, assignmentSuffix)
        }.generate(buffer)
        buffer.decrementIndent()
        if (type.isString) {
            buffer.printf("} else if (%s > 0x7fff) {%n", lengthVar)
            buffer.incrementIndent()
            buffer.printf(
                "throw new RuntimeException(\"string field %s had invalid length \" + %s);%n",
                name,
                lengthVar,
            )
            buffer.decrementIndent()
        }
        buffer.printf("} else {%n")
        buffer.incrementIndent()
        if (type.isString) buffer.printf(
            "%s_readable.readString(%s)%s",
            assignmentPrefix,
            lengthVar,
            assignmentSuffix,
        )
        else if (type.isBytes) {
            if (zeroCopy) buffer.printf(
                "%s_readable.readByteBuffer(%s)%s",
                assignmentPrefix, lengthVar, assignmentSuffix
            )
            else {
                buffer.printf("byte[] newBytes = _readable.readArray(%s);%n", lengthVar)
                buffer.printf("%snewBytes%s", assignmentPrefix, assignmentSuffix)
            }
        } else if (type.isRecords) buffer.printf(
            "%s_readable.readRecords(%s)%s",
            assignmentPrefix,
            lengthVar,
            assignmentSuffix,
        )
        else if (type.isArray) {
            val arrayType = type as FieldType.ArrayType
            buffer.printf("if (%s > _readable.remaining()) {%n", lengthVar)
            buffer.incrementIndent()
            buffer.printf(
                "throw new RuntimeException(\"Tried to allocate a collection of size \" + %s + " +
                        "\", but there are only \" + _readable.remaining() + \" bytes " +
                        "remaining.\");%n",
                lengthVar,
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
            if (isStructArrayWithKeys) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
                buffer.printf(
                    "%s newCollection = new %s(%s);%n",
                    FieldSpec.collectionType(arrayType.elementType().toString()),
                    FieldSpec.collectionType(arrayType.elementType().toString()), lengthVar
                )
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
                val boxedArrayType = arrayType.elementType().getBoxedJavaType(headerGenerator)
                buffer.printf(
                    "ArrayList<%s> newCollection = new ArrayList<>(%s);%n",
                    boxedArrayType,
                    lengthVar
                )
            }
            buffer.printf("for (int i = 0; i < %s; i++) {%n", lengthVar)
            buffer.incrementIndent()
            if (arrayType.elementType().isArray) throw RuntimeException(
                "Nested arrays are not supported. Use an array of structures containing another array."
            )
            else if (arrayType.elementType().isBytes || arrayType.elementType().isString) {
                generateVariableLengthReader(
                    fieldFlexibleVersions = fieldFlexibleVersions,
                    name = "$name element",
                    type = arrayType.elementType(),
                    possibleVersions = possibleVersions,
                    nullableVersions = Versions.NONE,
                    assignmentPrefix = "newCollection.add(",
                    assignmentSuffix = String.format(");%n"),
                    isStructArrayWithKeys = false,
                    zeroCopy = false,
                )
            } else buffer.printf(
                "newCollection.add(%s);%n",
                primitiveReadExpression(arrayType.elementType())
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
        buffer.printf("@Override%n")
        buffer.printf(
            "public void write(Writable _writable, ObjectSerializationCache _cache, short " +
                    "_version) {%n"
        )
        buffer.incrementIndent()
        VersionConditional.forVersions(struct.versions, parentVersions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw new UnsupportedVersionException(\"Can't write version \" + _version + " +
                            "\" of %s\");%n",
                    className,
                )
            }
            .generate(buffer)
        buffer.printf("int _numTaggedFields = 0;%n")

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
                                    && (field.type as FieldType.ArrayType).elementType()
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
                                "%s;%n",
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
                            buffer.printf("_numTaggedFields++;%n")
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
        buffer.printf("RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);%n")
        buffer.printf("_numTaggedFields += _rawWriter.numFields();%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).ifNotMember {
            generateCheckForUnsupportedNumTaggedFields("_numTaggedFields > 0")
        }.ifMember { flexibleVersions ->
            buffer.printf("_writable.writeUnsignedVarint(_numTaggedFields);%n")
            var prevTag: Int = -1
            for (field in taggedFields.values) {
                if (prevTag + 1 != field!!.tag)
                    buffer.printf("_rawWriter.writeRawTags(_writable, %d);%n", field.tag)

                VersionConditional.forVersions(
                    field.taggedVersions.intersect(field.versions),
                    flexibleVersions,
                ).allowMembershipCheckAlwaysFalse(false).ifMember { presentAndTaggedVersions ->
                    val cond: IsNullConditional = IsNullConditional.forName(field.camelCaseName())
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
                            buffer.printf("_writable.writeUnsignedVarint(%d);%n", field.tag)
                            if (field.type.isString) {
                                buffer.printf(
                                    "byte[] _stringBytes = _cache.getSerializedValue(this.%s);%n",
                                    field.camelCaseName(),
                                )
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "_writable.writeUnsignedVarint(_stringBytes.length + " +
                                            "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n"
                                )
                                buffer.printf("_writable.writeUnsignedVarint(_stringBytes.length + 1);%n")
                                buffer.printf("_writable.writeByteArray(_stringBytes);%n")
                            } else if (field.type.isBytes) {
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "_writable.writeUnsignedVarint(this.%s.length + " +
                                            "ByteUtils.sizeOfUnsignedVarint(this.%s.length + 1));%n",
                                    field.camelCaseName(),
                                    field.camelCaseName(),
                                )
                                buffer.printf(
                                    "_writable.writeUnsignedVarint(this.%s.length + 1);%n",
                                    field.camelCaseName(),
                                )
                                buffer.printf(
                                    "_writable.writeByteArray(this.%s);%n",
                                    field.camelCaseName(),
                                )
                            } else if (field.type.isArray) {
                                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                                buffer.printf(
                                    "_writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.%s));%n",
                                    field.camelCaseName()
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
                                    "_writable.writeUnsignedVarint(this.%s.size(_cache, _version));%n",
                                    field.camelCaseName(),
                                )
                                buffer.printf(
                                    "%s;%n",
                                    primitiveWriteExpression(field.type, field.camelCaseName()),
                                )
                            } else if (field.type.isRecords) throw RuntimeException(
                                "Unsupported attempt to declare field `${field.name}` with " +
                                        "`records` type as a tagged field."
                            )
                            else {
                                buffer.printf(
                                    "_writable.writeUnsignedVarint(%d);%n",
                                    field.type.fixedLength()
                                )
                                buffer.printf(
                                    "%s;%n",
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
                            buffer.printf("_writable.writeUnsignedVarint(%d);%n", field.tag)
                            buffer.printf("_writable.writeUnsignedVarint(1);%n")
                            buffer.printf("_writable.writeUnsignedVarint(0);%n")
                        }
                    }
                    cond.generate(buffer)
                }.generate(buffer)
                prevTag = field.tag!!
            }
            if (prevTag < Int.MAX_VALUE)
                buffer.printf("_rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);%n")
        }.generate(buffer)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateCheckForUnsupportedNumTaggedFields(conditional: String) {
        buffer.printf("if (%s) {%n", conditional)
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw new UnsupportedVersionException(\"Tagged fields were set, but version \" + " +
                    "_version + \" of this message does not support them.\");%n"
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun primitiveWriteExpression(type: FieldType, name: String): String = when (type) {
        is BoolFieldType -> String.format("_writable.writeByte(%s ? (byte) 1 : (byte) 0)", name)
        is Int8FieldType -> String.format("_writable.writeByte(%s)", name)
        is Int16FieldType -> String.format("_writable.writeShort(%s)", name)
        is Uint16FieldType -> String.format("_writable.writeUnsignedShort(%s)", name)
        is Uint32FieldType -> String.format("_writable.writeUnsignedInt(%s)", name)
        is Int32FieldType -> String.format("_writable.writeInt(%s)", name)
        is Int64FieldType -> String.format("_writable.writeLong(%s)", name)
        is UUIDFieldType -> String.format("_writable.writeUuid(%s)", name)
        is Float64FieldType -> String.format("_writable.writeDouble(%s)", name)
        else -> if (type is StructType)
            String.format("%s.write(_writable, _cache, _version)", name)
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
        IsNullConditional.forName(name)
            .possibleVersions(possibleVersions)
            .nullableVersions(nullableVersions)
            .alwaysEmitBlockScope(type.isString)
            .ifNull {
                VersionConditional.forVersions(nullableVersions, possibleVersions)
                    .ifMember { presentVersions ->
                        VersionConditional.forVersions(fieldFlexibleVersions, presentVersions)
                            .ifMember { buffer.printf("_writable.writeUnsignedVarint(0);%n") }
                            .ifNotMember {
                                if (type.isString)
                                    buffer.printf("_writable.writeShort((short) -1);%n")
                                else buffer.printf("_writable.writeInt(-1);%n")
                            }
                            .generate(buffer)
                    }.ifNotMember { buffer.printf("throw new NullPointerException();%n") }
                    .generate(buffer)
            }
            .ifShouldNotBeNull {
                val lengthExpression: String = if (type.isString) {
                    buffer.printf("byte[] _stringBytes = _cache.getSerializedValue(%s);%n", name)
                    "_stringBytes.length"
                } else if (type.isBytes) {
                    if (zeroCopy) "$name.remaining()"
                    else "$name.length"
                } else if (type.isRecords) "$name.sizeInBytes()"
                else if (type.isArray) "$name.size()"
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
                        buffer.printf("_writable.writeUnsignedVarint(%s + 1);%n", lengthExpression)
                    }.ifNotMember {
                        if (type.isString)
                            buffer.printf("_writable.writeShort((short) %s);%n", lengthExpression)
                        else buffer.printf("_writable.writeInt(%s);%n", lengthExpression)
                    }.generate(buffer)
                if (type.isString) buffer.printf("_writable.writeByteArray(_stringBytes);%n")
                else if (type.isBytes) {
                    if (zeroCopy) buffer.printf("_writable.writeByteBuffer(%s);%n", name)
                    else buffer.printf("_writable.writeByteArray(%s);%n", name)
                } else if (type.isRecords) buffer.printf("_writable.writeRecords(%s);%n", name)
                else if (type.isArray) {
                    val arrayType = type as FieldType.ArrayType
                    val elementType: FieldType = arrayType.elementType()
                    val elementName: String = String.format("%sElement", name)
                    buffer.printf(
                        "for (%s %s : %s) {%n",
                        elementType.getBoxedJavaType(headerGenerator),
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
                    else buffer.printf("%s;%n", primitiveWriteExpression(elementType, elementName))

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
        buffer.printf("@Override%n")
        buffer.printf(
            "public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, " +
                    "short _version) {%n",
        )
        buffer.incrementIndent()
        buffer.printf("int _numTaggedFields = 0;%n")
        VersionConditional.forVersions(parentVersions, struct.versions)
            .allowMembershipCheckAlwaysFalse(false)
            .ifNotMember {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
                buffer.printf(
                    "throw new UnsupportedVersionException(\"Can't size version \" + _version + " +
                            "\" of %s\");%n",
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
        buffer.printf("if (_unknownTaggedFields != null) {%n")
        buffer.incrementIndent()
        buffer.printf("_numTaggedFields += _unknownTaggedFields.size();%n")
        buffer.printf("for (RawTaggedField _field : _unknownTaggedFields) {%n")
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));%n")
        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));%n")
        buffer.printf("_size.addBytes(_field.size());%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).ifNotMember {
            generateCheckForUnsupportedNumTaggedFields("_numTaggedFields > 0")
        }.ifMember {
            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
            buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));%n")
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
                    .ifNotMember { buffer.printf("_size.addBytes(_stringBytes.length + 2);%n") }
                    .ifMember {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf(
                            "_size.addBytes(_stringBytes.length + " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n",
                        )
                    }
                    .generate(buffer)
            }

            is BytesFieldType -> {
                buffer.printf("_size.addBytes(%s.length);%n", fieldName)
                VersionConditional.forVersions(flexibleVersions, versions)
                    .ifNotMember { buffer.printf("_size.addBytes(4);%n") }
                    .ifMember {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf(
                            "_size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.length + 1));%n",
                            fieldName,
                        )
                    }
                    .generate(buffer)
            }

            is StructType -> buffer.printf("%s.addSize(_size, _cache, _version);%n", fieldName)
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
            buffer.printf("_numTaggedFields++;%n")
            buffer.printf("_size.addBytes(%d);%n", sizeOfUnsignedVarint(field.tag!!))
            // Account for the tagged field prefix length.
            buffer.printf("_size.addBytes(%d);%n", sizeOfUnsignedVarint(field.type.fixedLength()!!))
            buffer.printf("_size.addBytes(%d);%n", field.type.fixedLength())
            buffer.decrementIndent()
            buffer.printf("}%n")
        } else buffer.printf("_size.addBytes(%d);%n", field.type.fixedLength())
    }

    private fun generateVariableLengthFieldSize(
        field: FieldSpec,
        possibleVersions: Versions,
        tagged: Boolean
    ) {
        IsNullConditional.forField(field)
            .alwaysEmitBlockScope(true)
            .possibleVersions(possibleVersions)
            .nullableVersions(field.nullableVersions)
            .ifNull {
                if (!tagged || field.fieldDefault != "null") {
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            if (tagged) {
                                buffer.printf("_numTaggedFields++;%n")
                                buffer.printf(
                                    "_size.addBytes(%d);%n",
                                    sizeOfUnsignedVarint(field.tag!!),
                                )
                                buffer.printf(
                                    "_size.addBytes(%d);%n",
                                    sizeOfUnsignedVarint(sizeOfUnsignedVarint(0)),
                                )
                            }
                            buffer.printf("_size.addBytes(%d);%n", sizeOfUnsignedVarint(0))
                        }
                        .ifNotMember {
                            if (tagged) throw RuntimeException(
                                "Tagged field ${field.name} should not be present in " +
                                        "non-flexible versions.",
                            )
                            if (field.type.isString) buffer.printf("_size.addBytes(2);%n")
                            else buffer.printf("_size.addBytes(4);%n")
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
                    buffer.printf("_numTaggedFields++;%n")
                    buffer.printf("_size.addBytes(%d);%n", sizeOfUnsignedVarint(field.tag!!))
                }
                if (field.type.isString) {
                    generateStringToBytes(field.camelCaseName())
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            if (tagged) {
                                buffer.printf(
                                    "int _stringPrefixSize = " +
                                            "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);%n"
                                )
                                buffer.printf(
                                    "_size.addBytes(_stringBytes.length + _stringPrefixSize + " +
                                            "ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize + " +
                                            "_stringBytes.length));%n"
                                )
                            } else buffer.printf(
                                "_size.addBytes(_stringBytes.length + " +
                                        "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n"
                            )
                        }
                        .ifNotMember {
                            if (tagged) throw RuntimeException(
                                "Tagged field ${field.name} should not be present in " +
                                        "non-flexible versions.",
                            )
                            buffer.printf("_size.addBytes(_stringBytes.length + 2);%n")
                        }
                        .generate(buffer)
                } else if (field.type.isArray) {
                    if (tagged) buffer.printf("int _sizeBeforeArray = _size.totalSize();%n")
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            buffer.printf(
                                "_size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.size() + 1));%n",
                                field.camelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("_size.addBytes(4);%n") }
                        .generate(buffer)

                    val arrayType = field.type as FieldType.ArrayType
                    val elementType = arrayType.elementType()

                    if (elementType.fixedLength() != null) buffer.printf(
                        "_size.addBytes(%s.size() * %d);%n",
                        field.camelCaseName(),
                        elementType.fixedLength(),
                    ) else if (elementType is FieldType.ArrayType)
                        throw RuntimeException("Arrays of arrays are not supported (use a struct).")
                    else {
                        buffer.printf(
                            "for (%s %sElement : %s) {%n",
                            elementType.getBoxedJavaType(headerGenerator),
                            field.camelCaseName(),
                            field.camelCaseName(),
                        )
                        buffer.incrementIndent()
                        generateVariableLengthArrayElementSize(
                            fieldFlexibleVersions(field),
                            String.format("%sElement", field.camelCaseName()),
                            elementType,
                            possibleVersions,
                        )
                        buffer.decrementIndent()
                        buffer.printf("}%n")
                    }
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf("int _arraySize = _size.totalSize() - _sizeBeforeArray;%n")
                        buffer.printf(
                            "_cache.setArraySizeInBytes(%s, _arraySize);%n",
                            field.camelCaseName(),
                        )
                        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));%n")
                    }
                } else if (field.type.isBytes) {
                    if (tagged) buffer.printf("int _sizeBeforeBytes = _size.totalSize();%n")
                    if (field.zeroCopy) buffer.printf(
                        "_size.addZeroCopyBytes(%s.remaining());%n",
                        field.camelCaseName()
                    )
                    else buffer.printf("_size.addBytes(%s.length);%n", field.camelCaseName())

                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            if (field.zeroCopy) buffer.printf(
                                "_size.addBytes(" +
                                        "ByteUtils.sizeOfUnsignedVarint(%s.remaining() + 1));%n",
                                field.camelCaseName(),
                            )
                            else buffer.printf(
                                "_size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.length + 1));%n",
                                field.camelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("_size.addBytes(4);%n") }
                        .generate(buffer)
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                        buffer.printf("int _bytesSize = _size.totalSize() - _sizeBeforeBytes;%n")
                        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_bytesSize));%n")
                    }
                } else if (field.type.isRecords) {
                    buffer.printf(
                        "_size.addZeroCopyBytes(%s.sizeInBytes());%n",
                        field.camelCaseName()
                    )
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions)
                        .ifMember {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                            buffer.printf(
                                "_size.addBytes(" +
                                        "ByteUtils.sizeOfUnsignedVarint(%s.sizeInBytes() + 1));%n",
                                field.camelCaseName(),
                            )
                        }
                        .ifNotMember { buffer.printf("_size.addBytes(4);%n") }
                        .generate(buffer)
                } else if (field.type.isStruct) {
                    buffer.printf(
                        "int _sizeBeforeStruct = _size.totalSize();%n",
                        field.camelCaseName(),
                    )
                    buffer.printf(
                        "this.%s.addSize(_size, _cache, _version);%n",
                        field.camelCaseName(),
                    )
                    buffer.printf(
                        "int _structSize = _size.totalSize() - _sizeBeforeStruct;%n",
                        field.camelCaseName(),
                    )
                    if (tagged) buffer.printf(
                        "_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));%n",
                    )
                } else throw RuntimeException("unhandled type " + field.type)
                if (tagged && field.fieldDefault != "null") {
                    buffer.decrementIndent()
                    buffer.printf("}%n")
                }
            }
            .generate(buffer)
    }

    private fun generateStringToBytes(name: String) {
        headerGenerator.addImport(MessageGenerator.STANDARD_CHARSETS)
        buffer.printf("byte[] _stringBytes = %s.getBytes(StandardCharsets.UTF_8);%n", name)
        buffer.printf("if (_stringBytes.length > 0x7fff) {%n")
        buffer.incrementIndent()
        buffer.printf(
            "throw new RuntimeException(\"'%s' field is too long to be serialized\");%n",
            name,
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
        buffer.printf("_cache.cacheSerializedValue(%s, _stringBytes);%n", name)
    }

    private fun generateClassEquals(
        className: String, struct: StructSpec,
        elementKeysAreEqual: Boolean,
    ) {
        buffer.printf("@Override%n")
        buffer.printf(
            "public boolean %s(Object obj) {%n",
            if (elementKeysAreEqual) "elementKeysAreEqual" else "equals",
        )
        buffer.incrementIndent()
        buffer.printf("if (!(obj instanceof %s)) return false;%n", className)
        buffer.printf("%s other = (%s) obj;%n", className, className)
        if (struct.fields.isNotEmpty()) {
            for (field in struct.fields)
                if (!elementKeysAreEqual || field.mapKey) generateFieldEquals(field)
        }
        if (elementKeysAreEqual) buffer.printf("return true;%n")
        else {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            buffer.printf(
                "return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, " +
                        "other._unknownTaggedFields);%n",
            )
        }
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldEquals(field: FieldSpec) {
        if (field.type is UUIDFieldType) {
            buffer.printf(
                "if (!this.%s.equals(other.%s)) return false;%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else if (field.type.isString || field.type.isArray || field.type.isStruct) {
            buffer.printf("if (this.%s == null) {%n", field.camelCaseName())
            buffer.incrementIndent()
            buffer.printf("if (other.%s != null) return false;%n", field.camelCaseName())
            buffer.decrementIndent()
            buffer.printf("} else {%n")
            buffer.incrementIndent()
            buffer.printf(
                "if (!this.%s.equals(other.%s)) return false;%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        } else if (field.type.isBytes) {
            if (field.zeroCopy) {
                headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS)
                buffer.printf(
                    "if (!Objects.equals(this.%s, other.%s)) return false;%n",
                    field.camelCaseName(),
                    field.camelCaseName(),
                )
            } else {
                // Arrays#equals handles nulls.
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS)
                buffer.printf(
                    "if (!Arrays.equals(this.%s, other.%s)) return false;%n",
                    field.camelCaseName(),
                    field.camelCaseName(),
                )
            }
        } else if (field.type.isRecords) {
            headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS)
            buffer.printf(
                "if (!Objects.equals(this.%s, other.%s)) return false;%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else buffer.printf(
            "if (%s != other.%s) return false;%n",
            field.camelCaseName(),
            field.camelCaseName(),
        )
    }

    private fun generateClassHashCode(struct: StructSpec, onlyMapKeys: Boolean) {
        buffer.printf("@Override%n")
        buffer.printf("public int hashCode() {%n")
        buffer.incrementIndent()
        buffer.printf("int hashCode = 0;%n")
        for (field: FieldSpec in struct.fields)
            if ((!onlyMapKeys) || field.mapKey) generateFieldHashCode(field)

        buffer.printf("return hashCode;%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldHashCode(field: FieldSpec) {
        if (field.type is BoolFieldType)
            buffer.printf("hashCode = 31 * hashCode + (%s ? 1231 : 1237);%n", field.camelCaseName())
        else if (
            field.type is Int8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Int32FieldType
        ) buffer.printf("hashCode = 31 * hashCode + %s;%n", field.camelCaseName())
        else if (field.type is Int64FieldType || field.type is Uint32FieldType) {
            buffer.printf(
                "hashCode = 31 * hashCode + ((int) (%s >> 32) ^ (int) %s);%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else if (field.type is UUIDFieldType)
            buffer.printf("hashCode = 31 * hashCode + %s.hashCode();%n", field.camelCaseName())
        else if (field.type is Float64FieldType) {
            buffer.printf(
                "hashCode = 31 * hashCode + Double.hashCode(%s);%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else if (field.type.isBytes) {
            if (field.zeroCopy) {
                headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS)
                buffer.printf(
                    "hashCode = 31 * hashCode + Objects.hashCode(%s);%n",
                    field.camelCaseName(),
                )
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS)
                buffer.printf(
                    "hashCode = 31 * hashCode + Arrays.hashCode(%s);%n",
                    field.camelCaseName(),
                )
            }
        } else if (field.type.isRecords) {
            headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS)
            buffer.printf(
                "hashCode = 31 * hashCode + Objects.hashCode(%s);%n",
                field.camelCaseName(),
            )
        } else if (field.type.isStruct || field.type.isArray || field.type.isString)
            buffer.printf(
                "hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                field.camelCaseName(),
                field.camelCaseName(),
            )
        else throw RuntimeException("Unsupported field type " + field.type)
    }

    private fun generateClassDuplicate(className: String, struct: StructSpec) {
        buffer.printf("@Override%n")
        buffer.printf("public %s duplicate() {%n", className)
        buffer.incrementIndent()
        buffer.printf("%s _duplicate = new %s();%n", className, className)

        for (field: FieldSpec in struct.fields) generateFieldDuplicate(
            Target(
                field = field,
                sourceVariable = field.camelCaseName(),
                humanReadableName = field.camelCaseName(),
            ) { input -> String.format("_duplicate.%s = %s", field.camelCaseName(), input) }
        )

        buffer.printf("return _duplicate;%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldDuplicate(target: Target) {
        val field = target.field
        if (
            field.type is BoolFieldType
            || field.type is Int8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Uint32FieldType
            || field.type is Int32FieldType
            || field.type is Int64FieldType
            || field.type is Float64FieldType
            || field.type is UUIDFieldType
        ) buffer.printf("%s;%n", target.assignmentStatement(target.sourceVariable))
        else {
            val cond = IsNullConditional.forName(target.sourceVariable)
                .nullableVersions(target.field.nullableVersions)
                .ifNull { buffer.printf("%s;%n", target.assignmentStatement("null")) }
            if (field.type.isBytes) {
                if (field.zeroCopy) {
                    cond.ifShouldNotBeNull {
                        buffer.printf(
                            "%s;%n",
                            target.assignmentStatement(
                                String.format("%s.duplicate()", target.sourceVariable)
                            ),
                        )
                    }
                } else cond.ifShouldNotBeNull {
                    headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
                    buffer.printf(
                        "%s;%n",
                        target.assignmentStatement(
                            String.format("MessageUtil.duplicate(%s)", target.sourceVariable)
                        ),
                    )
                }
            } else if (field.type.isRecords) {
                cond.ifShouldNotBeNull {
                    headerGenerator.addImport(MessageGenerator.MEMORY_RECORDS_CLASS)
                    buffer.printf(
                        "%s;%n",
                        target.assignmentStatement(
                            "MemoryRecords.readableRecords(((MemoryRecords) " +
                                    "${target.sourceVariable}).buffer().duplicate())"
                        )
                    )
                }
            } else if (field.type.isStruct) cond.ifShouldNotBeNull {
                buffer.printf(
                    "%s;%n",
                    target.assignmentStatement("${target.sourceVariable}.duplicate()")
                )
            } else if (field.type.isString) {
                // Strings are immutable, so we don't need to duplicate them.
                cond.ifShouldNotBeNull {
                    buffer.printf("%s;%n", target.assignmentStatement(target.sourceVariable))
                }
            } else if (field.type.isArray) {
                cond.ifShouldNotBeNull {
                    val newArrayName = "new${field.capitalizedCamelCaseName()}"
                    val type: String = field.concreteJavaType(headerGenerator, structRegistry)
                    buffer.printf(
                        "%s %s = new %s(%s.size());%n",
                        type,
                        newArrayName,
                        type,
                        target.sourceVariable,
                    )
                    val arrayType = field.type as FieldType.ArrayType
                    buffer.printf(
                        "for (%s _element : %s) {%n",
                        arrayType.elementType().getBoxedJavaType(headerGenerator),
                        target.sourceVariable,
                    )
                    buffer.incrementIndent()
                    generateFieldDuplicate(
                        target.arrayElementTarget { input ->
                            String.format("%s.add(%s)", newArrayName, input)
                        }
                    )
                    buffer.decrementIndent()
                    buffer.printf("}%n")
                    buffer.printf(
                        "%s;%n",
                        target.assignmentStatement("new${field.capitalizedCamelCaseName()}"),
                    )
                }
            } else throw RuntimeException("Unhandled field type " + field.type)
            cond.generate(buffer)
        }
    }

    private fun generateClassToString(className: String, struct: StructSpec) {
        buffer.printf("@Override%n")
        buffer.printf("public String toString() {%n")
        buffer.incrementIndent()
        buffer.printf("return \"%s(\"%n", className)
        buffer.incrementIndent()
        var prefix = ""
        for (field in struct.fields) {
            generateFieldToString(prefix, field)
            prefix = ", "
        }
        buffer.printf("+ \")\";%n")
        buffer.decrementIndent()
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldToString(prefix: String, field: FieldSpec) {
        if (field.type is BoolFieldType) {
            buffer.printf(
                "+ \"%s%s=\" + (%s ? \"true\" : \"false\")%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else if (
            field.type is Int8FieldType
            || field.type is Int16FieldType
            || field.type is Uint16FieldType
            || field.type is Uint32FieldType
            || field.type is Int32FieldType
            || field.type is Int64FieldType
            || field.type is Float64FieldType
        ) {
            buffer.printf(
                "+ \"%s%s=\" + %s%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName()
            )
        } else if (field.type.isString) buffer.printf(
            "+ \"%s%s=\" + ((%s == null) ? \"null\" : \"'\" + %s.toString() + \"'\")%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
            field.camelCaseName(),
        ) else if (field.type.isBytes) {
            if (field.zeroCopy) buffer.printf(
                "+ \"%s%s=\" + %s%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
            else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS)
                buffer.printf(
                    "+ \"%s%s=\" + Arrays.toString(%s)%n",
                    prefix,
                    field.camelCaseName(),
                    field.camelCaseName(),
                )
            }
        } else if (field.type.isRecords) buffer.printf(
            "+ \"%s%s=\" + %s%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
        )
        else if (field.type is UUIDFieldType || field.type.isStruct) buffer.printf(
            "+ \"%s%s=\" + %s.toString()%n",
            prefix,
            field.camelCaseName(),
            field.camelCaseName(),
        )
        else if (field.type.isArray) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS)
            if (field.nullableVersions.empty()) buffer.printf(
                "+ \"%s%s=\" + MessageUtil.deepToString(%s.iterator())%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
            )
            else buffer.printf(
                "+ \"%s%s=\" + ((%s == null) ? \"null\" : " +
                        "MessageUtil.deepToString(%s.iterator()))%n",
                prefix,
                field.camelCaseName(),
                field.camelCaseName(),
                field.camelCaseName(),
            )
        } else throw RuntimeException("Unsupported field type " + field.type)
    }

    private fun generateFieldAccessor(field: FieldSpec) {
        buffer.printf("%n")
        generateAccessor(
            javaType = field.fieldAbstractJavaType(headerGenerator, structRegistry),
            functionName = field.camelCaseName(),
            memberName = field.camelCaseName(),
        )
    }

    private fun generateAccessor(javaType: String, functionName: String, memberName: String) {
        buffer.printf("public %s %s() {%n", javaType, functionName)
        buffer.incrementIndent()
        buffer.printf("return this.%s;%n", memberName)
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateFieldMutator(className: String, field: FieldSpec) {
        buffer.printf("%n")
        buffer.printf(
            "public %s set%s(%s v) {%n",
            className,
            field.capitalizedCamelCaseName(),
            field.fieldAbstractJavaType(headerGenerator, structRegistry),
        )
        buffer.incrementIndent()
        if (field.type is Uint16FieldType) {
            buffer.printf("if (v < 0 || v > %d) {%n", MessageGenerator.UNSIGNED_SHORT_MAX)
            buffer.incrementIndent()
            buffer.printf(
                "throw new RuntimeException(\"Invalid value \" + v + " +
                        "\" for unsigned short field.\");%n"
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
        if (field.type is Uint32FieldType) {
            buffer.printf("if (v < 0 || v > %dL) {%n", MessageGenerator.UNSIGNED_INT_MAX)
            buffer.incrementIndent()
            buffer.printf(
                "throw new RuntimeException(\"Invalid value \" + v + " +
                        "\" for unsigned int field.\");%n"
            )
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
        buffer.printf("this.%s = v;%n", field.camelCaseName())
        buffer.printf("return this;%n")
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    private fun generateSetter(javaType: String, functionName: String, memberName: String) {
        buffer.printf("public void %s(%s v) {%n", functionName, javaType)
        buffer.incrementIndent()
        buffer.printf("this.%s = v;%n", memberName)
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
