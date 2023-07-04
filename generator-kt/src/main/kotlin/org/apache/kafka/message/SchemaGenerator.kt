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
import org.apache.kafka.message.FieldType.UUIDFieldType
import org.apache.kafka.message.FieldType.Uint16FieldType
import org.apache.kafka.message.FieldType.Uint32FieldType
import java.util.*

/**
 * Generates Schemas for Kafka MessageData classes.
 *
 * @property headerGenerator The header file generator. This is shared with the MessageDataGenerator
 * instance that owns this SchemaGenerator.
 * @property structRegistry A registry with the structures we're generating.
 */
internal class SchemaGenerator(
    private val headerGenerator: HeaderGenerator,
    private val structRegistry: StructRegistry
) {

    /**
     * Schema information for a particular message.
     *
     * @property versions The versions of this message that we want to generate a schema for. This
     * will be constrained by the valid versions for the parent objects. For example, if the parent
     * message is valid for versions 0 and 1, we will only generate a version 0 and version 1 schema
     * for child classes, even if their valid versions are "0+".
     */
    internal class MessageInfo(val versions: Versions) {

        /**
         * Maps versions to schema declaration code. If the schema for a
         * particular version is the same as that of a previous version,
         * there will be no entry in the map for it.
         */
        val schemaForVersion = TreeMap<Short, CodeBuffer>()
    }

    /**
     * Maps message names to message information.
     */
    private val messages = HashMap<String, MessageInfo>()

    /**
     * The versions that implement a KIP-482 flexible schema.
     */
    private var messageFlexibleVersions: Versions? = null

    @Throws(Exception::class)
    fun generateSchemas(message: MessageSpec) {
        messageFlexibleVersions = message.flexibleVersions

        // First generate schemas for common structures so that they are
        // available when we generate the inline structures
        val iter = structRegistry.commonStructs()
        while (iter.hasNext()) {
            val struct = iter.next()
            generateSchemas(
                className = struct.name,
                struct = struct,
                parentVersions = message.struct.versions,
            )
        }

        // Generate schemas for inline structures
        generateSchemas(
            className = message.dataClassName(),
            struct = message.struct,
            parentVersions = message.struct.versions
        )
    }

    @Throws(Exception::class)
    fun generateSchemas(
        className: String, struct: StructSpec,
        parentVersions: Versions,
    ) {
        val versions = parentVersions.intersect(struct.versions)
        var messageInfo = messages[className]
        if (messageInfo != null) return

        messageInfo = MessageInfo(versions)
        messages[className] = messageInfo
        // Process the leaf classes first.
        for (field in struct.fields) {
            if (field.type.isStructArray) {
                val arrayType = field.type as FieldType.ArrayType
                generateSchemas(
                    className = arrayType.elementType.toString(),
                    struct = structRegistry.findStruct(field),
                    parentVersions = versions
                )
            } else if (field.type.isStruct) generateSchemas(
                className = field.type.toString(),
                struct = structRegistry.findStruct(field),
                parentVersions = versions
            )
        }
        var prev: CodeBuffer? = null
        for (v in versions.lowest..versions.highest) {
            val cur = CodeBuffer()
            generateSchemaForVersion(struct, v.toShort(), cur)
            // If this schema version is different from the previous one,
            // create a new map entry.
            if (cur != prev) messageInfo.schemaForVersion[v.toShort()] = cur
            prev = cur
        }
    }

    @Throws(Exception::class)
    private fun generateSchemaForVersion(
        struct: StructSpec,
        version: Short,
        buffer: CodeBuffer
    ) {
        // Find the last valid field index.
        var lastValidIndex = struct.fields.size - 1
        while (true) {
            if (lastValidIndex < 0) break
            val field = struct.fields[lastValidIndex]
            if (!field.taggedVersions.contains(version) && field.versions.contains(version)) break
            lastValidIndex--
        }
        var finalLine = lastValidIndex
        if (messageFlexibleVersions!!.contains(version)) finalLine++

        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS)
        buffer.printf("Schema(%n")
        buffer.incrementIndent()
        for (i in 0..lastValidIndex) {
            val field = struct.fields[i]
            if (!field.versions.contains(version) || field.taggedVersions.contains(version))
                continue

            val fieldFlexibleVersions = (field.flexibleVersions ?: messageFlexibleVersions)!!
            headerGenerator.addImport(MessageGenerator.FIELD_CLASS)
            buffer.printf(
                "Field(\"%s\", %s, \"%s\")%s%n",
                field.snakeCaseName(),
                fieldTypeToSchemaType(field, version, fieldFlexibleVersions),
                field.about,
                if (i == finalLine) "" else ","
            )
        }
        if (messageFlexibleVersions!!.contains(version))
            generateTaggedFieldsSchemaForVersion(struct, version, buffer)

        buffer.decrementIndent()
        buffer.printf(")%n")
    }

    @Throws(Exception::class)
    private fun generateTaggedFieldsSchemaForVersion(
        struct: StructSpec,
        version: Short,
        buffer: CodeBuffer,
    ) {
        headerGenerator.addImport(MessageGenerator.TAGGED_FIELDS_SECTION_CLASS)

        // Find the last valid tagged field index.
        var lastValidIndex = struct.fields.size - 1
        while (true) {
            if (lastValidIndex < 0) break
            val field = struct.fields[lastValidIndex]
            if (field.taggedVersions.contains(version) && field.versions.contains(version)) break
            lastValidIndex--
        }
        buffer.printf("TaggedFieldsSection.of(%n")
        buffer.incrementIndent()
        for (i in 0..lastValidIndex) {
            val field = struct.fields[i]
            if (!field.versions.contains(version) || !field.taggedVersions.contains(version))
                continue

            headerGenerator.addImport(MessageGenerator.FIELD_CLASS)
            val fieldFlexibleVersions = (field.flexibleVersions ?: messageFlexibleVersions)!!
            buffer.printf(
                "%d, Field(\"%s\", %s, \"%s\")%s%n",
                field.tag,
                field.snakeCaseName(),
                fieldTypeToSchemaType(field, version, fieldFlexibleVersions),
                field.about,
                if (i == lastValidIndex) "" else ","
            )
        }
        buffer.decrementIndent()
        buffer.printf(")%n")
    }

    private fun fieldTypeToSchemaType(
        field: FieldSpec,
        version: Short,
        fieldFlexibleVersions: Versions,
    ): String = fieldTypeToSchemaType(
        type = field.type,
        nullable = field.nullableVersions.contains(version),
        version = version,
        fieldFlexibleVersions = fieldFlexibleVersions,
        zeroCopy = field.zeroCopy,
    )

    private fun fieldTypeToSchemaType(
        type: FieldType,
        nullable: Boolean,
        version: Short,
        fieldFlexibleVersions: Versions,
        zeroCopy: Boolean,
    ): String {
        return if (type is BoolFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.BOOLEAN"
        } else if (type is Int8FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.INT8"
        } else if (type is Int16FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.INT16"
        } else if (type is Uint16FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.UINT16"
        } else if (type is Uint32FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.UNSIGNED_INT32"
        } else if (type is Int32FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.INT32"
        } else if (type is Int64FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.INT64"
        } else if (type is UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.UUID"
        } else if (type is Float64FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            "Type.FLOAT64"
        } else if (type is StringFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (fieldFlexibleVersions.contains(version)) {
                if (nullable) "Type.COMPACT_NULLABLE_STRING"
                else "Type.COMPACT_STRING"
            } else if (nullable) "Type.NULLABLE_STRING"
            else "Type.STRING"
        } else if (type is BytesFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (fieldFlexibleVersions.contains(version)) {
                if (nullable) "Type.COMPACT_NULLABLE_BYTES"
                else "Type.COMPACT_BYTES"
            } else if (nullable) "Type.NULLABLE_BYTES"
            else "Type.BYTES"
        } else if (type.isRecords) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS)
            if (fieldFlexibleVersions.contains(version)) "Type.COMPACT_RECORDS"
            else "Type.RECORDS"
        } else if (type.isArray) {
            if (fieldFlexibleVersions.contains(version)) {
                headerGenerator.addImport(MessageGenerator.COMPACT_ARRAYOF_CLASS)
                val arrayType = type as FieldType.ArrayType
                val prefix = if (nullable) "CompactArrayOf.nullable" else "CompactArrayOf"
                String.format(
                    "%s(%s)", prefix,
                    fieldTypeToSchemaType(
                        arrayType.elementType,
                        false,
                        version,
                        fieldFlexibleVersions,
                        false
                    )
                )
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYOF_CLASS)
                val arrayType = type as FieldType.ArrayType
                val prefix = if (nullable) "ArrayOf.nullable" else "ArrayOf"
                String.format(
                    "%s(%s)", prefix,
                    fieldTypeToSchemaType(
                        type = arrayType.elementType,
                        nullable = false,
                        version = version,
                        fieldFlexibleVersions = fieldFlexibleVersions,
                        zeroCopy = false,
                    )
                )
            }
        } else if (type.isStruct) {
            if (nullable) throw RuntimeException("Type $type cannot be nullable.")
            String.format("%s.SCHEMA_%d", type.toString(), floorVersion(type.toString(), version))
        } else throw RuntimeException("Unsupported type $type")
    }

    /**
     * Find the lowest schema version for a given class that is the same as the
     * given version.
     */
    private fun floorVersion(className: String, v: Short): Short =
        messages[className]!!.schemaForVersion.floorKey(v)

    /**
     * Write the message schema to the provided buffer.
     *
     * @param className The class name.
     * @param buffer The destination buffer.
     */
    @Throws(Exception::class)
    fun writeSchema(className: String, buffer: CodeBuffer) {
        val messageInfo = messages[className]!!
        val lowest = messageInfo.versions.lowest
        val highest = messageInfo.versions.highest

        buffer.printf("companion object {%n")
        buffer.incrementIndent()
        for (v in lowest..highest) {
            val declaration = messageInfo.schemaForVersion[v.toShort()]
            if (declaration == null)
                buffer.printf("val SCHEMA_%d: Schema = SCHEMA_%d%n", v, v - 1)
            else {
                buffer.printf("val SCHEMA_%d:Schema =%n", v)
                buffer.incrementIndent()
                declaration.write(buffer)
                buffer.decrementIndent()
            }
            buffer.printf("%n")
        }
        buffer.printf("val SCHEMAS: Array<Schema> = arrayOf(%n")
        buffer.incrementIndent()
        for (v in 0 until lowest)
            buffer.printf("null%s%n", if (v.toShort() == highest) "" else ",")

        for (v in lowest..highest)
            buffer.printf("SCHEMA_%d%s%n", v, if (v.toShort() == highest) "" else ",")

        buffer.decrementIndent()
        buffer.printf(")%n")
        buffer.printf("%n")
        buffer.printf("val LOWEST_SUPPORTED_VERSION: Short = %d%n", lowest)
        buffer.printf("val HIGHEST_SUPPORTED_VERSION: Short = %d%n", highest)
        buffer.printf("%n")

        buffer.decrementIndent()
        buffer.printf("}%n")
    }
}
