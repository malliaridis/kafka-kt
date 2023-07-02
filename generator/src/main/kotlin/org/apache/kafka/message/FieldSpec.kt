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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.message.FieldType.BoolFieldType
import org.apache.kafka.message.FieldType.Float64FieldType
import org.apache.kafka.message.FieldType.Int16FieldType
import org.apache.kafka.message.FieldType.Int32FieldType
import org.apache.kafka.message.FieldType.Int64FieldType
import org.apache.kafka.message.FieldType.Int8FieldType
import org.apache.kafka.message.FieldType.RecordsFieldType
import org.apache.kafka.message.FieldType.StringFieldType
import org.apache.kafka.message.FieldType.UUIDFieldType
import org.apache.kafka.message.FieldType.Uint16FieldType
import org.apache.kafka.message.FieldType.Uint32FieldType
import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import org.apache.kafka.message.MessageGenerator.lowerCaseFirst
import org.apache.kafka.message.MessageGenerator.toSnakeCase
import java.nio.ByteBuffer
import java.util.*
import java.util.regex.Pattern

class FieldSpec @JsonCreator constructor(
    @JsonProperty("name") val name: String,
    @JsonProperty("versions") versions: String?,
    @JsonProperty("fields") fields: List<FieldSpec>?,
    @JsonProperty("type") type: String,
    @JsonProperty("mapKey") val mapKey: Boolean,
    @JsonProperty("nullableVersions") nullableVersions: String?,
    @JsonProperty("default") fieldDefault: String?,
    @JsonProperty("ignorable") val ignorable: Boolean,
    @JsonProperty("entityType") entityType: EntityType?,
    @JsonProperty("about") about: String?,
    @JsonProperty("taggedVersions") taggedVersions: String?,
    @JsonProperty("flexibleVersions") flexibleVersions: String?,
    @JsonProperty("tag") val tag: Int?,
    @JsonProperty("zeroCopy") val zeroCopy: Boolean,
) {

    val versions: Versions

    @JsonProperty("fields")
    val fields: List<FieldSpec>

    val type: FieldType

    val nullableVersions: Versions

    @JsonProperty("default")
    val fieldDefault: String

    @JsonProperty("entityType")
    val entityType: EntityType

    @JsonProperty("about")
    val about: String

    val taggedVersions: Versions

    val flexibleVersions: Versions?

    init {
        if (!VALID_FIELD_NAMES.matcher(name).matches())
            throw RuntimeException("Invalid field name $name")

        this.taggedVersions = Versions.parse(taggedVersions, Versions.NONE)!!
        // If versions is not set, but taggedVersions is, default to taggedVersions.
        this.versions = Versions.parse(
            input = versions,
            defaultVersions = if (this.taggedVersions.empty()) null else this.taggedVersions,
        ) ?: throw RuntimeException("You must specify the version of the $name structure.")

        this.fields = fields?.toList() ?: emptyList()
        this.type = FieldType.parse(type)

        this.nullableVersions = Versions.parse(nullableVersions, Versions.NONE)!!
        if (!this.nullableVersions.empty() && !this.type.canBeNullable())
            throw RuntimeException("Type ${this.type} cannot be nullable.")

        this.fieldDefault = fieldDefault ?: ""
        this.entityType = entityType ?: EntityType.UNKNOWN
        this.entityType.verifyTypeMatches(name, this.type)
        this.about = about ?: ""
        if (this.fields.isNotEmpty()) {
            if (!this.type.isArray && !this.type.isStruct) {
                throw RuntimeException("Non-array or Struct field $name cannot have fields")
            }
        }
        if (flexibleVersions.isNullOrEmpty()) this.flexibleVersions = null
        else {
            this.flexibleVersions = Versions.parse(flexibleVersions, null)
            if (!(this.type.isString || this.type.isBytes)) {
                // For now, only allow flexibleVersions overrides for the string and bytes
                // types. Overrides are only needed to keep compatibility with some old formats,
                // so there isn't any need to support them for all types.
                throw RuntimeException(
                    "Invalid flexibleVersions override for $name. Only fields of type string or" +
                            " bytes can specify a flexibleVersions override."
                )
            }
        }
        if (tag != null && mapKey)
            throw RuntimeException("Tagged fields cannot be used as keys.")

        checkTagInvariants()
        if (this.zeroCopy && !this.type.isBytes) throw RuntimeException(
            "Invalid zeroCopy value for $name. Only fields of type bytes can use zeroCopy flag."
        )
    }

    private fun checkTagInvariants() {
        if (tag != null) {
            if (tag < 0) throw RuntimeException(
                "Field $name specifies a tag of $tag. Tags cannot be negative."
            )
            if (taggedVersions.empty()) throw RuntimeException(
                "Field $name specifies a tag of $tag, but has no tagged versions. If a tag is " +
                        "specified, taggedVersions must be specified as well."
            )
            val nullableTaggedVersions = nullableVersions.intersect(taggedVersions)
            if (!(nullableTaggedVersions.empty() || (nullableTaggedVersions == taggedVersions)))
                throw RuntimeException(
                    "Field $name specifies nullableVersions $nullableVersions and taggedVersions " +
                            "$taggedVersions. Either all tagged versions must be nullable, or " +
                            "none must be."
                )
            if (taggedVersions.highest < Short.MAX_VALUE) throw RuntimeException(
                "Field $name specifies taggedVersions $taggedVersions, which is not open-ended. " +
                        "taggedVersions must be either none, or an open-ended range (that ends " +
                        "with a plus sign)."
            )
            if (taggedVersions.intersect((versions)) != taggedVersions) throw RuntimeException(
                "Field $name specifies taggedVersions $taggedVersions, and versions $versions. " +
                        "taggedVersions must be a subset of versions."
            )
        } else if (!taggedVersions.empty()) throw RuntimeException(
            "Field $name does not specify a tag, but specifies tagged versions of " +
                    "$taggedVersions. Please specify a tag, or remove the taggedVersions."
        )
    }

    @JsonProperty("name")
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String = name

    fun capitalizedCamelCaseName(): String = capitalizeFirst(name)

    fun camelCaseName(): String = lowerCaseFirst(name)

    fun snakeCaseName(): String = toSnakeCase(name)

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("versions"),
    )
    fun versions(): Versions = versions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("versionsString"),
    )
    @JsonProperty("versions")
    fun versionsString(): String = versions.toString()

    val versionsString: String
        get() = versions.toString()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("fields"),
    )
    @JsonProperty("fields")
    fun fields(): List<FieldSpec> = fields

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("typeString"),
    )
    @JsonProperty("type")
    fun typeString(): String = type.toString()

    @get:JsonProperty("type")
    val typeString: String
        get() = type.toString()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("type"),
    )
    fun type(): FieldType = type

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("mapKey"),
    )
    @JsonProperty("mapKey")
    fun mapKey(): Boolean = mapKey

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("nullableVersions"),
    )
    fun nullableVersions(): Versions = nullableVersions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("nullableVersionsString"),
    )
    @JsonProperty("nullableVersions")
    fun nullableVersionsString(): String = nullableVersions.toString()

    @get:JsonProperty("nullableVersions")
    val nullableVersionsString: String
        get() = nullableVersions.toString()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("fieldDefault"),
    )
    @JsonProperty("default")
    fun defaultString(): String = fieldDefault

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("ignorable"),
    )
    @JsonProperty("ignorable")
    fun ignorable(): Boolean = ignorable

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("entityType"),
    )
    @JsonProperty("entityType")
    fun entityType(): EntityType = entityType

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("about"),
    )
    @JsonProperty("about")
    fun about(): String = about

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("taggedVersionsString"),
    )
    @JsonProperty("taggedVersions")
    fun taggedVersionsString(): String = taggedVersions.toString()

    @get:JsonProperty("taggedVersions")
    val taggedVersionsString: String
        get() = taggedVersions.toString()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("taggedVersions"),
    )
    fun taggedVersions(): Versions = taggedVersions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("flexibleVersionsString"),
    )
    @JsonProperty("flexibleVersions")
    fun flexibleVersionsString(): String? = flexibleVersions?.toString()

    @get:JsonProperty("flexibleVersions")
    val flexibleVersionsString: String?
        get() = flexibleVersions?.toString()

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("flexibleVersions"),
    )
    fun flexibleVersions(): Versions? = flexibleVersions

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("tagInteger"),
    )
    @JsonProperty("tag")
    fun tagInteger(): Int? = tag

    @get:JsonProperty("tag")
    val tagInteger: Int?
        get() = tag

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("tag"),
    )
    fun tag(): Int? = tag

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("zeroCopy"),
    )
    @JsonProperty("zeroCopy")
    fun zeroCopy(): Boolean = zeroCopy

    /**
     * Get a string representation of the field default.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     *
     * @return A string that can be used for the field default in the generated code.
     */
    fun fieldDefault(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        if (type is BoolFieldType) {
            return if (fieldDefault.isEmpty()) "false"
            else if (fieldDefault.equals("true", ignoreCase = true)) "true"
            else if (fieldDefault.equals("false", ignoreCase = true)) "false"
            else throw RuntimeException("Invalid default for boolean field $name: $fieldDefault")
        } else if (
            type is Int8FieldType
            || type is Int16FieldType
            || type is Uint16FieldType
            || type is Uint32FieldType
            || type is Int32FieldType
            || type is Int64FieldType
        ) {
            var base = 10
            var defaultString = fieldDefault
            if (defaultString.startsWith("0x")) {
                base = 16
                defaultString = defaultString.substring(2)
            }
            when (type) {
                is Int8FieldType -> {
                    return if (defaultString.isEmpty()) "(byte) 0"
                    else {
                        try {
                            java.lang.Byte.valueOf(defaultString, base)
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for int8 field $name: $defaultString",
                                e,
                            )
                        }
                        "(byte) $fieldDefault"
                    }
                }

                is Int16FieldType -> {
                    return if (defaultString.isEmpty()) "(short) 0"
                    else {
                        try {
                            defaultString.toShort(base)
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for int16 field $name: $defaultString",
                                e,
                            )
                        }
                        "(short) $fieldDefault"
                    }
                }

                is Uint16FieldType -> {
                    if (defaultString.isEmpty()) return "0"
                    else {
                        try {
                            val value = Integer.valueOf(defaultString, base)
                            if (value < 0 || value > MessageGenerator.UNSIGNED_SHORT_MAX)
                                throw RuntimeException(
                                    "Invalid default for uint16 field $name: out of range."
                                )
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for uint16 field $name: $defaultString",
                                e,
                            )
                        }
                        return fieldDefault
                    }
                }

                is Uint32FieldType -> {
                    if (defaultString.isEmpty()) return "0"
                    else {
                        try {
                            val value = java.lang.Long.valueOf(defaultString, base)
                            if (value < 0 || value > MessageGenerator.UNSIGNED_INT_MAX) {
                                throw RuntimeException(
                                    ("Invalid default for uint32 field " +
                                            name + ": out of range.")
                                )
                            }
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for uint32 field $name: $defaultString",
                                e,
                            )
                        }
                        return fieldDefault
                    }
                }

                is Int32FieldType -> {
                    return if (defaultString.isEmpty()) "0"
                    else {
                        try {
                            Integer.valueOf(defaultString, base)
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for int32 field $name: $defaultString",
                                e,
                            )
                        }
                        fieldDefault
                    }
                }

                is Int64FieldType -> {
                    return if (defaultString.isEmpty()) "0L"
                    else {
                        try {
                            java.lang.Long.valueOf(defaultString, base)
                        } catch (e: NumberFormatException) {
                            throw RuntimeException(
                                "Invalid default for int64 field $name: $defaultString",
                                e,
                            )
                        }
                        fieldDefault + "L"
                    }
                }

                else -> throw RuntimeException("Unsupported field type $type")
            }
        } else if (type is UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS)
            return if (fieldDefault.isEmpty()) "Uuid.ZERO_UUID"
            else {
                try {
                    val uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(fieldDefault))
                    uuidBytes.getLong()
                    uuidBytes.getLong()
                } catch (e: IllegalArgumentException) {
                    throw RuntimeException(
                        "Invalid default for uuid field $name: $fieldDefault",
                        e,
                    )
                }
                headerGenerator.addImport(MessageGenerator.UUID_CLASS)
                "Uuid.fromString(\"$fieldDefault\")"
            }
        } else if (type is Float64FieldType) {
            return if (fieldDefault.isEmpty()) "0.0"
            else {
                try {
                    fieldDefault.toDouble()
                } catch (e: NumberFormatException) {
                    throw RuntimeException(
                        ("Invalid default for float64 field " +
                                name + ": " + fieldDefault), e
                    )
                }
                "Double.parseDouble(\"$fieldDefault\")"
            }
        } else if (type is StringFieldType) {
            return if ((fieldDefault == "null")) {
                validateNullDefault()
                "null"
            } else "\"$fieldDefault\""
        } else if (type.isBytes) {
            if ((fieldDefault == "null")) {
                validateNullDefault()
                return "null"
            } else if (!fieldDefault.isEmpty()) throw RuntimeException(
                "Invalid default for bytes field $name. The only valid default for a bytes field " +
                        "is empty or null."
            )
            return if (zeroCopy) {
                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                "ByteUtils.EMPTY_BUF"
            } else {
                headerGenerator.addImport(MessageGenerator.BYTES_CLASS)
                "Bytes.EMPTY"
            }
        } else if (type.isRecords) return "null"
        else if (type.isStruct) {
            if (fieldDefault.isNotEmpty()) throw RuntimeException(
                "Invalid default for struct field $name: custom defaults are not supported for " +
                        "struct fields."
            )
            return "new $type()"
        } else if (type.isArray) {
            if (fieldDefault == "null") {
                validateNullDefault()
                return "null"
            } else if (fieldDefault.isNotEmpty()) throw RuntimeException(
                "Invalid default for array field $name. The only valid default for an array " +
                        "field is the empty array or null."
            )
            return String.format(
                "new %s(0)",
                concreteJavaType(headerGenerator, structRegistry),
            )
        } else throw RuntimeException("Unsupported field type $type")
    }

    private fun validateNullDefault() {
        if (!nullableVersions.contains(versions)) throw RuntimeException(
            "null cannot be the default for field $name, because not all versions of this field " +
                    "are nullable."
        )
    }

    /**
     * Get the abstract Java type of the field-- for example, List.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @return The abstract java type name.
     */
    fun fieldAbstractJavaType(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        when {
            type is BoolFieldType -> return "boolean"
            type is Int8FieldType -> return "byte"
            type is Int16FieldType -> return "short"
            type is Uint16FieldType -> return "int"
            type is Uint32FieldType -> return "long"
            type is Int32FieldType -> return "int"
            type is Int64FieldType -> return "long"
            type is UUIDFieldType -> {
                headerGenerator.addImport(MessageGenerator.UUID_CLASS)
                return "Uuid"
            }

            type is Float64FieldType -> return "double"
            type.isString -> return "String"
            type.isBytes -> {
                return if (zeroCopy) {
                    headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS)
                    "ByteBuffer"
                } else "byte[]"
            }

            type is RecordsFieldType -> {
                headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS)
                return "BaseRecords"
            }

            type.isStruct -> return capitalizeFirst(typeString)
            type.isArray -> {
                val arrayType = type as FieldType.ArrayType
                return if (structRegistry.isStructArrayWithKeys(this)) {
                    headerGenerator.addImport(
                        MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS
                    )
                    collectionType(arrayType.elementType().toString())
                } else {
                    headerGenerator.addImport(MessageGenerator.LIST_CLASS)
                    String.format(
                        "List<%s>",
                        arrayType.elementType().getBoxedJavaType(headerGenerator),
                    )
                }
            }

            else -> throw RuntimeException("Unknown field type $type")
        }
    }

    /**
     * Get the concrete Java type of the field-- for example, ArrayList.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @return The abstract java type name.
     */
    fun concreteJavaType(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        return if (type.isArray) {
            val arrayType = type as FieldType.ArrayType
            if (structRegistry.isStructArrayWithKeys(this))
                collectionType(arrayType.elementType().toString())
            else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
                String.format(
                    "ArrayList<%s>",
                    arrayType.elementType().getBoxedJavaType(headerGenerator)
                )
            }
        } else fieldAbstractJavaType(headerGenerator, structRegistry)
    }

    /**
     * Generate an if statement that checks if this field has a non-default value.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @param buffer The code buffer to write to.
     * @param fieldPrefix The prefix to prepend before references to this field.
     * @param nullableVersions The nullable versions to use for this field. This is mainly to let us
     * choose to ignore the possibility of nulls sometimes (like when dealing with array entries
     * that cannot be null).
     */
    fun generateNonDefaultValueCheck(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
        buffer: CodeBuffer,
        fieldPrefix: String?,
        nullableVersions: Versions,
    ) {
        val fieldDefault = fieldDefault(headerGenerator, structRegistry)
        if (type.isArray) {
            if (fieldDefault == "null")
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName())
            else if (nullableVersions.empty())
                buffer.printf("if (!%s%s.isEmpty()) {%n", fieldPrefix, camelCaseName())
            else buffer.printf(
                "if (%s%s == null || !%s%s.isEmpty()) {%n",
                fieldPrefix,
                camelCaseName(),
                fieldPrefix,
                camelCaseName(),
            )
        } else if (type.isBytes) {
            if (fieldDefault == "null")
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName())
            else if (nullableVersions.empty()) {
                if (zeroCopy) buffer.printf(
                    "if (%s%s.hasRemaining()) {%n",
                    fieldPrefix,
                    camelCaseName(),
                )
                else buffer.printf("if (%s%s.length != 0) {%n", fieldPrefix, camelCaseName())
            } else {
                if (zeroCopy) buffer.printf(
                    "if (%s%s == null || %s%s.remaining() > 0) {%n",
                    fieldPrefix,
                    camelCaseName(),
                    fieldPrefix,
                    camelCaseName(),
                )
                else buffer.printf(
                    "if (%s%s == null || %s%s.length != 0) {%n",
                    fieldPrefix,
                    camelCaseName(),
                    fieldPrefix,
                    camelCaseName(),
                )
            }
        } else if (type.isString || type.isStruct || type is UUIDFieldType) {
            if ((fieldDefault == "null"))
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName())
            else if (nullableVersions.empty()) buffer.printf(
                "if (!%s%s.equals(%s)) {%n",
                fieldPrefix,
                camelCaseName(),
                fieldDefault,
            )
            else buffer.printf(
                "if (%s%s == null || !%s%s.equals(%s)) {%n",
                fieldPrefix,
                camelCaseName(),
                fieldPrefix,
                camelCaseName(),
                fieldDefault,
            )
        } else if (type is BoolFieldType) buffer.printf(
            "if (%s%s%s) {%n",
            if (fieldDefault == "true") "!" else "", fieldPrefix, camelCaseName()
        )
        else buffer.printf(
            "if (%s%s != %s) {%n",
            fieldPrefix,
            camelCaseName(),
            fieldDefault,
        )
    }

    /**
     * Generate an if statement that checks if this field is non-default and also non-ignorable.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @param fieldPrefix The prefix to prepend before references to this field.
     * @param buffer The code buffer to write to.
     */
    fun generateNonIgnorableFieldCheck(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
        fieldPrefix: String?,
        buffer: CodeBuffer,
    ) {
        generateNonDefaultValueCheck(
            headerGenerator = headerGenerator,
            structRegistry = structRegistry,
            buffer = buffer,
            fieldPrefix = fieldPrefix,
            nullableVersions = nullableVersions,
        )
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw new UnsupportedVersionException(\"Attempted to write a non-default %s at " +
                    "version \" + _version);%n",
            camelCaseName()
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    companion object {

        private val VALID_FIELD_NAMES = Pattern.compile("[A-Za-z]([A-Za-z0-9]*)")

        fun collectionType(baseType: String): String = baseType + "Collection"
    }
}
