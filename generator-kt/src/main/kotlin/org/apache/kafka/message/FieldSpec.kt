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
import java.nio.ByteBuffer
import java.util.Base64
import java.util.regex.Pattern
import org.apache.kafka.message.FieldType.ArrayType
import org.apache.kafka.message.FieldType.BoolFieldType
import org.apache.kafka.message.FieldType.BytesFieldType
import org.apache.kafka.message.FieldType.Float32FieldType
import org.apache.kafka.message.FieldType.Float64FieldType
import org.apache.kafka.message.FieldType.Int16FieldType
import org.apache.kafka.message.FieldType.Int32FieldType
import org.apache.kafka.message.FieldType.Int64FieldType
import org.apache.kafka.message.FieldType.Int8FieldType
import org.apache.kafka.message.FieldType.RecordsFieldType
import org.apache.kafka.message.FieldType.StringFieldType
import org.apache.kafka.message.FieldType.StructType
import org.apache.kafka.message.FieldType.UUIDFieldType
import org.apache.kafka.message.FieldType.Uint16FieldType
import org.apache.kafka.message.FieldType.Uint32FieldType
import org.apache.kafka.message.FieldType.Uint64FieldType
import org.apache.kafka.message.FieldType.Uint8FieldType
import org.apache.kafka.message.MessageGenerator.capitalizeFirst
import org.apache.kafka.message.MessageGenerator.lowerCaseFirst
import org.apache.kafka.message.MessageGenerator.toSnakeCase

/**
 * @property ignorable Whether this field can be ignored from comparisons (and serialization?).
 */
class FieldSpec @JsonCreator constructor(
    @JsonProperty("name") val name: String,
    @JsonProperty("versions") versions: String?,
    @JsonProperty("fields") fields: List<FieldSpec>?,
    @JsonProperty("type") type: String,
    @JsonProperty("mapKey") val mapKey: Boolean,
    @JsonProperty("nullableVersions") nullableVersions: String?,
    @JsonProperty("default") val fieldDefault: String?,
    @JsonProperty("ignorable") val ignorable: Boolean,
    @JsonProperty("entityType") entityType: EntityType?,
    @JsonProperty("about") about: String?,
    @JsonProperty("taggedVersions") taggedVersions: String?,
    @JsonProperty("flexibleVersions") flexibleVersions: String?,
    @JsonProperty("tag") val tag: Int?,
    @JsonProperty("zeroCopy") val zeroCopy: Boolean,
) {

    /**
     * Versions for which this field is available.
     */
    val versions: Versions

    /**
     * List of the fields of this field, e.g. in case of a Struct.
     */
    @JsonProperty("fields")
    val fields: List<FieldSpec>

    val type: FieldType

    /**
     * Version range in which the field can be null.
     */
    val nullableVersions: Versions

    /**
     * The type of the inner entity, e.g.  in case of a collection.
     */
    @JsonProperty("entityType")
    val entityType: EntityType

    /**
     * Description of the field.
     */
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
            defaultVersions = if (this.taggedVersions.isEmpty) null else this.taggedVersions,
        ) ?: throw RuntimeException("You must specify the version of the $name structure.")

        this.fields = fields?.toList() ?: emptyList()

        val parsedNullableVersions = Versions.parse(nullableVersions, Versions.NONE)!!

        val parsedType = FieldType.parse(
            string = type,
            isNullable = !this.versions.intersect(parsedNullableVersions).isEmpty,
        )

        this.nullableVersions = parsedNullableVersions
        this.type = parsedType

        this.entityType = entityType ?: EntityType.UNKNOWN
        this.entityType.verifyTypeMatches(name, this.type)
        this.about = about ?: ""
        if (this.fields.isNotEmpty()) {
            if (!this.type.isArray && !this.type.isStruct)
                throw RuntimeException("Non-array or Struct field $name cannot have fields")
        }
        if (flexibleVersions.isNullOrEmpty()) this.flexibleVersions = null
        else {
            this.flexibleVersions = Versions.parse(flexibleVersions, null)
            if (!(this.type.isString || this.type.isBytes)) {
                // For now, only allow flexibleVersions overrides for the string and bytes
                // types. Overrides are only needed to keep compatibility with some old formats,
                // so there isn't any need to support them for all types.
                throw RuntimeException(
                    "Invalid flexibleVersions override for $name. Only fields of type string or " +
                            "bytes can specify a flexibleVersions override."
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
            if (taggedVersions.isEmpty) throw RuntimeException(
                "Field $name specifies a tag of $tag, but has no tagged versions. If a tag is " +
                        "specified, taggedVersions must be specified as well."
            )
            val nullableTaggedVersions = nullableVersions.intersect(taggedVersions)
            if (!(nullableTaggedVersions.isEmpty || nullableTaggedVersions == taggedVersions))
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
        } else if (!taggedVersions.isEmpty) throw RuntimeException(
            "Field $name does not specify a tag, but specifies tagged versions of " +
                    "$taggedVersions. Please specify a tag, or remove the taggedVersions."
        )
    }

    fun capitalizedCamelCaseName(): String = capitalizeFirst(name)

    fun camelCaseName(): String = lowerCaseFirst(name)

    /**
     * Returns the field name in camelcase and with `this.` prefixed.
     */
    fun prefixedCamelCaseName(): String = "this.${camelCaseName()}"

    /**
     * Returns the field's name with prefix "this." iff the type is not null.
     */
    fun safePrefixedCamelCaseName(prefix: String? = null): String =
        if (type.isNullable) camelCaseName()
        else "${prefix ?: "this."}${camelCaseName()}"

    /**
     * Returns the field name in camelcase and with `this@[className].` prefixed.
     */
    fun classPrefixedCamelCaseName(className: String): String =
        "this@$className.${camelCaseName()}"

    fun snakeCaseName(): String = toSnakeCase(name)

    @get:JsonProperty("versions")
    val versionsString: String
        get() = versions.toString()

    @get:JsonProperty("type")
    val typeString: String
        get() = type.toString()

    @get:JsonProperty("nullableVersions")
    val nullableVersionsString: String
        get() = nullableVersions.toString()

    @get:JsonProperty("taggedVersions")
    val taggedVersionsString: String
        get() = taggedVersions.toString()

    @get:JsonProperty("flexibleVersions")
    val flexibleVersionsString: String?
        get() = flexibleVersions?.toString()

    @get:JsonProperty("tag")
    val tagInteger: Int?
        get() = tag

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
        return when {
            // Nullable and fallback cases

            // In case of all versions nullable
            isNullDefaultAllowed() && fieldDefault == null -> "null"
            // In case non-nullable fallback value should be used
            fieldDefault.isNullOrEmpty() -> getFallbackValue(headerGenerator, structRegistry)
            // In case of nullable and default "null" (string)
            fieldDefault == "null" -> {
                requireNullDefaultAllowed() // all versions must be nullable
                "null"
            }

            type is RecordsFieldType -> "null" // ignore fieldDefault for RecordsFieldType

            // Parsing cases (non-nullable cases with default value provided)
            type is BoolFieldType ->
                // Parse field type
                if (fieldDefault.equals("true", ignoreCase = true)) "true"
                else if (fieldDefault.equals("false", ignoreCase = true)) "false"
                else throw RuntimeException("Invalid default for boolean field $name: $fieldDefault")

            type.isPrimitive -> {
                val (base, numberAsString) = getBaseAndDefaultString(fieldDefault)
                when (type) { // All the numeric field types
                    is Int8FieldType -> parseInt8String(base, numberAsString)
                    is Int16FieldType -> parseInt16String(base, numberAsString)
                    is Int32FieldType -> parseInt32String(base, numberAsString)
                    is Int64FieldType -> parseInt64String(base, numberAsString)
                    is Uint8FieldType -> parseUInt8String(base, numberAsString)
                    is Uint16FieldType -> parseUInt16String(base, numberAsString)
                    is Uint32FieldType -> parseUInt32String(base, numberAsString)
                    is Uint64FieldType -> parseUInt64String(base, numberAsString)
                    is Float32FieldType -> parseFloat32String(fieldDefault)
                    is Float64FieldType -> parseFloat64String(fieldDefault)
                    else -> throw RuntimeException(
                        "Unsupported default value $fieldDefault for primitive field type $type"
                    )
                }
            }

            type is StringFieldType -> "\"$fieldDefault\""
            type is UUIDFieldType -> parseUUIDString(headerGenerator)

            // Error cases / Invalid input (null or empty fieldDefault values already returned)
            type is StructType -> throw RuntimeException(
                "Invalid default for struct field $name: custom defaults are not supported for struct fields."
            )

            type is ArrayType -> throw RuntimeException(
                "Invalid default for array field $name. The only valid default for an array " +
                        "field is the empty array (empty string) or null (undefined or string \"null\")."
            )

            type is BytesFieldType -> throw RuntimeException(
                "Invalid default for bytes field $name. The only valid default for a bytes field is empty or null."
            )

            else -> throw RuntimeException("Unsupported field type $type")
        }
    }

    /**
     * Returns a non-null (except in for RecordsType) fallback field value that can be used in case [fieldDefault]
     * was null or empty.
     */
    private fun getFallbackValue(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        return when (type) {
            // Numeric
            is BoolFieldType -> "false"
            is Int8FieldType -> "0.toByte()"
            is Int16FieldType -> "0.toShort()"
            is Int32FieldType -> "0"
            is Int64FieldType -> "0L"
            is Uint8FieldType -> "0.toUByte()"
            is Uint16FieldType -> "0.toUShort()"
            is Uint32FieldType -> "0.toUInt()"
            is Uint64FieldType -> "0.toULong()"
            is Float32FieldType -> "0.0f"
            is Float64FieldType -> "0.0"

            // Lists / Arrays
            is ArrayType -> when (type.elementType) {
                is BoolFieldType -> "booleanArrayOf()"
                is Int8FieldType -> "byteArrayOf()"
                is Int16FieldType -> "shortArrayOf()"
                is Int32FieldType -> "intArrayOf()"
                is Int64FieldType -> "longArrayOf()"
                is Uint8FieldType -> "uByteArrayOf()"
                is Uint16FieldType -> "uShortArrayOf()"
                is Uint32FieldType -> "uIntArrayOf()"
                is Uint64FieldType -> "uLongArrayOf()"
                is Float32FieldType -> "floatArrayOf()"
                is Float64FieldType -> "doubleArrayOf()"
                else -> {
                    if (structRegistry.isStructArrayWithKeys(this))
                        collectionType(type.elementType.toString()) + "()"
                    else "emptyList()"
                }
            }

            // Classes (incl. ByteArray, since part of BytesFieldType)
            is StructType -> "$type()"
            is BytesFieldType -> if (zeroCopy) {
                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS)
                "ByteUtils.EMPTY_BUF"
            } else "byteArrayOf()"

            is RecordsFieldType -> "null" // TODO Use dummy instance to allow non-nullable fields
            is StringFieldType -> "\"\""
            is UUIDFieldType -> {
                headerGenerator.addImport(MessageGenerator.UUID_CLASS)
                "Uuid.ZERO_UUID"
            }
        }
    }

    /**
     * Checks if [text] is a valid int8 representation and returns the String representation
     * for instantiating that int8.
     */
    private fun parseInt8String(base: Int, text: String): String {
        val isNegative: Boolean
        try {
            isNegative = text.toByte(base) < 0
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for int8 field $name: $text", e)
        }
        return if (isNegative) "($fieldDefault).toByte()"
        else "$fieldDefault.toByte()"
    }

    /**
     * Checks if [text] is a valid int16 number representation and returns the String representation
     * for instantiating that int16.
     */
    private fun parseInt16String(base: Int, text: String): String {
        val isNegative: Boolean
        try {
            isNegative = text.toShort(base) < 0
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for int16 field $name: $text", e)
        }
        return if (isNegative) "($fieldDefault).toShort()"
        else "$fieldDefault.toShort()"
    }

    /**
     * Checks if [text] is a valid int32 number representation and returns the String representation
     * for instantiating that int32.
     */
    private fun parseInt32String(base: Int, text: String): String {
        try {
            text.toInt(base)
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for int32 field $name: $text", e)
        }
        return "$fieldDefault"
    }

    /**
     * Checks if [text] is a valid int64 number representation and returns the String representation
     * for instantiating that int64.
     */
    private fun parseInt64String(base: Int, text: String): String {
        try {
            text.toLong(base)
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for int64 field $name: $text", e)
        }
        return "${fieldDefault}L"
    }

    /**
     * Checks if [text] is a valid uint8 representation and returns the String representation
     * for instantiating that uint8.
     */
    private fun parseUInt8String(base: Int, text: String): String {
        try {
            val value = text.toUByte(base)
            if (value < 0U || value > UByte.MAX_VALUE)
                throw RuntimeException("Invalid default for uint8 field $name: out of range.")
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for uint8 field $name: $text", e)
        }
        return "${fieldDefault}u.toUByte()"
    }

    /**
     * Checks if [text] is a valid uint16 representation and returns the String representation
     * for instantiating that uint16.
     */
    private fun parseUInt16String(base: Int, text: String): String {
        try {
            val value = text.toUShort(base)
            if (value < 0U || value > UShort.MAX_VALUE)
                throw RuntimeException("Invalid default for uint16 field $name: out of range.")
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for uint16 field $name: $text", e)
        }
        return "${fieldDefault}u.toUShort()"
    }

    /**
     * Checks if [text] is a valid uint32 representation and returns the String representation
     * for instantiating that uint32.
     */
    private fun parseUInt32String(base: Int, text: String): String {
        try {
            val value = text.toUInt(base)
            if (value < 0U || value > UInt.MAX_VALUE)
                throw RuntimeException("Invalid default for uint32 field $name: out of range.")
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for uint32 field $name: $text", e)
        }
        return "${fieldDefault}u"
    }

    /**
     * Checks if [text] is a valid uint64 representation and returns the String representation
     * for instantiating that uint64.
     */
    private fun parseUInt64String(base: Int, text: String): String {
        try {
            val value = text.toULong(base)
            if (value < 0U || value > ULong.MAX_VALUE)
                throw RuntimeException("Invalid default for uint64 field $name: out of range.")
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for uint64 field $name: $text", e)
        }
        return "${fieldDefault}uL"
    }

    /**
     * Checks if [text] is a valid float32 representation and returns the String representation
     * for instantiating that float32.
     */
    private fun parseFloat32String(text: String): String {
        try {
            text.toFloat()
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for float32 field $name: $text", e)
        }
        return "\"$text\".toFloat()"
    }

    /**
     * Checks if [text] is a valid float64 representation and returns the String representation
     * for instantiating that float64.
     */
    private fun parseFloat64String(text: String): String {
        try {
            text.toDouble()
        } catch (e: NumberFormatException) {
            throw RuntimeException("Invalid default for float64 field $name: $text", e)
        }
        return "\"$text\".toDouble()"
    }

    private fun parseUUIDString(headerGenerator: HeaderGenerator): String {
        try {
            val uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(fieldDefault))
            uuidBytes.getLong()
            uuidBytes.getLong()
        } catch (e: IllegalArgumentException) {
            throw RuntimeException("Invalid default for uuid field $name: $fieldDefault", e)
        }
        headerGenerator.addImport(MessageGenerator.UUID_CLASS)
        return "Uuid.fromString(\"$fieldDefault\")"
    }

    private fun getBaseAndDefaultString(fieldDefault: String): Pair<Int, String> {
        var base = 10
        var defaultString = fieldDefault
        if (defaultString.startsWith("0x")) {
            base = 16
            defaultString = defaultString.substring(2)
        }
        return Pair(base, defaultString)
    }

    private fun requireNullDefaultAllowed() {
        if (!isNullDefaultAllowed()) throw RuntimeException(
            "null cannot be the default for field $name, because not all versions of this field are nullable."
        )
    }

    /**
     * Returns `true` iff all versions are nullable versions.
     */
    private fun isNullDefaultAllowed(): Boolean = nullableVersions.contains(versions)

    /**
     * Get the abstract Kotlin type of the field-- for example, List.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @return The abstract java type name.
     */
    fun fieldAbstractKotlinType(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        return when {
            type is BoolFieldType
                    || type is Int8FieldType
                    || type is Int16FieldType
                    || type is Uint16FieldType
                    || type is Uint32FieldType
                    || type is Int32FieldType
                    || type is Int64FieldType
                    || type is UUIDFieldType
                    || type is Float32FieldType
                    || type is Float64FieldType
                    || type is RecordsFieldType
                    || type.isString
                    || (type.isBytes && zeroCopy) -> type.getBoxedKotlinType(headerGenerator)!!

            type.isBytes -> "ByteArray" + if (type.isNullable) "?" else ""
            type.isStruct -> capitalizeFirst(typeString) + if (type.isNullable) "?" else ""
            type.isArray -> {
                val arrayType = type as ArrayType
                if (structRegistry.isStructArrayWithKeys(this)) {
                    headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS)
                    collectionType(arrayType.elementType.toString()) + if (type.isNullable) "?" else ""
                } else when (arrayType.elementType) {
                    is BoolFieldType -> "BooleanArray"
                    is Int8FieldType -> "ByteArray"
                    is Uint8FieldType -> "UByteArray"
                    is Int16FieldType -> "ShortArray"
                    is Uint16FieldType -> "UShortArray"
                    is Int32FieldType -> "IntArray"
                    is Uint32FieldType -> "UIntArray"
                    is Int64FieldType -> "LongArray"
                    is Uint64FieldType -> "ULongArray"
                    is Float32FieldType -> "FloatArray"
                    is Float64FieldType -> "DoubleArray"
                    else -> {
                        headerGenerator.addImport(MessageGenerator.LIST_CLASS)
                        String.format("List<%s>", arrayType.elementType.getBoxedKotlinType(headerGenerator))
                    }
                } + if (type.isNullable) "?" else ""
            }

            else -> throw RuntimeException("Unknown field type $type")
        }
    }

    /**
     * Get the concrete Kotlin type of the field-- for example, ArrayList.
     *
     * @param headerGenerator The header generator in case we need to add imports.
     * @param structRegistry The struct registry in case we need to look up structs.
     * @return The abstract kotlin type name.
     */
    fun concreteKotlinType(
        headerGenerator: HeaderGenerator,
        structRegistry: StructRegistry,
    ): String {
        return if (type.isArray) {
            val arrayType = type as ArrayType
            if (structRegistry.isStructArrayWithKeys(this))
                collectionType(arrayType.elementType.toString())
            else {
                return when (arrayType.elementType) {
                    is BoolFieldType -> "BooleanArray"
                    is Int8FieldType -> "ByteArray"
                    is Uint8FieldType -> "UByteArray"
                    is Int16FieldType -> "ShortArray"
                    is Uint16FieldType -> "UShortArray"
                    is Int32FieldType -> "IntArray"
                    is Uint32FieldType -> "UIntArray"
                    is Int64FieldType -> "LongArray"
                    is Uint64FieldType -> "ULongArray"
                    is Float32FieldType -> "FloatArray"
                    is Float64FieldType -> "DoubleArray"
                    else -> {
                        headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS)
                        String.format(
                            "ArrayList<%s>",
                            arrayType.elementType.getBoxedKotlinType(headerGenerator)
                        )
                    }
                }
            }
        } else fieldAbstractKotlinType(headerGenerator, structRegistry)
    }

    /**
     * Generate an if statement that checks if this field has a non-default value.
     *
     * Note that the field has to be immutable if it is nullable.
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
        fromIfNotNull: Boolean = false,
    ) {
        val fieldDefaultString = fieldDefault(headerGenerator, structRegistry)
        val prefixedFieldName = "${fieldPrefix ?: ""}${camelCaseName()}"

        when {
            // All fields with null default (includes Records too)
            fieldDefaultString == "null" ->
                buffer.printf("if (%s != null) {%n", prefixedFieldName)

            type is ArrayType && type.isNullable ->
                buffer.printf(
                    "if (%s%s.isNotEmpty()) {%n",
                    if (fromIfNotNull) "" else "$prefixedFieldName == null || ",
                    prefixedFieldName,
                )

            type is ArrayType -> buffer.printf("if (%s.isNotEmpty()) {%n", prefixedFieldName)

            type is BytesFieldType && type.isNullable ->
                if(zeroCopy) buffer.printf(
                    "if (%s%s.remaining() > 0) {%n",
                    if (fromIfNotNull) "" else "$prefixedFieldName == null || ",
                    prefixedFieldName,
                )
                else buffer.printf(
                    "if (%s%s.isNotEmpty()) {%n",
                    if (fromIfNotNull) "" else "$prefixedFieldName == null || ",
                    prefixedFieldName,
                )

            type is BytesFieldType ->
                if(zeroCopy) buffer.printf("if (%s.remaining() > 0) {%n", prefixedFieldName)
                else buffer.printf("if (%s.isNotEmpty()) {%n", prefixedFieldName)

            type is StringFieldType ||type is StructType || type is BoolFieldType || type.isPrimitive ->
                buffer.printf("if (%s != %s) {%n", prefixedFieldName, fieldDefaultString)

            type is UUIDFieldType -> {
                headerGenerator.addImport(MessageGenerator.UUID_CLASS)
                buffer.printf("if (%s != Uuid.ZERO_UUID) {%n", prefixedFieldName)
            }
        }
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
        )
        buffer.incrementIndent()
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS)
        buffer.printf(
            "throw UnsupportedVersionException(\"Attempted to write a non-default %s at version \$version\")%n",
            camelCaseName()
        )
        buffer.decrementIndent()
        buffer.printf("}%n")
    }

    companion object {

        private val VALID_FIELD_NAMES = Pattern.compile("[A-Za-z]([A-Za-z0-9]*)")

        fun collectionType(baseType: String): String = baseType + "Collection"

        fun primitiveArrayType(baseType: FieldType): String = when (baseType) {
            is BoolFieldType -> "BooleanArray"
            is Int8FieldType -> "ByteArray"
            is Uint8FieldType -> "UByteArray"
            is Int16FieldType -> "ShortArray"
            is Uint16FieldType -> "UShortArray"
            is Int32FieldType -> "IntArray"
            is Uint32FieldType -> "UIntArray"
            is Int64FieldType -> "LongArray"
            is Uint64FieldType -> "ULongArray"
            is Float32FieldType -> "FloatArray"
            is Float64FieldType -> "DoubleArray"
            else -> throw RuntimeException("Base type $baseType is not a supported primitive type.")
        }
    }
}
