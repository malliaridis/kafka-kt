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

interface FieldType {

    fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String?

    /**
     * Returns `true` if this is an array type.
     */
    val isArray: Boolean
        get() = false

    /**
     * Returns `true` if this is an array of structures.
     */
    val isStructArray: Boolean
        get() = false

    val isPrimitive: Boolean

    /**
     * Returns `true` if the serialization of this type is different in flexible versions.
     */
    fun serializationIsDifferentInFlexibleVersions(): Boolean = false

    /**
     * Returns `true` if this is a string type.
     */
    val isString: Boolean
        get() = false

    /**
     * Returns `true` if this is a bytes type.
     */
    val isBytes: Boolean
        get() = false

    /**
     * Returns `true` if this is a records type
     */
    val isRecords: Boolean
        get() = false

    val isLazilyLoaded: Boolean
        get() = false

    /**
     * Returns `true` if this is a floating point type.
     */
    val isFloat: Boolean
        get() = false

    /**
     * Returns `true` if this is a struct type.
     */
    val isStruct: Boolean
        get() = false

    /**
     * `true` if this field type has nullable versions.
     */
    val isNullable: Boolean
        get() = false

    /**
     * Gets the fixed length of the field, or None if the field is variable-length.
     */
    fun fixedLength(): Int? = null

    val isVariableLength: Boolean
        get() = fixedLength() == null

    /**
     * Convert the field type to a JSON string.
     */
    override fun toString(): String

    /**
     * Field type of [kotlin.Boolean].
     */
    class BoolFieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Boolean" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = BoolFieldType()

            val NULLABLE_INSTANCE = BoolFieldType(isNullable = true)

            internal const val NAME = "bool"
        }
    }

    /**
     * Field type of [kotlin.Byte].
     */
    class Int8FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Byte" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int8FieldType()

            val NULLABLE_INSTANCE = Int8FieldType(isNullable = true)

            internal const val NAME = "int8"
        }
    }

    /**
     * Field type of [kotlin.UByte].
     */
    class Uint8FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "UByte" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint8FieldType()

            val NULLABLE_INSTANCE = Uint8FieldType(isNullable = true)

            internal const val NAME = "uint8"
        }
    }

    /**
     * Field type of [kotlin.Short].
     */
    class Int16FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Short" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int16FieldType()

            val NULLABLE_INSTANCE = Int16FieldType(isNullable = true)

            internal const val NAME = "int16"
        }
    }

    /**
     * Field type of [kotlin.UShort].
     */
    class Uint16FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "UShort" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint16FieldType()

            val NULLABLE_INSTANCE = Uint16FieldType(isNullable = true)

            internal const val NAME = "uint16"
        }
    }

    /**
     * Field type of [kotlin.Int].
     */
    class Int32FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Int" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int32FieldType()

            val NULLABLE_INSTANCE = Int32FieldType(isNullable = true)

            internal const val NAME = "int32"
        }
    }

    /**
     * Field type of [kotlin.UInt].
     */
    class Uint32FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "UInt" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint32FieldType()

            val NULLABLE_INSTANCE = Uint32FieldType(isNullable = true)

            internal const val NAME = "uint32"
        }
    }

    /**
     * Field type of [kotlin.Int].
     */
    class Int64FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Long" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 8

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int64FieldType()

            val NULLABLE_INSTANCE = Int64FieldType(isNullable = true)

            internal const val NAME = "int64"
        }
    }

    /**
     * Field type of [kotlin.UInt].
     */
    class Uint64FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "ULong" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override fun fixedLength(): Int = 8

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint64FieldType()

            val NULLABLE_INSTANCE = Uint64FieldType(isNullable = true)

            internal const val NAME = "uint64"
        }
    }

    /**
     * Field type of [java.util.UUID].
     */
    class UUIDFieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS)
            return "Uuid"
        }

        override val isPrimitive: Boolean = false

        override fun fixedLength(): Int = 16

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = UUIDFieldType()

            val NULLABLE_INSTANCE = UUIDFieldType(isNullable = true)

            internal const val NAME = "uuid"
        }
    }

    /**
     * Field type of [kotlin.Float].
     */
    class Float32FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun fixedLength(): Int = 4

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Float" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override val isFloat: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Float32FieldType()

            val NULLABLE_INSTANCE = Float32FieldType(isNullable = true)

            internal const val NAME = "float32"
        }
    }

    /**
     * Field type of [kotlin.Double].
     */
    class Float64FieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun fixedLength(): Int = 8

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "Double" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = true

        override val isFloat: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Float64FieldType()

            val NULLABLE_INSTANCE = Float64FieldType(isNullable = true)

            internal const val NAME = "float64"
        }
    }

    /**
     * Field type of [kotlin.String].
     */
    class StringFieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            "String" + if (isNullable) "?" else ""

        override val isPrimitive: Boolean = false

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isString: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = StringFieldType()

            val NULLABLE_INSTANCE = StringFieldType(isNullable = true)

            internal const val NAME = "string"
        }
    }

    /**
     * Field type of [kotlin.ByteArray].
     */
    class BytesFieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS)
            return "ByteBuffer" + if (isNullable) "?" else ""
        }

        override val isPrimitive: Boolean = false

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isBytes: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = BytesFieldType()

            val NULLABLE_INSTANCE = BytesFieldType(isNullable = true)

            internal const val NAME = "bytes"
        }
    }

    class RecordsFieldType(
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS)
            return "BaseRecords" + if (isNullable) "?" else ""
        }

        override val isPrimitive: Boolean = false

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isRecords: Boolean = true

        // Records should be lazily loaded if not nullable
        override val isLazilyLoaded: Boolean = !isNullable

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = RecordsFieldType()

            val NULLABLE_INSTANCE = RecordsFieldType(isNullable = true)

            internal const val NAME = "records"
        }
    }

    class StructType internal constructor(
        private val type: String,
        override val isNullable: Boolean = false,
    ) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = type

        override val isPrimitive: Boolean = false

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isStruct: Boolean = true

        val typeName: String = type

        override fun toString(): String = type
    }

    class ArrayType internal constructor(
        val elementType: FieldType,
        override val isNullable: Boolean = false,
    ) : FieldType {

        init {
            require(!elementType.isPrimitive || !elementType.isNullable) {
                "Nullable element types of primitive type arrays are not supported."
            }
        }

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            throw UnsupportedOperationException()

        override val isPrimitive: Boolean = false

        override val isArray: Boolean = true

        override val isStructArray: Boolean = elementType.isStruct

        val elementName: String = elementType.toString()

        override fun toString(): String = "[]$elementType"
    }

    companion object {

        fun parse(
            string: String,
            isNullable: Boolean = false,
        ): FieldType =  when (val trimmed = string.trim { it <= ' ' }) {
            BoolFieldType.NAME ->
                if (isNullable) BoolFieldType.NULLABLE_INSTANCE
                else BoolFieldType.INSTANCE
            Int8FieldType.NAME ->
                if (isNullable) Int8FieldType.NULLABLE_INSTANCE
                else Int8FieldType.INSTANCE
            Uint8FieldType.NAME ->
                if (isNullable) Uint8FieldType.NULLABLE_INSTANCE
                else Uint8FieldType.INSTANCE
            Int16FieldType.NAME ->
                if (isNullable) Int16FieldType.NULLABLE_INSTANCE
                else Int16FieldType.INSTANCE
            Uint16FieldType.NAME ->
                if (isNullable) Uint16FieldType.NULLABLE_INSTANCE
                else Uint16FieldType.INSTANCE
            Int32FieldType.NAME ->
                if (isNullable) Int32FieldType.NULLABLE_INSTANCE
                else Int32FieldType.INSTANCE
            Uint32FieldType.NAME ->
                if (isNullable) Uint32FieldType.NULLABLE_INSTANCE
                else Uint32FieldType.INSTANCE
            Int64FieldType.NAME ->
                if (isNullable) Int64FieldType.NULLABLE_INSTANCE
                else Int64FieldType.INSTANCE
            Uint64FieldType.NAME ->
                if (isNullable) Uint64FieldType.NULLABLE_INSTANCE
                else Uint64FieldType.INSTANCE
            UUIDFieldType.NAME ->
                if (isNullable) UUIDFieldType.NULLABLE_INSTANCE
                else UUIDFieldType.INSTANCE
            Float32FieldType.NAME ->
                if (isNullable) Float32FieldType.NULLABLE_INSTANCE
                else Float32FieldType.INSTANCE
            Float64FieldType.NAME ->
                if (isNullable) Float64FieldType.NULLABLE_INSTANCE
                else Float64FieldType.INSTANCE
            StringFieldType.NAME ->
                if (isNullable) StringFieldType.NULLABLE_INSTANCE
                else StringFieldType.INSTANCE
            BytesFieldType.NAME ->
                if (isNullable) BytesFieldType.NULLABLE_INSTANCE
                else BytesFieldType.INSTANCE
            RecordsFieldType.NAME ->
                if (isNullable) RecordsFieldType.NULLABLE_INSTANCE
                else RecordsFieldType.INSTANCE
            else -> if (trimmed.startsWith(ARRAY_PREFIX)) {
                val elementTypeString = trimmed.substring(ARRAY_PREFIX.length)
                if (elementTypeString.isEmpty()) throw RuntimeException(
                    "Can't parse array type $trimmed. No element type found."
                )

                val elementType = parse(elementTypeString)
                if (elementType.isArray) throw RuntimeException(
                    "Can't have an array of arrays. Use an array of structs containing an " +
                            "array instead."
                )
                ArrayType(elementType, isNullable)

            } else if (MessageGenerator.firstIsCapitalized(trimmed)) StructType(trimmed, isNullable)
            else throw RuntimeException("Can't parse type $trimmed")
        }

        const val ARRAY_PREFIX = "[]"
    }
}
