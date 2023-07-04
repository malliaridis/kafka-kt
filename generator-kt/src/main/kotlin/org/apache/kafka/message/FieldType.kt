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
     * Returns `true` if this field type is compatible with nullability.
     */
    fun canBeNullable(): Boolean = false

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
    class BoolFieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Boolean"

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = BoolFieldType()

            internal const val NAME = "bool"
        }
    }

    /**
     * Field type of [kotlin.Byte].
     */
    class Int8FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Byte"

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int8FieldType()

            internal const val NAME = "int8"
        }
    }

    /**
     * Field type of [kotlin.UByte].
     */
    class Uint8FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "UByte"

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint8FieldType()

            internal const val NAME = "uint8"
        }
    }

    /**
     * Field type of [kotlin.Short].
     */
    class Int16FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Short"

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int16FieldType()

            internal const val NAME = "int16"
        }
    }

    /**
     * Field type of [kotlin.UShort].
     */
    class Uint16FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "UShort"

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint16FieldType()

            internal const val NAME = "uint16"
        }
    }

    /**
     * Field type of [kotlin.Int].
     */
    class Int32FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Int"

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int32FieldType()

            internal const val NAME = "int32"
        }
    }

    /**
     * Field type of [kotlin.UInt].
     */
    class Uint32FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "UInt"

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint32FieldType()

            internal const val NAME = "uint32"
        }
    }

    /**
     * Field type of [kotlin.Int].
     */
    class Int64FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Long"

        override fun fixedLength(): Int = 8

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int64FieldType()

            internal const val NAME = "int64"
        }
    }

    /**
     * Field type of [kotlin.UInt].
     */
    class Uint64FieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "ULong"

        override fun fixedLength(): Int = 8

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint64FieldType()

            internal const val NAME = "uint64"
        }
    }

    /**
     * Field type of [java.util.UUID].
     */
    class UUIDFieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS)
            return "Uuid"
        }

        override fun fixedLength(): Int = 16

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = UUIDFieldType()

            internal const val NAME = "uuid"
        }
    }

    /**
     * Field type of [kotlin.Float].
     */
    class Float32FieldType : FieldType {

        override fun fixedLength(): Int = 4

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Float"

        override val isFloat: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Float32FieldType()

            internal const val NAME = "float32"
        }
    }

    /**
     * Field type of [kotlin.Double].
     */
    class Float64FieldType : FieldType {

        override fun fixedLength(): Int = 8

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "Double"

        override val isFloat: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Float64FieldType()

            internal const val NAME = "float64"
        }
    }

    /**
     * Field type of [kotlin.String].
     */
    class StringFieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = "String"

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isString: Boolean = true

        override fun canBeNullable(): Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = StringFieldType()

            internal const val NAME = "string"
        }
    }

    /**
     * Field type of [kotlin.ByteArray].
     */
    class BytesFieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS)
            return "ByteBuffer"
        }

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isBytes: Boolean = true

        override fun canBeNullable(): Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = BytesFieldType()

            internal const val NAME = "bytes"
        }
    }

    class RecordsFieldType : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String {
            headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS)
            return "BaseRecords"
        }

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isRecords: Boolean = true

        override fun canBeNullable(): Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = RecordsFieldType()

            internal const val NAME = "records"
        }
    }

    class StructType internal constructor(private val type: String) : FieldType {

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String = type

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isStruct: Boolean = true

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("typeName"),
        )
        fun typeName(): String = type

        val typeName: String = type

        override fun toString(): String = type
    }

    class ArrayType internal constructor(val elementType: FieldType) : FieldType {

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override fun getBoxedKotlinType(headerGenerator: HeaderGenerator): String =
            throw UnsupportedOperationException()

        override val isArray: Boolean = true

        override val isStructArray: Boolean = elementType.isStruct

        override fun canBeNullable(): Boolean = true

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("elementType"),
        )
        fun elementType(): FieldType = elementType

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("elementName"),
        )
        fun elementName(): String = elementType.toString()

        val elementName: String = elementType.toString()

        override fun toString(): String = "[]$elementType"
    }

    companion object {

        fun parse(string: String): FieldType =  when (val trimmed = string.trim { it <= ' ' }) {
            BoolFieldType.NAME -> BoolFieldType.INSTANCE
            Int8FieldType.NAME -> Int8FieldType.INSTANCE
            Uint8FieldType.NAME -> Uint8FieldType.INSTANCE
            Int16FieldType.NAME -> Int16FieldType.INSTANCE
            Uint16FieldType.NAME -> Uint16FieldType.INSTANCE
            Int32FieldType.NAME -> Int32FieldType.INSTANCE
            Uint32FieldType.NAME -> Uint32FieldType.INSTANCE
            Int64FieldType.NAME -> Int64FieldType.INSTANCE
            Uint64FieldType.NAME -> Uint64FieldType.INSTANCE
            UUIDFieldType.NAME -> UUIDFieldType.INSTANCE
            Float32FieldType.NAME -> Float32FieldType.INSTANCE
            Float64FieldType.NAME -> Float64FieldType.INSTANCE
            StringFieldType.NAME -> StringFieldType.INSTANCE
            BytesFieldType.NAME -> BytesFieldType.INSTANCE
            RecordsFieldType.NAME -> RecordsFieldType.INSTANCE
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
                ArrayType(elementType)

            } else if (MessageGenerator.firstIsCapitalized(trimmed)) StructType(trimmed)
            else throw RuntimeException("Can't parse type $trimmed")
        }

        const val ARRAY_PREFIX = "[]"
    }
}
