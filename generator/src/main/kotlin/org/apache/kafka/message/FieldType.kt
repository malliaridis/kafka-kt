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

    fun getBoxedJavaType(headerGenerator: HeaderGenerator): String?

    /**
     * Returns true if this is an array type.
     */
    val isArray: Boolean
        get() = false

    /**
     * Returns true if this is an array of structures.
     */
    val isStructArray: Boolean
        get() = false

    /**
     * Returns true if the serialization of this type is different in flexible versions.
     */
    fun serializationIsDifferentInFlexibleVersions(): Boolean = false

    /**
     * Returns true if this is a string type.
     */
    val isString: Boolean
        get() = false

    /**
     * Returns true if this is a bytes type.
     */
    val isBytes: Boolean
        get() = false

    /**
     * Returns true if this is a records type
     */
    val isRecords: Boolean
        get() = false

    /**
     * Returns true if this is a floating point type.
     */
    val isFloat: Boolean
        get() = false

    /**
     * Returns true if this is a struct type.
     */
    val isStruct: Boolean
        get() = false

    /**
     * Returns true if this field type is compatible with nullability.
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

    class BoolFieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Boolean"

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = BoolFieldType()

            internal const val NAME = "bool"
        }
    }

    class Int8FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Byte"

        override fun fixedLength(): Int = 1

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int8FieldType()

            internal const val NAME = "int8"
        }
    }

    class Int16FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Short"

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int16FieldType()

            internal const val NAME = "int16"
        }
    }

    class Uint16FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Integer"

        override fun fixedLength(): Int = 2

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint16FieldType()

            internal const val NAME = "uint16"
        }
    }

    class Int32FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Integer"

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int32FieldType()

            internal const val NAME = "int32"
        }
    }

    class Uint32FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Long"

        override fun fixedLength(): Int = 4

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Uint32FieldType()

            internal const val NAME = "uint32"
        }
    }

    class Int64FieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Long"

        override fun fixedLength(): Int = 8

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Int64FieldType()

            internal const val NAME = "int64"
        }
    }

    class UUIDFieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String {
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

    class Float64FieldType : FieldType {

        override fun fixedLength(): Int = 8

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "Double"

        override val isFloat: Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = Float64FieldType()

            internal const val NAME = "float64"
        }
    }

    class StringFieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = "String"

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isString: Boolean = true

        override fun canBeNullable(): Boolean = true

        override fun toString(): String = NAME

        companion object {

            val INSTANCE = StringFieldType()

            internal const val NAME = "string"
        }
    }

    class BytesFieldType : FieldType {

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String {
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

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String {
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

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String = type

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override val isStruct: Boolean = true

        fun typeName(): String = type

        override fun toString(): String = type
    }

    class ArrayType internal constructor(private val elementType: FieldType) : FieldType {

        override fun serializationIsDifferentInFlexibleVersions(): Boolean = true

        override fun getBoxedJavaType(headerGenerator: HeaderGenerator): String =
            throw UnsupportedOperationException()

        override val isArray: Boolean = true

        override val isStructArray: Boolean = elementType.isStruct

        override fun canBeNullable(): Boolean = true

        fun elementType(): FieldType = elementType

        fun elementName(): String = elementType.toString()

        override fun toString(): String = "[]$elementType"
    }

    companion object {

        fun parse(string: String): FieldType =  when (val trimmed = string.trim { it <= ' ' }) {
            BoolFieldType.NAME -> BoolFieldType.INSTANCE
            Int8FieldType.NAME -> Int8FieldType.INSTANCE
            Int16FieldType.NAME -> Int16FieldType.INSTANCE
            Uint16FieldType.NAME -> Uint16FieldType.INSTANCE
            Uint32FieldType.NAME -> Uint32FieldType.INSTANCE
            Int32FieldType.NAME -> Int32FieldType.INSTANCE
            Int64FieldType.NAME -> Int64FieldType.INSTANCE
            UUIDFieldType.NAME -> UUIDFieldType.INSTANCE
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
