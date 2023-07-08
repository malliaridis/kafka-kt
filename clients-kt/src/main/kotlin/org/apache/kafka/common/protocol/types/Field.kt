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

package org.apache.kafka.common.protocol.types

open class Field(
    val name: String,
    val type: Type,
    val docString: String? = null,
    val hasDefaultValue: Boolean = false,
    val defaultValue: Any? = null,
) {

    init {
        if (hasDefaultValue) type.validate(defaultValue)
    }

    constructor(
        name: String,
        type: Type,
        docString: String?,
        defaultValue: Any?,
    ) : this(
        name = name,
        type = type,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class Bool(
        name: String,
        docString: String?,
        defaultValue: Boolean? = null,
    ) : Field(
        name = name,
        type = Type.BOOLEAN,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableBool(
        name: String,
        docString: String?,
        defaultValue: Boolean? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_BOOLEAN,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue,
    )

    class Int8(
        name: String,
        docString: String?,
        defaultValue: Byte? = null,
    ) : Field(
        name = name,
        type = Type.INT8,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue,
    )

    class NullableInt8(
        name: String,
        docString: String?,
        defaultValue: Byte? = null,
    ) : Field(
        name = name,
        type = Type.INT8,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue,
    )

    class Uint8(
        name: String,
        docString: String?,
        defaultValue: UByte? = null,
    ) : Field(
        name = name,
        type = Type.UINT8,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue,
    )

    class NullableUint8(
        name: String,
        docString: String?,
        defaultValue: UByte? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_UINT8,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue,
    )

    class Int16(
        name: String,
        docString: String?,
        defaultValue: Short? = null,
    ) : Field(
        name = name,
        type = Type.INT16,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue,
    )

    class NullableInt16(
        name: String,
        docString: String?,
        defaultValue: Short? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_INT16,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue,
    )

    class Uint16(
        name: String,
        docString: String?,
        defaultValue: UShort? = null,
    ) : Field(
        name = name,
        type = Type.UINT16,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue,
    )

    class NullableUint16(
        name: String,
        docString: String?,
        defaultValue: UShort? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_UINT16,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue,
    )

    class Int32(
        name: String,
        docString: String?,
        defaultValue: Int? = null,
    ) : Field(
        name = name,
        type = Type.INT32,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableInt32(
        name: String,
        docString: String?,
        defaultValue: Int? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_INT32,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class Uint32(
        name: String,
        docString: String?,
    ) : Field(
        name = name,
        type = Type.UNSIGNED_INT32,
        docString = docString,
        hasDefaultValue = false,
    )

    class NullableUint32(
        name: String,
        docString: String?,
    ) : Field(
        name = name,
        type = Type.NULLABLE_UINT32,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = null,
    )

    class Int64(
        name: String,
        docString: String?,
        defaultValue: Long? = null,
    ) : Field(
        name = name,
        type = Type.INT64,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableInt64(
        name: String,
        docString: String?,
        defaultValue: Long? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_INT64,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class UUID(
        name: String,
        docString: String?,
        defaultValue: UUID? = null,
    ) : Field(
        name = name,
        type = Type.UUID,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableUUID(
        name: String,
        docString: String?,
        defaultValue: UUID? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_UUID,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class Float32(
        name: String,
        docString: String?,
        defaultValue: Double? = null,
    ) : Field(
        name = name,
        type = Type.FLOAT32,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableFloat32(
        name: String,
        docString: String?,
        defaultValue: Double? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_FLOAT32,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class Float64(
        name: String,
        docString: String?,
        defaultValue: Double? = null,
    ) : Field(
        name = name,
        type = Type.FLOAT64,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableFloat64(
        name: String,
        docString: String?,
        defaultValue: Double? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_FLOAT64,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class Str(
        name: String,
        docString: String?,
        defaultValue: String? = null,
    ) : Field(
        name = name,
        type = Type.STRING,
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue,
    )

    class CompactStr(
        name: String,
        docString: String?,
    ) : Field(
        name = name,
        type = Type.COMPACT_STRING,
        docString = docString,
        hasDefaultValue = false,
        defaultValue = null
    )

    class NullableStr(
        name: String,
        docString: String?,
        defaultValue: String? = null,
    ) : Field(
        name = name,
        type = Type.NULLABLE_STRING,
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class CompactNullableStr(
        name: String,
        docString: String?,
    ) : Field(
        name = name,
        type = Type.COMPACT_NULLABLE_STRING,
        docString = docString,
        hasDefaultValue = false,
        defaultValue = null
    )

    class Array(
        name: String,
        elementType: Type,
        docString: String?,
        defaultValue: Any? = null,
    ) : Field(
        name = name,
        type = ArrayOf(elementType),
        docString = docString,
        hasDefaultValue = defaultValue != null,
        defaultValue = defaultValue
    )

    class NullableArray(
        name: String,
        elementType: Type,
        docString: String?,
        defaultValue: Any? = null,
    ) : Field(
        name = name,
        type = ArrayOf(elementType),
        docString = docString,
        hasDefaultValue = true,
        defaultValue = defaultValue
    )

    class CompactArray(
        name: String,
        elementType: Type,
        docString: String?,
    ) : Field(
        name = name,
        type = CompactArrayOf(elementType),
        docString = docString,
        hasDefaultValue = false,
        defaultValue = null
    )

    class TaggedFieldsSection(type: Type) : Field(
        name = NAME,
        type = type,
        docString = DOC_STRING,
        hasDefaultValue = false,
        defaultValue = null
    ) {

        companion object {

            private const val NAME = "_tagged_fields"

            private const val DOC_STRING = "The tagged fields"

            /**
             * Create a new TaggedFieldsSection with the given tags and fields.
             *
             * @param fields This is an array containing Integer tags followed by associated Field
             * objects.
             * @return The new [TaggedFieldsSection]
             */
            fun of(vararg fields: Any): TaggedFieldsSection =
                TaggedFieldsSection(TaggedFields.of(*fields))
        }
    }

    class ComplexArray(val name: String, val docString: String) {

        fun withFields(vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf(elementType),
                docString = docString,
                hasDefaultValue = false,
                defaultValue = null,
            )
        }

        fun nullableWithFields(vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf.nullable(elementType),
                docString = docString,
                hasDefaultValue = false,
                defaultValue = null
            )
        }

        fun withFields(docStringOverride: String?, vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf(elementType),
                docString = docStringOverride,
                hasDefaultValue = false,
                defaultValue = null
            )
        }
    }

    class ComplexNullableArray(val name: String, val docString: String) {

        fun withFields(vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf(elementType),
                docString = docString,
                hasDefaultValue = false,
                defaultValue = null,
            )
        }

        fun nullableWithFields(vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf.nullable(elementType),
                docString = docString,
                hasDefaultValue = true,
                defaultValue = null
            )
        }

        fun withFields(docStringOverride: String?, vararg fields: Field): Field {
            val elementType = Schema(*fields)
            return Field(
                name = name,
                type = ArrayOf(elementType),
                docString = docStringOverride,
                hasDefaultValue = false,
                defaultValue = null
            )
        }
    }
}
