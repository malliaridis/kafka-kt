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

import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.protocol.MessageUtil.UNSIGNED_INT_MAX
import org.apache.kafka.common.protocol.MessageUtil.UNSIGNED_SHORT_MAX
import org.apache.kafka.common.protocol.types.Field.ComplexArray
import org.apache.kafka.common.protocol.types.Field.ComplexNullableArray
import org.apache.kafka.common.protocol.types.Field.Float32
import org.apache.kafka.common.protocol.types.Field.Float64
import org.apache.kafka.common.protocol.types.Field.Int16
import org.apache.kafka.common.protocol.types.Field.Int32
import org.apache.kafka.common.protocol.types.Field.Int64
import org.apache.kafka.common.protocol.types.Field.Int8
import org.apache.kafka.common.protocol.types.Field.NullableFloat32
import org.apache.kafka.common.protocol.types.Field.NullableFloat64
import org.apache.kafka.common.protocol.types.Field.NullableInt16
import org.apache.kafka.common.protocol.types.Field.NullableInt32
import org.apache.kafka.common.protocol.types.Field.NullableInt64
import org.apache.kafka.common.protocol.types.Field.NullableInt8
import org.apache.kafka.common.protocol.types.Field.NullableStr
import org.apache.kafka.common.protocol.types.Field.NullableUUID
import org.apache.kafka.common.protocol.types.Field.NullableUint16
import org.apache.kafka.common.protocol.types.Field.NullableUint32
import org.apache.kafka.common.protocol.types.Field.NullableUint8
import org.apache.kafka.common.protocol.types.Field.Str
import org.apache.kafka.common.protocol.types.Field.UUID
import org.apache.kafka.common.protocol.types.Field.Uint16
import org.apache.kafka.common.protocol.types.Field.Uint32
import org.apache.kafka.common.protocol.types.Field.Uint8
import org.apache.kafka.common.record.BaseRecords

/**
 * A record that can be serialized and deserialized according to a pre-defined schema
 */
data class Struct(
    val schema: Schema,
    private val values: Array<Any?>,
) {

    constructor(schema: Schema) : this(
        schema = schema,
        values = arrayOfNulls(schema.numFields),
    )

    /**
     * The schema for this struct.
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("schema")
    )
    fun schema(): Schema = schema

    /**
     * Return the value of the given pre-validated field, or if the value is missing return the
     * default value.
     *
     * @param field The field for which to get the default value
     * @throws SchemaException if the field has no value and has no default.
     */
    private fun <T> getFieldOrDefault(field: BoundField): T {
        return (values[field.index]
            ?: if (field.def.hasDefaultValue) field.def.defaultValue
            else if (field.def.type.isNullable) null
            else throw SchemaException(
                "Missing value for field '${field.def.name}' which has no default value."
            )) as T
    }

    /**
     * Get the value for the field directly by the field index with no lookup needed (faster!)
     *
     * @param field The field to look up
     * @return The value for that field.
     * @throws SchemaException if the field has no value and has no default.
     */
    operator fun <T> get(field: BoundField): T {
        validateField(field)
        return getFieldOrDefault(field)
    }

    operator fun get(field: Field.Bool): Boolean = getBoolean(field.name)

    operator fun get(field: Field.NullableBool): Boolean? = getBooleanOrNull(field.name)

    operator fun get(field: Int8): Byte = getByte(field.name)

    operator fun get(field: NullableInt8): Byte? = getByteOrNull(field.name)

    operator fun get(field: Uint8): UByte = getUnsignedByte(field.name)

    operator fun get(field: NullableUint8): UByte? = getUnsignedByteOrNull(field.name)

    operator fun get(field: Int16): Short = getShort(field.name)

    operator fun get(field: NullableInt16): Short? = getShortOrNull(field.name)

    operator fun get(field: Uint16): UShort = getUnsignedShort(field.name)

    operator fun get(field: NullableUint16): UShort? = getUnsignedShortOrNull(field.name)

    operator fun get(field: Int32): Int = getInt(field.name)

    operator fun get(field: NullableInt32): Int? = getIntOrNull(field.name)

    operator fun get(field: Uint32): UInt = getUnsignedInt(field.name)

    operator fun get(field: NullableUint32): UInt? = getUnsignedIntOrNull(field.name)

    operator fun get(field: Int64): Long = getLong(field.name)

    operator fun get(field: NullableInt64): Long? = getLongOrNull(field.name)

    // TODO add UInt64
    // operator fun get(field: Uint64): ULong = getUnsignedLong(field.name)

    // TODO add nullable UInt64
    // operator fun get(field: NullableUint64): ULong? = getUnsignedLongOrNull(field.name)

    operator fun get(field: Float32): Float = getFloat(field.name)

    operator fun get(field: NullableFloat32): Float? = getFloatOrNull(field.name)

    operator fun get(field: Float64): Double = getDouble(field.name)

    operator fun get(field: NullableFloat64): Double? = getDoubleOrNull(field.name)

    operator fun get(field: UUID): Uuid = getUuid(field.name)

    operator fun get(field: NullableUUID): Uuid? = getUuidOrNull(field.name)

    operator fun get(field: Str): String = getString(field.name)

    operator fun get(field: NullableStr): String? = getStringOrNull(field.name)

    operator fun get(field: Field.Array): Array<Any?> = getArray(field.name)

    operator fun get(field: ComplexArray): Array<Any?> = getArray(field.name)

    operator fun get(field: Field.NullableArray): Array<Any?>? = getArrayOrNull(field.name)

    operator fun get(field: ComplexNullableArray): Array<Any?>? = getArrayOrNull(field.name)

    fun getOrElse(field: Int64, alternative: Long): Long = getLongOrNull(field.name) ?: alternative

    fun getOrElse(field: UUID, alternative: Uuid): Uuid = getUuidOrNull(field.name) ?: alternative

    fun getOrElse(field: Int16, alternative: Short): Short =
        getShortOrNull(field.name) ?: alternative

    fun getOrElse(field: Int8, alternative: Byte): Byte = getByteOrNull(field.name) ?: alternative

    fun getOrElse(field: Int32, alternative: Int): Int = getIntOrNull(field.name) ?: alternative

    fun getOrElse(field: Float64, alternative: Double): Double =
        getDoubleOrNull(field.name) ?: alternative

    fun getOrElse(field: NullableStr, alternative: String): String =
        getStringOrNull(field.name) ?: alternative

    fun getOrElse(field: Str, alternative: String): String = getString(field.name) ?: alternative

    fun getOrElse(field: Field.Bool, alternative: Boolean): Boolean =
        getBooleanOrNull(field.name) ?: alternative

    fun getOrEmpty(field: Field.Array): Array<Any?> = getArray(field.name) ?: emptyArray()

    fun getOrEmpty(field: ComplexArray): Array<Any?> = getArray(field.name) ?: emptyArray()

    /**
     * Get the record value for the field with the given name by doing a hash table lookup (slower!)
     *
     * @param name The name of the field
     * @return The value in the record
     * @throws SchemaException If no such field exists
     */
    operator fun <T> get(name: String): T {
        val field = schema[name] ?: throw SchemaException("No such field: $name")
        return getFieldOrDefault(field)
    }

    /**
     * Check if the struct contains a field.
     *
     * @param name
     * @return Whether a field exists.
     */
    fun hasField(name: String): Boolean = schema[name] != null

    fun hasField(def: Field): Boolean = schema[def.name] != null

    fun hasField(def: ComplexArray): Boolean = schema[def.name] != null

    fun getStruct(field: BoundField): Struct? = get(field) as Struct?

    fun getStruct(name: String): Struct? = get(name) as Struct?

    fun getByte(field: BoundField): Byte = get(field)

    fun getByte(name: String): Byte = get(name)

    fun getByteOrNull(field: BoundField): Byte? = get(field)

    fun getByteOrNull(name: String): Byte? = get(name)

    fun getUnsignedByte(field: BoundField): UByte = get(field)

    fun getUnsignedByte(name: String): UByte = get(name)

    fun getUnsignedByteOrNull(field: BoundField): UByte? = get(field)

    fun getUnsignedByteOrNull(name: String): UByte? = get(name)

    fun getRecords(name: String): BaseRecords? = get(name) as BaseRecords?

    fun getShort(field: BoundField): Short = get(field) as Short

    fun getShort(name: String): Short = get(name) as Short

    fun getShortOrNull(field: BoundField): Short? = get(field)

    fun getShortOrNull(name: String): Short? = get(name)

    fun getUnsignedShort(field: BoundField): UShort = get(field)

    fun getUnsignedShort(name: String): UShort = get(name)

    fun getUnsignedShortOrNull(name: String): UShort? = get(name)

    fun getUnsignedShortOrNull(field: BoundField): UShort? = get(field)

    fun getInt(field: BoundField): Int = get(field) as Int

    fun getInt(name: String): Int = get(name) as Int

    fun getIntOrNull(field: BoundField): Int? = get(field)

    fun getIntOrNull(name: String): Int? = get(name)

    fun getUnsignedInt(name: String): UInt = get(name)

    fun getUnsignedIntOrNull(name: String): UInt? = get(name)

    fun getUnsignedInt(field: BoundField): Long? = get(field)

    // TODO Add unsigned nullable int

    fun getLong(field: BoundField): Long = get(field)

    fun getLong(name: String): Long = get(name)

    fun getLongOrNull(field: BoundField): Long? = get(field)

    fun getLongOrNull(name: String): Long? = get(name)

    fun getUuid(field: BoundField): Uuid = get(field)

    fun getUuid(name: String): Uuid = get(name)

    fun getUuidOrNull(field: BoundField): Uuid? = get(field)

    fun getUuidOrNull(name: String): Uuid? = get(name)

    fun getFloat(field: BoundField): Float = get(field)

    fun getFloat(name: String): Float = get(name)

    fun getFloatOrNull(field: BoundField): Float? = get(field)

    fun getFloatOrNull(name: String): Float? = get(name)

    fun getDouble(field: BoundField): Double = get(field)

    fun getDouble(name: String): Double = get(name)

    fun getDoubleOrNull(field: BoundField): Double? = get(field)

    fun getDoubleOrNull(name: String): Double? = get(name)

    fun getArray(field: BoundField): Array<Any?> = get(field)

    fun getArray(name: String): Array<Any?> = get(name)

    fun getArrayOrNull(field: BoundField): Array<Any?>? = get(field)

    fun getArrayOrNull(name: String): Array<Any?>? = get(name)

    fun getString(field: BoundField): String = get(field)

    fun getString(name: String): String = get(name)

    fun getStringOrNull(field: BoundField): String = get(field)

    fun getStringOrNull(name: String): String? = get(name)

    fun getBoolean(field: BoundField): Boolean = get(field)

    fun getBoolean(name: String): Boolean = get(name)

    fun getBooleanOrNull(field: BoundField): Boolean? = get(field)

    fun getBooleanOrNull(name: String): Boolean? = get(name)

    fun getBytes(field: BoundField): ByteBuffer? {
        val result: Any? = get(field)
        return if (result is ByteArray) ByteBuffer.wrap(result as ByteArray?)
        else result as ByteBuffer?
    }

    fun getBytes(name: String): ByteBuffer? {
        val result: Any? = get(name)
        return if (result is ByteArray) ByteBuffer.wrap(result as ByteArray?)
        else result as ByteBuffer?
    }

    fun getByteArray(name: String): ByteArray {
        val result: Any? = get(name)
        if (result is ByteArray) return result
        val buf = result as ByteBuffer?
        val arr = ByteArray(buf!!.remaining())
        buf[arr]
        buf.flip()
        return arr
    }

    /**
     * Set the given field to the specified value
     *
     * @param field The field
     * @param value The value
     * @throws SchemaException If the validation of the field failed
     */
    operator fun set(field: BoundField, value: Any?): Struct {
        validateField(field)
        values[field.index] = value
        return this
    }

    /**
     * Set the field specified by the given name to the value
     *
     * @param name The name of the field
     * @param value The value to set
     * @throws SchemaException If the field is not known
     */
    operator fun set(name: String, value: Any?): Struct {
        val field = schema[name] ?: throw SchemaException("Unknown field: $name")
        values[field.index] = value
        return this
    }

    operator fun set(def: Field.Bool, value: Boolean): Struct = set(def.name, value)

    operator fun set(def: Field.NullableBool, value: Boolean?): Struct = set(def.name, value)

    operator fun set(def: Int8, value: Byte): Struct = set(def.name, value)

    operator fun set(def: NullableInt8, value: Byte?): Struct = set(def.name, value)

    operator fun set(def: Uint8, value: UByte): Struct = set(def.name, value)

    operator fun set(def: NullableUint8, value: UByte?): Struct = set(def.name, value)

    operator fun set(def: Int16, value: Short): Struct = set(def.name, value)

    operator fun set(def: NullableInt16, value: Short?): Struct = set(def.name, value)

    operator fun set(def: Uint16, value: Uint16): Struct = set(def.name, value)

    operator fun set(def: NullableUint16, value: Uint16?): Struct = set(def.name, value)

    operator fun set(def: Int32, value: Int): Struct = set(def.name, value)

    operator fun set(def: NullableInt32, value: Int?): Struct = set(def.name, value)

    operator fun set(def: Uint32, value: UInt): Struct = set(def.name, value)

    operator fun set(def: NullableUint32, value: UInt?): Struct = set(def.name, value)

    operator fun set(def: Int64, value: Long): Struct = set(def.name, value)

    operator fun set(def: NullableInt64, value: Long?): Struct = set(def.name, value)

    operator fun set(def: UUID, value: Uuid): Struct = set(def.name, value)

    operator fun set(def: NullableUUID, value: Uuid?): Struct = set(def.name, value)

    operator fun set(def: Float32, value: Float): Struct = set(def.name, value)

    operator fun set(def: NullableFloat32, value: Float?): Struct = set(def.name, value)

    operator fun set(def: Float64, value: Double): Struct = set(def.name, value)

    operator fun set(def: NullableFloat64, value: Double?): Struct = set(def.name, value)

    operator fun set(def: Str, value: String): Struct = set(def.name, value)

    operator fun set(def: NullableStr, value: String?): Struct = set(def.name, value)

    operator fun set(def: Field.Array, value: Array<Any?>): Struct = set(def.name, value)

    operator fun set(def: Field.NullableArray, value: Array<Any?>?): Struct = set(def.name, value)

    operator fun set(def: ComplexArray, value: Array<Any?>?): Struct = set(def.name, value)

    operator fun set(def: ComplexNullableArray, value: Array<Any?>?): Struct = set(def.name, value)

    fun setByteArray(name: String, value: ByteArray?): Struct {
        val buf = if (value == null) null else ByteBuffer.wrap(value)
        return set(name, buf)
    }

    // TODO Consider adding setShortArray, setIntArray, setLongArray, setFloatArray and setDoubleArray

    fun setIfExists(def: Field.Array, value: Array<Any?>?): Struct = setIfExists(def.name, value)

    fun setIfExists(def: ComplexArray, value: Array<Any?>?): Struct = setIfExists(def.name, value)

    fun setIfExists(def: Field, value: Any?): Struct = setIfExists(def.name, value)

    fun setIfExists(fieldName: String, value: Any?): Struct {
        val field = schema[fieldName]
        if (field != null) values[field.index] = value
        return this
    }

    /**
     * Create a struct for the schema of a container type (struct or array). Note that for array
     * type, this method assumes that the type is an array of schema and creates a struct of that
     * schema. Arrays of other types can't be instantiated with this method.
     *
     * @param field The field to create an instance of
     * @return The struct
     * @throws SchemaException If the given field is not a container type
     */
    fun instance(field: BoundField): Struct {
        validateField(field)

        return if (field.def.type is Schema) Struct(field.def.type)
        else if (field.def.type.isArray) Struct(field.def.type.arrayElementType() as Schema)
        else throw SchemaException(
            "Field '${field.def.name}' is not a container type, it is of type ${field.def.type}"
        )
    }

    /**
     * Create a struct instance for the given field which must be a container type (struct or array)
     *
     * @param field The name of the field to create (field must be a schema type)
     * @return The struct
     * @throws SchemaException If the given field is not a container type
     */
    fun instance(field: String): Struct = instance(schema[field]!!)

    fun instance(field: Field): Struct = instance(schema[field.name]!!)

    fun instance(field: ComplexArray): Struct = instance(schema[field.name]!!)

    /**
     * Empty all the values from this record
     */
    fun clear() {
        Arrays.fill(values, null)
    }

    /**
     * Get the serialized size of this object
     */
    fun sizeOf(): Int = schema.sizeOf(this)

    /**
     * Write this struct to a buffer
     */
    fun writeTo(buffer: ByteBuffer) = schema.write(buffer, this)

    /**
     * Ensure the user doesn't try to access fields from the wrong schema
     *
     * @throws SchemaException If validation fails
     */
    private fun validateField(field: BoundField) {
        if (schema !== field.schema) throw SchemaException(
            "Attempt to access field '${field.def.name}' from a different schema instance."
        )
        if (field.index > values.size) throw SchemaException("Invalid field index: ${field.index}")
    }

    /**
     * Validate the contents of this struct against its schema
     *
     * @throws SchemaException If validation fails
     */
    fun validate() = schema.validate(this)

    override fun toString(): String {
        val b = StringBuilder()
        b.append('{')
        values.forEachIndexed { index, value ->
            val f = schema[index]
            b.append(f.def.name)
            b.append('=')
            if (f.def.type.isArray && value != null) {
                val arrayValue = value as Array<Any>
                b.append('[')
                b.append(arrayValue.joinToString(","))
                b.append(']')
            } else b.append(value)
            if (index < values.size - 1) b.append(',')
        }
        b.append('}')
        return b.toString()
    }

    override fun hashCode(): Int {
        val prime = 31
        var result = 1
        for (index in values.indices) {
            val f = schema[index]
            val field: Any? = this[f]
            if (f.def.type.isArray) {
                if (field != null) {
                    val arrayObject = field as Array<Any?>
                    for (arrayItem in arrayObject)
                        result = prime * result + arrayItem.hashCode()
                }
            } else {
                if (field != null) result = prime * result + field.hashCode()
            }
        }
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (javaClass != other.javaClass) return false

        other as Struct
        if (schema !== other.schema) return false
        for (i in values.indices) {
            val f = schema[i]
            val thisField: Any? = this[f]
            val otherField: Any? = other[f]
            val result = if (f.def.type.isArray)
                (thisField as Array<Any?>?) contentEquals (otherField as Array<Any?>?)
            else thisField == otherField

            if (!result) return false
        }
        return true
    }
}
