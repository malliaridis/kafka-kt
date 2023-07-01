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
import org.apache.kafka.common.protocol.types.Field.Float64
import org.apache.kafka.common.protocol.types.Field.Int16
import org.apache.kafka.common.protocol.types.Field.Int32
import org.apache.kafka.common.protocol.types.Field.Int64
import org.apache.kafka.common.protocol.types.Field.Int8
import org.apache.kafka.common.protocol.types.Field.NullableStr
import org.apache.kafka.common.protocol.types.Field.Str
import org.apache.kafka.common.protocol.types.Field.Uint32
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
        values = arrayOfNulls(schema.numFields()),
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
    private fun getFieldOrDefault(field: BoundField): Any? {
        return values[field.index]
            ?: if (field.def.hasDefaultValue) field.def.defaultValue
            else if (field.def.type.isNullable) null
            else throw SchemaException(
                "Missing value for field '${field.def.name}' which has no default value."
            )
    }

    /**
     * Get the value for the field directly by the field index with no lookup needed (faster!)
     *
     * @param field The field to look up
     * @return The value for that field.
     * @throws SchemaException if the field has no value and has no default.
     */
    operator fun get(field: BoundField): Any? {
        validateField(field)
        return getFieldOrDefault(field)
    }

    operator fun get(field: Int8): Byte? = getByte(field.name)

    operator fun get(field: Int32): Int? = getInt(field.name)

    operator fun get(field: Int64): Long? = getLong(field.name)

    operator fun get(field: Field.UUID): Uuid? = getUuid(field.name)

    operator fun get(field: Field.Uint16): Int? = getInt(field.name)

    operator fun get(field: Uint32): Long? = getLong(field.name)

    operator fun get(field: Int16): Short? = getShort(field.name)

    operator fun get(field: Float64): Double? = getDouble(field.name)

    operator fun get(field: Str): String? = getString(field.name)

    operator fun get(field: NullableStr): String? = getString(field.name)

    operator fun get(field: Field.Bool): Boolean? = getBoolean(field.name)

    operator fun get(field: Field.Array): Array<Any?>? = getArray(field.name)

    operator fun get(field: ComplexArray): Array<Any?>? = getArray(field.name)

    fun getOrElse(field: Int64, alternative: Long): Long = getLong(field.name) ?: alternative

    fun getOrElse(field: Field.UUID, alternative: Uuid): Uuid = getUuid(field.name) ?: alternative

    fun getOrElse(field: Int16, alternative: Short): Short = getShort(field.name) ?: alternative

    fun getOrElse(field: Int8, alternative: Byte): Byte = getByte(field.name) ?: alternative

    fun getOrElse(field: Int32, alternative: Int): Int = getInt(field.name) ?: alternative

    fun getOrElse(field: Float64, alternative: Double): Double =
        getDouble(field.name) ?: alternative

    fun getOrElse(field: NullableStr, alternative: String): String =
        getString(field.name) ?: alternative

    fun getOrElse(field: Str, alternative: String): String = getString(field.name) ?: alternative

    fun getOrElse(field: Field.Bool, alternative: Boolean): Boolean =
        getBoolean(field.name) ?: alternative

    fun getOrEmpty(field: Field.Array): Array<Any?> = getArray(field.name) ?: emptyArray()

    fun getOrEmpty(field: ComplexArray): Array<Any?> = getArray(field.name) ?: emptyArray()

    /**
     * Get the record value for the field with the given name by doing a hash table lookup (slower!)
     *
     * @param name The name of the field
     * @return The value in the record
     * @throws SchemaException If no such field exists
     */
    operator fun get(name: String): Any? {
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

    fun getByte(field: BoundField): Byte? = get(field) as Byte?

    fun getByte(name: String): Byte? = get(name) as Byte?

    fun getRecords(name: String): BaseRecords? = get(name) as BaseRecords?

    fun getShort(field: BoundField): Short? = get(field) as Short?

    fun getShort(name: String): Short? = get(name) as Short?

    fun getUnsignedShort(field: BoundField): Int? = get(field) as Int?

    fun getUnsignedShort(name: String): Int? = get(name) as Int?

    fun getInt(field: BoundField): Int? = get(field) as Int?

    fun getInt(name: String): Int? = get(name) as Int?

    fun getUnsignedInt(name: String): Long? = get(name) as Long?

    fun getUnsignedInt(field: BoundField): Long? = get(field) as Long?

    fun getLong(field: BoundField): Long? = get(field) as Long?

    fun getLong(name: String): Long? = get(name) as Long?

    fun getUuid(field: BoundField): Uuid? = get(field) as Uuid?

    fun getUuid(name: String): Uuid? = get(name) as Uuid?

    fun getDouble(field: BoundField): Double? = get(field) as Double?

    fun getDouble(name: String): Double? = get(name) as Double?

    fun getArray(field: BoundField): Array<Any>? = get(field) as Array<Any>?

    fun getArray(name: String): Array<Any?>? = get(name) as Array<Any?>?

    fun getString(field: BoundField): String? = get(field) as String?

    fun getString(name: String): String? = get(name) as String?

    fun getBoolean(field: BoundField): Boolean? = get(field) as Boolean?

    fun getBoolean(name: String): Boolean? = get(name) as Boolean?

    fun getBytes(field: BoundField): ByteBuffer? {
        val result = get(field)
        return if (result is ByteArray) ByteBuffer.wrap(result as ByteArray?)
        else result as ByteBuffer?
    }

    fun getBytes(name: String): ByteBuffer? {
        val result = get(name)
        return if (result is ByteArray) ByteBuffer.wrap(result as ByteArray?)
        else result as ByteBuffer?
    }

    fun getByteArray(name: String): ByteArray {
        val result = get(name)
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
        val field = schema.get(name) ?: throw SchemaException("Unknown field: $name")
        values[field.index] = value
        return this
    }

    operator fun set(def: Str, value: String?): Struct = set(def.name, value)

    operator fun set(def: NullableStr, value: String?): Struct = set(def.name, value)

    operator fun set(def: Int8, value: Byte): Struct = set(def.name, value)

    operator fun set(def: Int32, value: Int): Struct = set(def.name, value)

    operator fun set(def: Int64, value: Long): Struct = set(def.name, value)

    operator fun set(def: Field.UUID, value: Uuid?): Struct = set(def.name, value)

    operator fun set(def: Int16, value: Short): Struct = set(def.name, value)

    operator fun set(def: Field.Uint16, value: Int): Struct {
        if (value < 0 || value > UNSIGNED_SHORT_MAX)
            throw RuntimeException("Invalid value for unsigned short for ${def.name}: $value")
        return set(def.name, value)
    }

    operator fun set(def: Uint32, value: Long): Struct {
        if (value < 0 || value > UNSIGNED_INT_MAX)
            throw RuntimeException("Invalid value for unsigned int for ${def.name}: $value")
        return set(def.name, value)
    }

    operator fun set(def: Float64, value: Double): Struct = set(def.name, value)

    operator fun set(def: Field.Bool, value: Boolean): Struct = set(def.name, value)

    operator fun set(def: Field.Array, value: Array<Any?>?): Struct = set(def.name, value)

    operator fun set(def: ComplexArray, value: Array<Any?>?): Struct = set(def.name, value)

    fun setByteArray(name: String, value: ByteArray?): Struct {
        val buf = if (value == null) null else ByteBuffer.wrap(value)
        return set(name, buf)
    }

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
            if (f.def.type.isArray) {
                if (this[f] != null) {
                    val arrayObject = this[f] as Array<Any>
                    for (arrayItem: Any in arrayObject)
                        result = prime * result + arrayItem.hashCode()
                }
            } else {
                val field = this[f]
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
            val result: Boolean = if (f.def.type.isArray)
                Arrays.equals(this[f] as Array<Any?>?, other[f] as Array<Any?>?)
            else {
                val thisField = this[f]
                val otherField = other[f]
                (thisField == otherField)
            }
            if (!result) return false
        }
        return true
    }
}
