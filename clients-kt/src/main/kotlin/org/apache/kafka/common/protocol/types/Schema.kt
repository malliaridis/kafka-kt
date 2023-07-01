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

/**
 * The schema for a compound record definition
 *
 * @constructor Construct the schema with a given list of its field values and the ability to
 * tolerate missing optional fields with defaults at the end of the schema definition.
 * @property tolerateMissingFieldsWithDefaults whether to accept records with missing optional
 * fields the end of the schema
 * @param fs the fields of this schema
 * @throws SchemaException If the given list have duplicate fields
 */
class Schema(
    private val tolerateMissingFieldsWithDefaults: Boolean,
    vararg fs: Field,
) : Type() {

    private val fields: Array<BoundField>

    private val fieldsByName: Map<String, BoundField>

    private val cachedStruct: Struct?

    /**
     * Construct the schema with a given list of its field values
     *
     * @param fs the fields of this schema
     *
     * @throws SchemaException If the given list have duplicate fields
     */
    constructor(vararg fs: Field) : this(false, *fs)

    init {
        fieldsByName = mutableMapOf()
        fields = fs.mapIndexed { i, def ->
            if (fieldsByName.containsKey(def.name))
                throw SchemaException("Schema contains a duplicate field: ${def.name}")

            val bound = BoundField(def, this, i)
            fieldsByName[def.name] = bound
            bound
        }.toTypedArray()

        // 6 schemas have no fields at the time of this writing (3 versions each of list_groups and
        // api_versions) for such schemas there's no point in even creating a unique Struct object
        // when deserializing.
        cachedStruct =
            if (fields.isNotEmpty()) null
            else Struct(this, NO_VALUES)
    }

    /**
     * Write a struct to the buffer
     */
    override fun write(buffer: ByteBuffer, o: Any?) {
        val r = o as Struct?
        for (field: BoundField in fields) {
            try {
                val value = field.def.type.validate(r!![(field)])
                field.def.type.write(buffer, value)
            } catch (e: Exception) {
                throw SchemaException(
                    "Error writing field '${field.def.name}': " + (e.message ?: e.javaClass.name)
                )
            }
        }
    }

    /**
     * Read a struct from the buffer. If this schema is configured to tolerate missing optional
     * fields at the end of the buffer, these fields are replaced with their default values;
     * otherwise, if the schema does not tolerate missing fields, or if missing fields don't have a
     * default value, a `SchemaException` is thrown to signify that mandatory fields are missing.
     */
    override fun read(buffer: ByteBuffer): Struct {
        if (cachedStruct != null) return cachedStruct

        val objects = fields.map { field ->
            try {
                if (tolerateMissingFieldsWithDefaults) {
                    if (buffer.hasRemaining()) field.def.type.read(buffer)
                    else if (field.def.hasDefaultValue) field.def.defaultValue
                    else throw SchemaException(
                        "Missing value for field '${field.def.name}' which has no default value."
                    )
                } else field.def.type.read(buffer)
            } catch (e: Exception) {
                throw SchemaException(
                    "Error reading field '${field.def.name}': " + (e.message ?: e.javaClass.name)
                )
            }
        }.toTypedArray()

        return Struct(this, objects)
    }

    /**
     * The size of the given record
     */
    override fun sizeOf(o: Any?): Int {
        var size = 0
        val r = o as Struct?
        for (field: BoundField in fields) {
            try {
                size += field.def.type.sizeOf(r!![(field)])
            } catch (e: Exception) {
                throw SchemaException(
                    "Error computing size for field '${field.def.name}': " +
                            (e.message ?: e.javaClass.name)
                )
            }
        }
        return size
    }

    /**
     * The number of fields in this schema
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numFields")
    )
    fun numFields(): Int = fields.size

    /**
     * The number of fields in this schema
     */
    val numFields: Int
        get() = fields.size

    /**
     * Get a field by its slot in the record array
     *
     * @param slot The slot at which this field sits
     * @return The field
     */
    operator fun get(slot: Int): BoundField = fields[slot]

    /**
     * Get a field by its name
     *
     * @param name The name of the field
     * @return The field
     */
    operator fun get(name: String): BoundField? = fieldsByName[name]

    /**
     * Get all the fields in this schema
     */
    fun fields(): Array<BoundField> = fields

    /**
     * Display a string representation of the schema
     */
    override fun toString(): String {
        val b = StringBuilder()
        b.append('{')
        b.append(fields.joinToString(","))
        b.append("}")
        return b.toString()
    }

    override fun validate(item: Any?): Struct {
        try {
            val struct = item as Struct
            fields.forEach { field: BoundField ->
                try {
                    field.def.type.validate(struct[(field)])
                } catch (e: SchemaException) {
                    throw SchemaException("Invalid value for field '${field.def.name}': ${e.message}")
                }
            }
            return struct
        } catch (e: ClassCastException) {
            throw SchemaException("Not a Struct.")
        }
    }

    fun walk(visitor: Visitor) = handleNode(this, visitor)

    /**
     * Override one or more of the visit methods with the desired logic.
     */
    abstract class Visitor {

        open fun visit(schema: Schema?) = Unit

        open fun visit(field: Type?) = Unit
    }

    companion object {

        private val NO_VALUES = arrayOfNulls<Any>(0)

        private fun handleNode(node: Type, visitor: Visitor) {
            if (node is Schema) {
                visitor.visit(node)
                for (f: BoundField in node.fields()) handleNode(f.def.type, visitor)
            } else if (node.isArray) {
                visitor.visit(node)
                handleNode(node.arrayElementType()!!, visitor)
            } else visitor.visit(node)
        }
    }
}
