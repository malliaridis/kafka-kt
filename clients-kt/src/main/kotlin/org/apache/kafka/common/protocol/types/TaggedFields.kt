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
import java.util.Collections
import java.util.NavigableMap
import java.util.TreeMap
import org.apache.kafka.common.protocol.types.Type.DocumentedType
import org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint

/**
 * Represents a tagged fields section.
 */
class TaggedFields(private val fields: Map<Int, Field>) : DocumentedType() {

    override fun write(buffer: ByteBuffer, o: Any?) {
        val objects = o as NavigableMap<Int, Any>

        writeUnsignedVarint(objects.size, buffer)

        objects.forEach { (tag, value) ->
            val field = fields[tag]
            writeUnsignedVarint(tag, buffer)

            if (field == null) {
                val (_, data) = value as RawTaggedField
                writeUnsignedVarint(data.size, buffer)
                buffer.put(data)
            } else {
                writeUnsignedVarint(field.type.sizeOf(value), buffer)
                field.type.write(buffer, value)
            }
        }
    }

    override fun read(buffer: ByteBuffer): NavigableMap<Int, Any?>? {
        val numTaggedFields = readUnsignedVarint(buffer)
        if (numTaggedFields == 0) return Collections.emptyNavigableMap()

        val objects: NavigableMap<Int, Any?> = TreeMap()
        var prevTag = -1
        for (i in 0..<numTaggedFields) {
            val tag = readUnsignedVarint(buffer)
            if (tag <= prevTag) throw RuntimeException("Invalid or out-of-order tag $tag")

            prevTag = tag
            val size = readUnsignedVarint(buffer)
            if (size < 0) throw SchemaException("field size $size cannot be negative")
            if (size > buffer.remaining())
                throw SchemaException("Error reading field of size $size, only ${buffer.remaining()} bytes available")

            val field = fields[tag]
            if (field == null) {
                val bytes = ByteArray(size)
                buffer[bytes]
                objects[tag] = RawTaggedField(tag, bytes)
            } else objects[tag] = field.type.read(buffer)
        }
        return objects
    }

    override fun sizeOf(o: Any?): Int {
        var size = 0
        val objects = o as NavigableMap<Int, Any>
        size += sizeOfUnsignedVarint(objects.size)

        for ((tag, value1) in objects) {
            size += sizeOfUnsignedVarint(tag)
            val field = fields[tag]
            size += if (field == null) {
                val (_, data) = value1 as RawTaggedField
                data.size + sizeOfUnsignedVarint(
                    data.size
                )
            } else {
                val valueSize = field.type.sizeOf(value1)
                valueSize + sizeOfUnsignedVarint(valueSize)
            }
        }
        return size
    }

    override fun toString(): String {
        val bld = StringBuilder("TAGGED_FIELDS_TYPE_NAME(")
        var prefix = ""
        for ((key, value) in fields) {
            bld.append(prefix)
            prefix = ", "
            bld.append(key).append(" -> ").append(value.toString())
        }
        bld.append(")")
        return bld.toString()
    }

    override fun validate(item: Any?): Map<Int, Any>? {
        return try {
            val objects = item as NavigableMap<Int, Any>
            for ((tag, value) in objects)  {
                val field = fields[tag]
                if (field == null) {
                    if (value !is RawTaggedField) throw SchemaException(
                        "The value associated with tag $tag must be a RawTaggedField in this " +
                                "version of the software."
                    )
                } else field.type.validate(value)
            }
            objects
        } catch (e: ClassCastException) {
            throw SchemaException("Not a NavigableMap. Found class ${item!!.javaClass.name}")
        }
    }

    override fun typeName(): String = TAGGED_FIELDS_TYPE_NAME

    override fun documentation(): String = "Represents a series of tagged fields."

    /**
     * The number of tagged fields
     */
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numFields")
    )
    fun numFields(): Int = fields.size

    /**
     * The number of tagged fields
     */
    val numFields: Int
        get() = fields.size

    companion object {

        private const val TAGGED_FIELDS_TYPE_NAME = "TAGGED_FIELDS"

        /**
         * Create a new TaggedFields object with the given tags and fields.
         *
         * @param fields This is an array containing Integer tags followed
         * by associated Field objects.
         * @return The new [TaggedFields]
         */
        fun of(vararg fields: Any): TaggedFields {
            if (fields.size % 2 != 0)
                throw RuntimeException("TaggedFields#of takes an even number of parameters.")

            val newFields = TreeMap<Int, Field>()
            var i = 0
            while (i < fields.size) {
                val tag = fields[i] as Int
                val field = fields[i + 1] as Field
                newFields[tag] = field
                i += 2
            }
            return TaggedFields(newFields)
        }
    }
}
