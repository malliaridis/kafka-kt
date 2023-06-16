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
import org.apache.kafka.common.protocol.types.Type.DocumentedType
import org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint

/**
 * Represents a type for a compact array of a particular type. A compact array represents its length
 * with a varint rather than a fixed-length field.
 */
class CompactArrayOf private constructor(
    private val type: Type,
    override val isNullable: Boolean,
) : DocumentedType() {

    constructor(type: Type) : this(type, false)

    override fun write(buffer: ByteBuffer, o: Any?) {
        if (o == null) {
            writeUnsignedVarint(0, buffer)
            return
        }
        val objs = o as Array<Any>
        val size = objs.size
        writeUnsignedVarint(size + 1, buffer)
        for (obj in objs) type.write(buffer, obj)
    }

    override fun read(buffer: ByteBuffer): Any? {
        val n = readUnsignedVarint(buffer)
        if (n == 0) {
            return if (isNullable) null
            else throw SchemaException("This array is not nullable.")
        }
        val size = n - 1
        if (size > buffer.remaining()) throw SchemaException(
            "Error reading array of size $size, only ${buffer.remaining()} bytes available"
        )
        val objs = arrayOfNulls<Any>(size)
        for (i in 0 until size) objs[i] = type.read(buffer)
        return objs
    }

    override fun sizeOf(o: Any?): Int {
        if (o == null) return 1

        val objs = o as Array<Any>
        var size = sizeOfUnsignedVarint(objs.size + 1)
        for (obj in objs) size += type.sizeOf(obj)
        return size
    }

    override fun arrayElementType(): Type = type

    override fun toString(): String = "$COMPACT_ARRAY_TYPE_NAME($type)"

    override fun validate(item: Any?): Array<Any>? {
        return try {
            if (isNullable && item == null) return null
            val array = item as Array<Any>
            for (obj in array) type.validate(obj)
            array
        } catch (e: ClassCastException) {
            throw SchemaException("Not an Object[].")
        }
    }

    override fun typeName(): String = COMPACT_ARRAY_TYPE_NAME

    override fun documentation(): String {
        return "Represents a sequence of objects of a given type T. " +
                "Type T can be either a primitive type (e.g. $STRING) or a structure. " +
                "First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. " +
                "A null array is represented with a length of 0. " +
                "In protocol documentation an array of T instances is referred to as [T]."
    }

    companion object {

        private const val COMPACT_ARRAY_TYPE_NAME = "COMPACT_ARRAY"

        fun nullable(type: Type): CompactArrayOf = CompactArrayOf(type, true)
    }
}
