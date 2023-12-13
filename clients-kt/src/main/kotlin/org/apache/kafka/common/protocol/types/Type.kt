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
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.ByteUtils.readDouble
import org.apache.kafka.common.utils.ByteUtils.readFloat
import org.apache.kafka.common.utils.ByteUtils.readUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.readVarint
import org.apache.kafka.common.utils.ByteUtils.readVarlong
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.sizeOfVarint
import org.apache.kafka.common.utils.ByteUtils.sizeOfVarlong
import org.apache.kafka.common.utils.ByteUtils.writeDouble
import org.apache.kafka.common.utils.ByteUtils.writeFloat
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedInt
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarint
import org.apache.kafka.common.utils.ByteUtils.writeVarlong
import org.apache.kafka.common.utils.Utils.utf8
import org.apache.kafka.common.utils.Utils.utf8Length

/**
 * A serializable type
 */
abstract class Type {
    
    /**
     * Write the typed object to the buffer
     *
     * @throws SchemaException If the object is not valid for its type
     */
    abstract fun write(buffer: ByteBuffer, o: Any?)

    /**
     * Read the typed object from the buffer
     * Please remember to do size validation before creating the container (ex: array) for the following data
     *
     * @throws SchemaException If the object is not valid for its type
     */
    abstract fun read(buffer: ByteBuffer): Any?

    /**
     * Validate the object. If succeeded return its typed object.
     *
     * @throws SchemaException If validation failed
     */
    abstract fun validate(item: Any?): Any?

    /**
     * Return the size of the object in bytes
     */
    abstract fun sizeOf(o: Any?): Int

    /**
     * Check if the type supports null values
     * @return whether null is a valid value for the type implementation
     */
    open val isNullable: Boolean = false

    /**
     * If the type is an array, return the type of the array elements. Otherwise, return empty.
     */
    open fun arrayElementType(): Type? = null

    /**
     * Returns true if the type is an array.
     */
    val isArray: Boolean
        get() = arrayElementType() != null

    /**
     * A Type that can return its description for documentation purposes.
     */
    abstract class DocumentedType : Type() {
        
        /**
         * Short name of the type to identify it in documentation;
         * @return the name of the type
         */
        abstract fun typeName(): String

        /**
         * Documentation of the Type.
         *
         * @return details about valid values, representation
         */
        abstract fun documentation(): String?
        
        override fun toString(): String = typeName()
    }

    companion object {

        private const val NULLABILITY_BYTE_SIZE = 1
        private const val IS_NULL = 0.toByte()
        private const val IS_NOT_NULL = 1.toByte()

        private const val BOOLEAN_SIZE = 1
        private const val INT8_SIZE = 1
        private const val INT16_SIZE = 2
        private const val INT32_SIZE = 4
        private const val INT64_SIZE = 8
        private const val FLOAT32_SIZE = 4
        private const val FLOAT64_SIZE = 8
        private const val UUID_SIZE = 16
        
        /**
         * The Boolean type represents a boolean value in a byte by using the value of 0 to
         * represent false, and 1 to represent true.
         *
         * If for some reason a value that is not 0 or 1 is read, then any non-zero value will
         * return true.
         */
        val BOOLEAN: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o as Boolean) buffer.put(1.toByte())
                else buffer.put(0.toByte())
            }

            override fun read(buffer: ByteBuffer): Boolean = buffer.get() != 0.toByte()

            override fun sizeOf(o: Any?): Int = BOOLEAN_SIZE

            override fun typeName(): String = "BOOLEAN"

            override fun validate(item: Any?): Boolean {
                return if (item is Boolean) item
                else throw SchemaException("$item is not a Boolean.")
            }

            override fun documentation(): String {
                return "Represents a boolean value in a byte. " +
                        "Values 0 and 1 are used to represent false and true respectively. " +
                        "When reading a boolean value, any non-zero value is considered true."
            }
        }

        /**
         * The nullable Boolean type represents a boolean value in a byte by using the value of 0 to
         * represent false, and 1 to represent true. Any other value is considered null.
         *
         * If a value that is not 0 or 1 is read, then null is returned.
         */
        val NULLABLE_BOOLEAN: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                when (o) {
                    is Boolean -> buffer.put(if (o) 1 else 0)
                    0x0 -> buffer.put(0)
                    0xF -> buffer.put(1)
                    else -> buffer.put(0x1) // put non-zero, neither 0 (0x0), nor 1 (0xF)
                }
            }

            override fun read(buffer: ByteBuffer): Boolean? {
                val nullable = buffer.get()
                if (nullable == 0x0.toByte()) return false
                if (nullable == 0xf.toByte()) return true
                return null
            }

            override fun sizeOf(o: Any?): Int = BOOLEAN_SIZE

            override fun typeName(): String = "NULLABLE_BOOLEAN"

            override fun validate(item: Any?): Boolean? {
                return when(item) {
                    null -> null
                    is Boolean -> item
                    else -> throw SchemaException("$item is not a nullable Boolean.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable boolean value in a byte. Values 0 and 1 are used " +
                        "to represent false and true respectively. When reading a boolean value, " +
                        "any non-zero value is considered null."
            }
        }

        val INT8: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.put(o as Byte)
            }

            override fun read(buffer: ByteBuffer): Byte = buffer.get()

            override fun sizeOf(o: Any?): Int = 1

            override fun typeName(): String = "INT8"

            override fun validate(item: Any?): Byte {
                return if (item is Byte) item
                else throw SchemaException("$item is not a Byte.")
            }

            override fun documentation(): String {
                return "Represents an integer between -2<sup>7</sup> and 2<sup>7</sup>-1 inclusive."
            }
        }

        val NULLABLE_INT8: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.put(o as Byte)
            }

            override fun read(buffer: ByteBuffer): Byte? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.get()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT8_SIZE
            }

            override fun typeName(): String = "NULLABLE_INT8"

            override fun validate(item: Any?): Byte? {
                return when(item) {
                    null -> null
                    is Byte -> item
                    else -> throw SchemaException("$item is not a nullable Byte.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable integer between -2<sup>7</sup> and " +
                        "2<sup>7</sup-1 inclusive. For non-null integer, first the nullability " +
                        "byte is given as an $INT8 of $IS_NOT_NULL. A null value is encoded with " +
                        "a nullability byte of $IS_NULL and there are no following bytes."
            }
        }

        val UINT8: DocumentedType = object : DocumentedType() {

            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.put((o as Short).toByte())
            }

            override fun read(buffer: ByteBuffer): UByte {
                return buffer.getShort().toUByte()
            }

            override fun sizeOf(o: Any?): Int = INT8_SIZE

            override fun typeName(): String = "UINT8"

            override fun validate(item: Any?): Short {
                return if (item is Short) item
                else throw SchemaException("$item is not a Short (encoding an unsigned byte)")
            }

            override fun documentation(): String {
                return "Represents an integer between 0 and 255 inclusive. The values are " +
                        "encoded using one byte."
            }
        }

        val NULLABLE_UINT8: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.put((o as Short).toByte())
            }

            override fun read(buffer: ByteBuffer): UByte? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getShort().toUByte()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT8_SIZE
            }

            override fun typeName(): String = "NULLABLE_UINT8"

            override fun validate(item: Any?): Short? {
                return when(item) {
                    null -> null
                    is Short -> item
                    else -> throw SchemaException("$item is not a Short (encoding an unsigned byte)")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable unsigned integer between 0 and 255 inclusive. For " +
                        "non-null integer, first the nullability byte is given as an $INT8 of " +
                        "$IS_NOT_NULL. Then the values are encoded using one byte. A null value " +
                        "is encoded with a nullability byte of $IS_NULL and there are no " +
                        "following bytes."
            }
        }

        val INT16: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.putShort(o as Short)
            }

            override fun read(buffer: ByteBuffer): Short = buffer.getShort()

            override fun sizeOf(o: Any?): Int = 2

            override fun typeName(): String = "INT16"

            override fun validate(item: Any?): Short {
                return if (item is Short) item
                else throw SchemaException("$item is not a Short.")
            }

            override fun documentation(): String {
                return "Represents an integer between -2<sup>15</sup> and 2<sup>15</sup>-1 inclusive. " +
                        "The values are encoded using two bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_INT16: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.putShort(o as Short)
            }

            override fun read(buffer: ByteBuffer): Short? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getShort()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT16_SIZE
            }

            override fun typeName(): String = "NULLABLE_INT16"

            override fun validate(item: Any?): Short? {
                return when(item) {
                    null -> null
                    is Short -> item
                    else -> throw SchemaException("$item is not a nullable Short.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable integer between -2<sup>15</sup> and " +
                        "2<sup>15</sup-1 inclusive. For non-null integer, first the nullability " +
                        "byte is given as an $INT8 of $IS_NOT_NULL. Then the values are encoded " +
                        "using two bytes in network byte order (big-endian). A null value is " +
                        "encoded with a nullability byte of $IS_NULL and there are no following " +
                        "bytes."
            }
        }

        val UINT16: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.putShort((o as Int).toShort())
            }

            override fun read(buffer: ByteBuffer): UShort {
                return buffer.getInt().toUShort()
            }

            override fun sizeOf(o: Any?): Int = INT16_SIZE

            override fun typeName(): String = "UINT16"

            override fun validate(item: Any?): Int {
                return if (item is Int) item
                else throw SchemaException("$item is not an a Integer (encoding an unsigned short)")
            }

            override fun documentation(): String {
                return "Represents an integer between 0 and 65535 inclusive. " +
                        "The values are encoded using two bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_UINT16: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.putShort((o as Int).toShort())
            }

            override fun read(buffer: ByteBuffer): UShort? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getInt().toUShort()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT16_SIZE
            }

            override fun typeName(): String = "NULLABLE_UINT16"

            override fun validate(item: Any?): Int? {
                return when(item) {
                    null -> null
                    is Int -> item
                    else -> throw SchemaException("$item is not an Int (encoding an unsigned short)")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable unsigned integer between 0 and 65535 inclusive. " +
                        "For non-null integer, first the nullability byte is given as an $INT8 " +
                        "of $IS_NOT_NULL. Then the values are encoded using two bytes in network " +
                        "byte order (big-endian). A null value is encoded with a nullability " +
                        "byte of $IS_NULL and there are no following bytes."
            }
        }

        val INT32: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.putInt(o as Int)
            }

            override fun read(buffer: ByteBuffer): Int = buffer.getInt()

            override fun sizeOf(o: Any?): Int = 4

            override fun typeName(): String = "INT32"

            override fun validate(item: Any?): Int {
                return if (item is Int) item
                else throw SchemaException("$item is not an Integer.")
            }

            override fun documentation(): String {
                return "Represents an integer between -2<sup>31</sup> and 2<sup>31</sup>-1 inclusive. " +
                        "The values are encoded using four bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_INT32: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.putInt(o as Int)
            }

            override fun read(buffer: ByteBuffer): Int? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getInt()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT32_SIZE
            }

            override fun typeName(): String = "NULLABLE_INT32"

            override fun validate(item: Any?): Int? {
                return when(item) {
                    null -> null
                    is Int -> item
                    else -> throw SchemaException("$item is not a nullable Integer.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable integer between -2<sup>31</sup> and " +
                        "2<sup>31</sup-1 inclusive. For non-null integer, first the nullability " +
                        "byte is given as an $INT8 of $IS_NOT_NULL. Then the values are encoded " +
                        "using four bytes in network byte order (big-endian). A null value is " +
                        "encoded with a nullability byte of $IS_NULL and there are no following " +
                        "bytes."
            }
        }

        val UNSIGNED_INT32: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                writeUnsignedInt(buffer, o as UInt)
            }

            override fun read(buffer: ByteBuffer): UInt {
                return buffer.getLong().toUInt()
            }

            override fun sizeOf(o: Any?): Int = 4

            override fun typeName(): String = "UINT32"

            override fun validate(item: Any?): Long {
                return if (item is Long) item
                else throw SchemaException("$item is not an a Long (encoding an unsigned integer).")
            }

            override fun documentation(): String {
                return "Represents an integer between 0 and 2<sup>32</sup>-1 inclusive. " +
                        "The values are encoded using four bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_UINT32: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.putInt((o as Long).toInt())
            }

            override fun read(buffer: ByteBuffer): UInt? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getLong().toUInt()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT32_SIZE
            }

            override fun typeName(): String = "NULLABLE_UINT32"

            override fun validate(item: Any?): Long? {
                return when(item) {
                    null -> null
                    is Long -> item
                    else -> throw SchemaException("$item is not an Long (encoding an unsigned integer)")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable unsigned integer between 0 and 4294967295 inclusive. " +
                        "For non-null integer, first the nullability byte is given as an $INT8 " +
                        "of $IS_NOT_NULL. Then the values are encoded using four bytes in network " +
                        "byte order (big-endian). A null value is encoded with a nullability " +
                        "byte of $IS_NULL and there are no following bytes."
            }
        }
        
        val INT64: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                buffer.putLong(o as Long)
            }

            override fun read(buffer: ByteBuffer): Any = buffer.getLong()

            override fun sizeOf(o: Any?): Int = 8

            override fun typeName(): String = "INT64"

            override fun validate(item: Any?): Long {
                return if (item is Long) item
                else throw SchemaException("$item is not a Long.")
            }

            override fun documentation(): String {
                return "Represents an integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. " +
                        "The values are encoded using eight bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_INT64: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                buffer.putLong(o as Long)
            }

            override fun read(buffer: ByteBuffer): Long? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return buffer.getLong()
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + INT64_SIZE
            }

            override fun typeName(): String = "NULLABLE_INT64"

            override fun validate(item: Any?): Long? {
                return when(item) {
                    null -> null
                    is Long -> item
                    else -> throw SchemaException("$item is not a nullable Long.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable integer between -2<sup>64</sup> and " +
                        "2<sup>63</sup-1 inclusive. For non-null integer, first the nullability " +
                        "byte is given as an $INT8 of $IS_NOT_NULL. Then the values are encoded " +
                        "using eight bytes in network byte order (big-endian). A null value is " +
                        "encoded with a nullability byte of $IS_NULL and there are no following " +
                        "bytes."
            }
        }

        val UUID: DocumentedType = object : DocumentedType() {

            override fun write(buffer: ByteBuffer, o: Any?) {
                val (mostSignificantBits, leastSignificantBits) = o as Uuid
                buffer.putLong(mostSignificantBits)
                buffer.putLong(leastSignificantBits)
            }

            override fun read(buffer: ByteBuffer): Any = Uuid(buffer.getLong(), buffer.getLong())

            override fun sizeOf(o: Any?): Int = 16

            override fun typeName(): String = "UUID"

            override fun validate(item: Any?): Uuid {
                return if (item is Uuid) item
                else throw SchemaException("$item is not a Uuid.")
            }

            override fun documentation(): String {
                return "Represents a type 4 immutable universally unique identifier (Uuid). " +
                        "The values are encoded using sixteen bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_UUID: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                val (mostSignificantBits, leastSignificantBits) = o as Uuid
                buffer.putLong(mostSignificantBits)
                buffer.putLong(leastSignificantBits)
            }

            override fun read(buffer: ByteBuffer): Uuid? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return Uuid(buffer.getLong(), buffer.getLong())
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + UUID_SIZE
            }

            override fun typeName(): String = "NULLABLE_UUID"

            override fun validate(item: Any?): Uuid? {
                return when (item) {
                    null -> null
                    is Uuid -> item
                    else -> throw SchemaException("$item is not a nullable Uuid.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable type 4 immutable universally unique identifier " +
                        "(Uuid). For non-null Uuid, first the nullability byte is given as an " +
                        "$INT8 of $IS_NOT_NULL. Then the values are encoded using sixteen bytes " +
                        "in network byte order (big-endian). A null value is encoded with a " +
                        "nullability byte of $IS_NULL and there are no following bytes."
            }
        }

        val FLOAT32: DocumentedType = object : DocumentedType() {

            override fun write(buffer: ByteBuffer, o: Any?) {
                writeFloat(o as Float, buffer)
            }

            override fun read(buffer: ByteBuffer): Any = readDouble(buffer)

            override fun sizeOf(o: Any?): Int = FLOAT32_SIZE

            override fun typeName(): String = "FLOAT32"

            override fun validate(item: Any?): Float {
                return if (item is Float) item
                else throw SchemaException("$item is not a Float.")
            }

            override fun documentation(): String {
                return "Represents a single-precision 32-bit format IEEE 754 value. " +
                        "The values are encoded using eight bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_FLOAT32: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                writeFloat(o as Float, buffer)
            }

            override fun read(buffer: ByteBuffer): Float? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return readFloat(buffer)
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + FLOAT32_SIZE
            }

            override fun typeName(): String = "NULLABLE_FLOAT32"

            override fun validate(item: Any?): Float? {
                return when(item) {
                    null -> null
                    is Float -> item
                    else -> throw SchemaException("$item is not a nullable Float.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable single-precision 32-bit format IEEE 754 value. " +
                        "For non-null integer, first the nullability byte is given as an $INT8 " +
                        "of $IS_NOT_NULL. Then the values are encoded using four bytes in " +
                        "network byte order (big-endian).A null value is encoded with a " +
                        "nullability byte of $IS_NULL and there are no following bytes."
            }
        }

        val FLOAT64: DocumentedType = object : DocumentedType() {

            override fun write(buffer: ByteBuffer, o: Any?) {
                writeDouble(o as Double, buffer)
            }

            override fun read(buffer: ByteBuffer): Any = readDouble(buffer)

            override fun sizeOf(o: Any?): Int = FLOAT64_SIZE

            override fun typeName(): String = "FLOAT64"

            override fun validate(item: Any?): Double {
                return if (item is Double) item
                else throw SchemaException("$item is not a Double.")
            }

            override fun documentation(): String {
                return "Represents a double-precision 64-bit format IEEE 754 value. " +
                        "The values are encoded using eight bytes in network byte order (big-endian)."
            }
        }

        val NULLABLE_FLOAT64: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.put(IS_NULL)
                    return
                }
                buffer.put(IS_NOT_NULL)
                writeDouble(o as Double, buffer)
            }

            override fun read(buffer: ByteBuffer): Double? {
                val nullable = buffer.get()
                if (nullable == IS_NULL) return null

                return readDouble(buffer)
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) NULLABILITY_BYTE_SIZE
                else NULLABILITY_BYTE_SIZE + FLOAT64_SIZE
            }

            override fun typeName(): String = "NULLABLE_FLOAT64"

            override fun validate(item: Any?): Double? {
                return when(item) {
                    null -> null
                    is Double -> item
                    else -> throw SchemaException("$item is not a nullable Double.")
                }
            }

            override fun documentation(): String {
                return "Represents a nullable double-precision 64-bit format IEEE 754 value. " +
                        "For non-null integer, first the nullability byte is given as an $INT8 " +
                        "of $IS_NOT_NULL. Then the values are encoded using eight bytes in " +
                        "network byte order (big-endian). A null value is encoded with a " +
                        "nullability byte of $IS_NULL and there are no following bytes."
            }
        }

        val STRING: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                val bytes = (o as String).toByteArray()
                
                if (bytes.size > Short.MAX_VALUE) throw SchemaException(
                    "String length ${bytes.size} is larger than the maximum string length."
                )
                
                buffer.putShort(bytes.size.toShort())
                buffer.put(bytes)
            }

            override fun read(buffer: ByteBuffer): String {
                val length = buffer.getShort()
                
                if (length < 0) throw SchemaException("String length $length cannot be negative")
                if (length > buffer.remaining()) throw SchemaException(
                    "Error reading string of length $length, only ${buffer.remaining()} bytes available"
                )
                
                val result = utf8(buffer, length.toInt())
                buffer.position(buffer.position() + length)
                return result
            }

            override fun sizeOf(o: Any?): Int {
                return 2 + utf8Length(o as String)
            }

            override fun typeName(): String = "STRING"

            override fun validate(item: Any?): String {
                return if (item is String) item
                else throw SchemaException("$item is not a String.")
            }

            override fun documentation(): String {
                return "Represents a sequence of characters. First the length N is given as an $INT16. " +
                        "Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
                        "Length must not be negative."
            }
        }
        
        val COMPACT_STRING: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                val bytes = (o as String).toByteArray()
                
                if (bytes.size > Short.MAX_VALUE) throw SchemaException(
                    "String length ${bytes.size} is larger than the maximum string length."
                )
                
                writeUnsignedVarint(bytes.size + 1, buffer)
                buffer.put(bytes)
            }

            override fun read(buffer: ByteBuffer): String {
                val length = readUnsignedVarint(buffer) - 1
                
                if (length < 0) throw SchemaException("String length $length cannot be negative")
                if (length > Short.MAX_VALUE) throw SchemaException(
                    "String length $length is larger than the maximum string length."
                )
                if (length > buffer.remaining()) throw SchemaException(
                    "Error reading string of length $length, only ${buffer.remaining()} bytes available"
                )
                
                val result = utf8(buffer, length)
                buffer.position(buffer.position() + length)
                return result
            }

            override fun sizeOf(o: Any?): Int {
                val length = utf8Length(o as String)
                return sizeOfUnsignedVarint(length + 1) + length
            }

            override fun typeName(): String = "COMPACT_STRING"

            override fun validate(item: Any?): String {
                return if (item is String) item
                else throw SchemaException("$item is not a String.")
            }

            override fun documentation(): String {
                return "Represents a sequence of characters. First the length N + 1 is given as " +
                        "an UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding " +
                        "of the character sequence."
            }
        }
        
        val NULLABLE_STRING: DocumentedType = object : DocumentedType() {
            
            override val isNullable: Boolean= true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.putShort((-1).toShort())
                    return
                }
                val bytes = (o as String).toByteArray()
                
                if (bytes.size > Short.MAX_VALUE) throw SchemaException(
                    "String length ${bytes.size} is larger than the maximum string length."
                )
                
                buffer.putShort(bytes.size.toShort())
                buffer.put(bytes)
            }

            override fun read(buffer: ByteBuffer): String? {
                val length = buffer.getShort()
                if (length < 0) return null
                
                if (length > buffer.remaining()) throw SchemaException(
                    "Error reading string of length $length, only ${buffer.remaining()} bytes available"
                )
                
                val result = utf8(buffer, length.toInt())
                buffer.position(buffer.position() + length)
                return result
            }

            override fun sizeOf(o: Any?): Int {
                return if (o == null) 2
                else 2 + utf8Length(o as String)
            }

            override fun typeName(): String = "NULLABLE_STRING"

            override fun validate(item: Any?): String? {
                return when (item) {
                    null -> null
                    is String -> item
                    else -> throw SchemaException("$item is not a String.")
                }
            }

            override fun documentation(): String {
                return "Represents a sequence of characters or null. For non-null strings, first " +
                        "the length N is given as an $INT16. Then N bytes follow which are the " +
                        "UTF-8 encoding of the character sequence. A null value is encoded with " +
                        "length of -1 and there are no following bytes."
            }
        }
        
        val COMPACT_NULLABLE_STRING: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) writeUnsignedVarint(0, buffer)
                else {
                    val bytes = (o as String).toByteArray()
                    if (bytes.size > Short.MAX_VALUE) throw SchemaException(
                        "String length ${bytes.size} is larger than the maximum string length."
                    )
                    writeUnsignedVarint(bytes.size + 1, buffer)
                    buffer.put(bytes)
                }
            }

            override fun read(buffer: ByteBuffer): String? {
                val length = readUnsignedVarint(buffer) - 1
                return if (length < 0) null
                else if (length > Short.MAX_VALUE) throw SchemaException(
                    "String length $length is larger than the maximum string length."
                ) else if (length > buffer.remaining()) throw SchemaException(
                    "Error reading string of length $length, only ${buffer.remaining()} bytes available"
                ) else {
                    val result = utf8(buffer, length)
                    buffer.position(buffer.position() + length)
                    result
                }
            }

            override fun sizeOf(o: Any?): Int {
                if (o == null) return 1
                
                val length = utf8Length(o as String)
                return sizeOfUnsignedVarint(length + 1) + length
            }

            override fun typeName(): String = "COMPACT_NULLABLE_STRING"

            override fun validate(item: Any?): String? {
                return when (item) {
                    null -> null
                    is String -> item
                    else -> throw SchemaException("$item is not a String.")
                }
            }

            override fun documentation(): String {
                return "Represents a sequence of characters. First the length N + 1 is given as an " +
                        "UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding of the " +
                        "character sequence. A null string is represented with a length of 0."
            }
        }
        
        val BYTES: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                val arg = o as ByteBuffer
                val pos = arg.position()
                buffer.putInt(arg.remaining())
                buffer.put(arg)
                arg.position(pos)
            }

            override fun read(buffer: ByteBuffer): Any {
                val size = buffer.getInt()
                
                if (size < 0) throw SchemaException("Bytes size $size cannot be negative")
                if (size > buffer.remaining()) throw SchemaException(
                    "Error reading bytes of size $size, only ${buffer.remaining()} bytes available"
                )

                val limit = buffer.limit()
                val newPosition = buffer.position() + size
                buffer.limit(newPosition)
                val value = buffer.slice()
                buffer.limit(limit)
                buffer.position(newPosition)
                return value
            }

            override fun sizeOf(o: Any?): Int {
                return 4 + (o as ByteBuffer).remaining()
            }

            override fun typeName(): String = "BYTES"

            override fun validate(item: Any?): ByteBuffer {
                return if (item is ByteBuffer) item
                else throw SchemaException("$item is not a java.nio.ByteBuffer.")
            }

            override fun documentation(): String {
                return "Represents a raw sequence of bytes. First the length N is given as an " +
                        "$INT32. Then N bytes follow."
            }
        }
        
        val COMPACT_BYTES: DocumentedType = object : DocumentedType() {
            
            override fun write(buffer: ByteBuffer, o: Any?) {
                val arg = o as ByteBuffer
                val pos = arg.position()
                writeUnsignedVarint(arg.remaining() + 1, buffer)
                buffer.put(arg)
                arg.position(pos)
            }

            override fun read(buffer: ByteBuffer): Any {
                val size = readUnsignedVarint(buffer) - 1
                
                if (size < 0) throw SchemaException("Bytes size $size cannot be negative")
                if (size > buffer.remaining()) throw SchemaException(
                    "Error reading bytes of size $size, only ${buffer.remaining()} bytes available"
                )

                val limit = buffer.limit()
                val newPosition = buffer.position() + size
                buffer.limit(newPosition)
                val value = buffer.slice()
                buffer.limit(limit)
                buffer.position(newPosition)
                return value
            }

            override fun sizeOf(o: Any?): Int {
                val buffer = o as ByteBuffer
                val remaining = buffer.remaining()
                return sizeOfUnsignedVarint(remaining + 1) + remaining
            }

            override fun typeName(): String = "COMPACT_BYTES"

            override fun validate(item: Any?): ByteBuffer {
                return if (item is ByteBuffer) item
                else throw SchemaException("$item is not a java.nio.ByteBuffer.")
            }

            override fun documentation(): String {
                return "Represents a raw sequence of bytes. First the length N+1 is given as an " +
                        "UNSIGNED_VARINT.Then N bytes follow."
            }
        }
        
        val NULLABLE_BYTES: DocumentedType = object : DocumentedType() {
            
            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) {
                    buffer.putInt(-1)
                    return
                }
                val arg = o as ByteBuffer
                val pos = arg.position()
                buffer.putInt(arg.remaining())
                buffer.put(arg)
                arg.position(pos)
            }

            override fun read(buffer: ByteBuffer): Any? {
                val size = buffer.getInt()
                if (size < 0) return null
                
                if (size > buffer.remaining()) throw SchemaException(
                    "Error reading bytes of size $size, only ${buffer.remaining()} bytes available"
                )

                val limit = buffer.limit()
                val newPosition = buffer.position() + size
                buffer.limit(newPosition)
                val value = buffer.slice()
                buffer.limit(limit)
                buffer.position(newPosition)
                return value
            }

            override fun sizeOf(o: Any?): Int {
                if (o == null) return 4
                return 4 + (o as ByteBuffer).remaining()
            }

            override fun typeName(): String = "NULLABLE_BYTES"

            override fun validate(item: Any?): ByteBuffer? {
                return when (item) {
                    null -> null
                    is ByteBuffer -> item
                    else -> throw SchemaException("$item is not a java.nio.ByteBuffer.")
                }
            }

            override fun documentation(): String {
                return "Represents a raw sequence of bytes or null. For non-null values, first " +
                        "the length N is given as an $INT32. Then N bytes follow. A null value " +
                        "is encoded with length of -1 and there are no following bytes."
            }
        }
        
        val COMPACT_NULLABLE_BYTES: DocumentedType = object : DocumentedType() {
            
            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                if (o == null) writeUnsignedVarint(0, buffer)
                else {
                    val arg = o as ByteBuffer
                    val pos = arg.position()
                    writeUnsignedVarint(arg.remaining() + 1, buffer)
                    buffer.put(arg)
                    arg.position(pos)
                }
            }

            override fun read(buffer: ByteBuffer): Any? {
                val size = readUnsignedVarint(buffer) - 1
                if (size < 0) return null
                if (size > buffer.remaining()) throw SchemaException(
                    "Error reading bytes of size $size, only ${buffer.remaining()} bytes available"
                )

                val limit = buffer.limit()
                val newPosition = buffer.position() + size
                buffer.limit(newPosition)
                val value = buffer.slice()
                buffer.limit(limit)
                buffer.position(newPosition)
                return value
            }

            override fun sizeOf(o: Any?): Int {
                if (o == null) return 1
                
                val buffer = o as ByteBuffer
                val remaining = buffer.remaining()
                return sizeOfUnsignedVarint(remaining + 1) + remaining
            }

            override fun typeName(): String = "COMPACT_NULLABLE_BYTES"

            override fun validate(item: Any?): ByteBuffer? {
                return when (item) {
                    null -> null
                    is ByteBuffer -> item
                    else -> throw SchemaException("$item is not a java.nio.ByteBuffer.")
                }
            }

            override fun documentation(): String {
                return "Represents a raw sequence of bytes. First the length N+1 is given as an " +
                        "UNSIGNED_VARINT.Then N bytes follow. A null object is represented with " +
                        "a length of 0."
            }
        }
        
        val COMPACT_RECORDS: DocumentedType = object : DocumentedType() {
            
            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                when (o) {
                    null -> COMPACT_NULLABLE_BYTES.write(buffer, null)
                    is MemoryRecords -> COMPACT_NULLABLE_BYTES.write(buffer, o.buffer().duplicate())
                    else -> throw IllegalArgumentException("Unexpected record type: " + o.javaClass)
                }
            }

            override fun read(buffer: ByteBuffer): MemoryRecords? {
                val recordsBuffer = COMPACT_NULLABLE_BYTES.read(buffer) as ByteBuffer?
                return if (recordsBuffer == null) null
                else MemoryRecords.readableRecords(recordsBuffer)
            }

            override fun sizeOf(o: Any?): Int {
                if (o == null) return 1
                val records = o as BaseRecords
                val recordsSize = records.sizeInBytes()
                return sizeOfUnsignedVarint(recordsSize + 1) + recordsSize
            }

            override fun typeName(): String = "COMPACT_RECORDS"

            override fun validate(item: Any?): BaseRecords? {
                return when (item) {
                    null -> null
                    is BaseRecords -> item
                    else -> throw SchemaException(
                        "$item is not an instance of ${BaseRecords::class.java.name}"
                    )
                }
            }

            override fun documentation(): String {
                return "Represents a sequence of Kafka records as $COMPACT_NULLABLE_BYTES. " +
                        "For a detailed description of records see " +
                        "<a href=\"/documentation/#messageformat\">Message Sets</a>."
            }
        }

        val RECORDS: DocumentedType = object : DocumentedType() {

            override val isNullable: Boolean = true

            override fun write(buffer: ByteBuffer, o: Any?) {
                when (o) {
                    null -> NULLABLE_BYTES.write(buffer, null)
                    is MemoryRecords -> NULLABLE_BYTES.write(buffer, o.buffer().duplicate())
                    else -> throw IllegalArgumentException("Unexpected record type: " + o.javaClass)
                }
            }

            override fun read(buffer: ByteBuffer): MemoryRecords? {
                val recordsBuffer = NULLABLE_BYTES.read(buffer) as ByteBuffer?
                return if (recordsBuffer == null) {
                    null
                } else {
                    MemoryRecords.readableRecords(recordsBuffer)
                }
            }

            override fun sizeOf(o: Any?): Int {
                if (o == null) return 4
                return 4 + (o as BaseRecords).sizeInBytes()
            }

            override fun typeName(): String = "RECORDS"

            override fun validate(item: Any?): BaseRecords? {
                return when (item) {
                    null -> null
                    is BaseRecords -> item
                    else -> throw SchemaException(
                        "$item is not an instance of ${BaseRecords::class.java.name}"
                    )
                }
            }

            override fun documentation(): String {
                return "Represents a sequence of Kafka records as $NULLABLE_BYTES. " +
                        "For a detailed description of records see " +
                        "<a href=\"/documentation/#messageformat\">Message Sets</a>."
            }
        }

        val VARINT: DocumentedType = object : DocumentedType() {

            override fun write(buffer: ByteBuffer, o: Any?) {
                writeVarint(o as Int, buffer)
            }

            override fun read(buffer: ByteBuffer): Int = readVarint(buffer)

            override fun validate(item: Any?): Int {
                if (item is Int) return item
                throw SchemaException("$item is not an integer")
            }

            override fun typeName(): String = "VARINT"

            override fun sizeOf(o: Any?): Int = sizeOfVarint(o as Int)

            override fun documentation(): String {
                return "Represents an integer between -2<sup>31</sup> and 2<sup>31</sup>-1 inclusive. " +
                        "Encoding follows the variable-length zig-zag encoding from " +
                        " <a href=\"http://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>."
            }
        }

        val VARLONG: DocumentedType = object : DocumentedType() {
            override fun write(buffer: ByteBuffer, o: Any?) {
                writeVarlong(o as Long, buffer)
            }

            override fun read(buffer: ByteBuffer): Long = readVarlong(buffer)

            override fun validate(item: Any?): Long {
                if (item is Long) return item
                throw SchemaException("$item is not a long")
            }

            override fun typeName(): String = "VARLONG"

            override fun sizeOf(o: Any?): Int = sizeOfVarlong(o as Long)

            override fun documentation(): String {
                return "Represents an integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. " +
                        "Encoding follows the variable-length zig-zag encoding from " +
                        " <a href=\"http://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>."
            }
        }

        private fun toHtml(): String {
            val types = arrayOf(
                BOOLEAN,
                NULLABLE_BOOLEAN,
                INT8,
                NULLABLE_INT8,
                INT16,
                NULLABLE_INT16,
                INT32,
                NULLABLE_INT32,
                UNSIGNED_INT32,
                INT64,
                NULLABLE_INT64,
                VARINT,
                VARLONG,
                UUID,
                FLOAT32,
                FLOAT64,
                STRING,
                COMPACT_STRING,
                NULLABLE_STRING,
                COMPACT_NULLABLE_STRING,
                BYTES,
                COMPACT_BYTES,
                NULLABLE_BYTES,
                COMPACT_NULLABLE_BYTES,
                RECORDS,
                ArrayOf(STRING),
                CompactArrayOf(COMPACT_STRING),
            )
            val b = StringBuilder()
            b.append("<table class=\"data-table\"><tbody>\n")
            b.append("<tr>")
            b.append("<th>Type</th>\n")
            b.append("<th>Description</th>\n")
            b.append("</tr>\n")
            for (type in types) {
                b.append("<tr>")
                b.append("<td>")
                b.append(type.typeName())
                b.append("</td>")
                b.append("<td>")
                b.append(type.documentation())
                b.append("</td>")
                b.append("</tr>\n")
            }
            b.append("</tbody></table>\n")
            return b.toString()
        }

        @JvmStatic
        fun main(args: Array<String>) {
            println(toHtml())
        }
    }
}
