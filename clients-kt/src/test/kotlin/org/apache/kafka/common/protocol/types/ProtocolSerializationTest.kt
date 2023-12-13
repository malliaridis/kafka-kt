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
import org.apache.kafka.common.utils.ByteUtils.sizeOfUnsignedVarint
import org.apache.kafka.common.utils.ByteUtils.writeUnsignedVarint
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProtocolSerializationTest {
    
    private lateinit var schema: Schema
    
    private lateinit var struct: Struct
    
    @BeforeEach
    fun setup() {
        schema = Schema(
            Field(name = "boolean", type = Type.BOOLEAN),
            Field(name = "int8", type = Type.INT8),
            Field(name = "int16", type = Type.INT16),
            Field(name = "int32", type = Type.INT32),
            Field(name = "int64", type = Type.INT64),
            Field(name = "varint", type = Type.VARINT),
            Field(name = "varlong", type = Type.VARLONG),
            Field(name = "float64", type = Type.FLOAT64),
            Field(name = "string", type = Type.STRING),
            Field(name = "compact_string", type = Type.COMPACT_STRING),
            Field(name = "nullable_string", type = Type.NULLABLE_STRING),
            Field(name = "compact_nullable_string", type = Type.COMPACT_NULLABLE_STRING),
            Field(name = "bytes", type = Type.BYTES),
            Field(name = "compact_bytes", type = Type.COMPACT_BYTES),
            Field(name = "nullable_bytes", type = Type.NULLABLE_BYTES),
            Field(name = "compact_nullable_bytes", type = Type.COMPACT_NULLABLE_BYTES),
            Field(name = "array", type = ArrayOf(Type.INT32)),
            Field(name = "compact_array", type = CompactArrayOf(Type.INT32)),
            Field(name = "null_array", type = ArrayOf.nullable(Type.INT32)),
            Field(name = "compact_null_array", type = CompactArrayOf.nullable(Type.INT32)),
            Field(name = "struct", type = Schema(Field("field", ArrayOf(Type.INT32))))
        )
        struct = Struct(schema).set("boolean", true)
            .set("int8", 1.toByte())
            .set("int16", 1.toShort())
            .set("int32", 1)
            .set("int64", 1L)
            .set("varint", 300)
            .set("varlong", 500L)
            .set("float64", 0.5)
            .set("string", "1")
            .set("compact_string", "1")
            .set("nullable_string", null)
            .set("compact_nullable_string", null)
            .set("bytes", ByteBuffer.wrap("1".toByteArray()))
            .set("compact_bytes", ByteBuffer.wrap("1".toByteArray()))
            .set("nullable_bytes", null)
            .set("compact_nullable_bytes", null)
            .set("array", arrayOf(1))
            .set("compact_array", arrayOf(1))
            .set("null_array", null)
            .set("compact_null_array", null)
        struct["struct"] = struct.instance("struct").set("field", arrayOf(1, 2, 3))
    }

    @Test
    fun testSimple() {
        check(Type.BOOLEAN, false, "BOOLEAN")
        check(Type.BOOLEAN, true, "BOOLEAN")
        check(Type.INT8, (-111).toByte(), "INT8")
        check(Type.INT16, (-11111).toShort(), "INT16")
        check(Type.INT32, -11111111, "INT32")
        check(Type.INT64, -11111111111L, "INT64")
        check(Type.FLOAT64, 2.5, "FLOAT64")
        check(Type.FLOAT64, -0.5, "FLOAT64")
        check(Type.FLOAT64, 1e300, "FLOAT64")
        check(Type.FLOAT64, 0.0, "FLOAT64")
        check(Type.FLOAT64, -0.0, "FLOAT64")
        check(Type.FLOAT64, Double.MAX_VALUE, "FLOAT64")
        check(Type.FLOAT64, Double.MIN_VALUE, "FLOAT64")
        check(Type.FLOAT64, Double.NaN, "FLOAT64")
        check(Type.FLOAT64, Double.NEGATIVE_INFINITY, "FLOAT64")
        check(Type.FLOAT64, Double.POSITIVE_INFINITY, "FLOAT64")
        check(Type.STRING, "", "STRING")
        check(Type.STRING, "hello", "STRING")
        check(Type.STRING, "A\u00ea\u00f1\u00fcC", "STRING")
        check(Type.COMPACT_STRING, "", "COMPACT_STRING")
        check(Type.COMPACT_STRING, "hello", "COMPACT_STRING")
        check(Type.COMPACT_STRING, "A\u00ea\u00f1\u00fcC", "COMPACT_STRING")
        check(Type.NULLABLE_STRING, null, "NULLABLE_STRING")
        check(Type.NULLABLE_STRING, "", "NULLABLE_STRING")
        check(Type.NULLABLE_STRING, "hello", "NULLABLE_STRING")
        check(Type.COMPACT_NULLABLE_STRING, null, "COMPACT_NULLABLE_STRING")
        check(Type.COMPACT_NULLABLE_STRING, "", "COMPACT_NULLABLE_STRING")
        check(Type.COMPACT_NULLABLE_STRING, "hello", "COMPACT_NULLABLE_STRING")
        check(Type.BYTES, ByteBuffer.allocate(0), "BYTES")
        check(Type.BYTES, ByteBuffer.wrap("abcd".toByteArray()), "BYTES")
        check(Type.COMPACT_BYTES, ByteBuffer.allocate(0), "COMPACT_BYTES")
        check(Type.COMPACT_BYTES, ByteBuffer.wrap("abcd".toByteArray()), "COMPACT_BYTES")
        check(Type.NULLABLE_BYTES, null, "NULLABLE_BYTES")
        check(Type.NULLABLE_BYTES, ByteBuffer.allocate(0), "NULLABLE_BYTES")
        check(Type.NULLABLE_BYTES, ByteBuffer.wrap("abcd".toByteArray()), "NULLABLE_BYTES")
        check(Type.COMPACT_NULLABLE_BYTES, null, "COMPACT_NULLABLE_BYTES")
        check(Type.COMPACT_NULLABLE_BYTES, ByteBuffer.allocate(0), "COMPACT_NULLABLE_BYTES")
        check(
            Type.COMPACT_NULLABLE_BYTES,
            ByteBuffer.wrap("abcd".toByteArray()),
            "COMPACT_NULLABLE_BYTES",
        )
        check(Type.VARINT, Int.MAX_VALUE, "VARINT")
        check(Type.VARINT, Int.MIN_VALUE, "VARINT")
        check(Type.VARLONG, Long.MAX_VALUE, "VARLONG")
        check(Type.VARLONG, Long.MIN_VALUE, "VARLONG")
        check(ArrayOf(Type.INT32), arrayOf(1, 2, 3, 4), "ARRAY(INT32)")
        check(ArrayOf(Type.STRING), arrayOf<String>(), "ARRAY(STRING)")
        check(
            ArrayOf(Type.STRING),
            arrayOf("hello", "there", "beautiful"),
            "ARRAY(STRING)",
        )
        check(
            CompactArrayOf(Type.INT32),
            arrayOf(1, 2, 3, 4),
            "COMPACT_ARRAY(INT32)",
        )
        check(
            CompactArrayOf(Type.COMPACT_STRING),
            arrayOf<String>(),
            "COMPACT_ARRAY(COMPACT_STRING)",
        )
        check(
            CompactArrayOf(Type.COMPACT_STRING),
            arrayOf("hello", "there", "beautiful"),
            "COMPACT_ARRAY(COMPACT_STRING)",
        )
        check(ArrayOf.nullable(Type.STRING), null, "ARRAY(STRING)")
        check(CompactArrayOf.nullable(Type.COMPACT_STRING), null, "COMPACT_ARRAY(COMPACT_STRING)")
    }

    @Test
    fun testNulls() {
        for (f in schema.fields()) {
            val o = struct.get<Any>(f)
            try {
                struct[f] = null
                struct.validate()
                assertTrue(f.def.type.isNullable, "Should not allow serialization of null value.")
            } catch (_: SchemaException) {
                assertFalse(f.def.type.isNullable, "$f should not be nullable")
            } finally {
                struct[f] = o
            }
        }
    }

    @Test
    fun testDefault() {
        val schema = Schema(Field(name = "field", type = Type.INT32, docString = "doc", defaultValue = 42))
        val struct = Struct(schema)
        assertEquals(42, struct["field"], "Should get the default value")
        struct.validate() // should be valid even with missing value
    }

    @Test
    fun testNullableDefault() {
        checkNullableDefault(Type.NULLABLE_BYTES, ByteBuffer.allocate(0))
        checkNullableDefault(Type.COMPACT_NULLABLE_BYTES, ByteBuffer.allocate(0))
        checkNullableDefault(Type.NULLABLE_STRING, "default")
        checkNullableDefault(Type.COMPACT_NULLABLE_STRING, "default")
    }

    private fun checkNullableDefault(type: Type, defaultValue: Any) {
        // Should use default even if the field allows null values
        val schema = Schema(Field("field", type, "doc", defaultValue))
        val struct = Struct(schema)
        assertEquals(defaultValue, struct["field"], "Should get the default value")
        struct.validate() // should be valid even with missing value
    }

    @Test
    fun testReadArraySizeTooLarge() {
        val type: Type = ArrayOf(Type.INT8)
        val size = 10
        val invalidBuffer = ByteBuffer.allocate(4 + size)
        invalidBuffer.putInt(Int.MAX_VALUE)
        for (i in 0 until size) invalidBuffer.put(i.toByte())
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Array size not validated") {
            type.read(invalidBuffer)
        }
    }

    @Test
    fun testReadCompactArraySizeTooLarge() {
        val type: Type = CompactArrayOf(Type.INT8)
        val size = 10
        val invalidBuffer = ByteBuffer.allocate(sizeOfUnsignedVarint(Int.MAX_VALUE) + size)
        writeUnsignedVarint(Int.MAX_VALUE, invalidBuffer)
        for (i in 0 until size) invalidBuffer.put(i.toByte())
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Array size not validated") {
            type.read(invalidBuffer)
        }
    }

    @Test
    fun testReadTaggedFieldsSizeTooLarge() {
        val tag = 1
        val type: Type = TaggedFields.of(tag, Field("field", Type.NULLABLE_STRING))
        val size = 10
        val buffer = ByteBuffer.allocate(size)
        val numTaggedFields = 1
        // write the number of tagged fields
        writeUnsignedVarint(numTaggedFields, buffer)
        // write the tag of the first tagged fields
        writeUnsignedVarint(tag, buffer)
        // write the size of tagged fields for this tag, using a large number for testing
        writeUnsignedVarint(Int.MAX_VALUE, buffer)
        val expectedRemaining = buffer.remaining()
        buffer.rewind()

        // should throw SchemaException while reading the buffer, instead of OOM
        val exception = assertFailsWith<SchemaException> { type.read(buffer) }
        assertEquals(
            expected = "Error reading field of size ${Int.MAX_VALUE}, only $expectedRemaining bytes available",
            actual = exception.message,
        )
    }

    @Test
    fun testReadNegativeArraySize() {
        val type: Type = ArrayOf(Type.INT8)
        val size = 10
        val invalidBuffer = ByteBuffer.allocate(4 + size)
        invalidBuffer.putInt(-1)
        for (i in 0 until size) invalidBuffer.put(i.toByte())
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Array size not validated") {
            type.read(invalidBuffer)
        }
    }

    @Test
    fun testReadZeroCompactArraySize() {
        val type: Type = CompactArrayOf(Type.INT8)
        val size = 10
        val invalidBuffer = ByteBuffer.allocate(sizeOfUnsignedVarint(0) + size)
        writeUnsignedVarint(0, invalidBuffer)
        for (i in 0 until size) invalidBuffer.put(i.toByte())
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Array size not validated") {
            type.read(invalidBuffer)
        }
    }

    @Test
    fun testReadStringSizeTooLarge() {
        val stringBytes = "foo".toByteArray()
        val invalidBuffer = ByteBuffer.allocate(2 + stringBytes.size)
        invalidBuffer.putShort((stringBytes.size * 5).toShort())
        invalidBuffer.put(stringBytes)
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("String size not validated") {
            Type.STRING.read(invalidBuffer)
        }
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("String size not validated") {
            Type.NULLABLE_STRING.read(invalidBuffer)
        }
    }

    @Test
    fun testReadNegativeStringSize() {
        val stringBytes = "foo".toByteArray()
        val invalidBuffer = ByteBuffer.allocate(2 + stringBytes.size)
        invalidBuffer.putShort((-1).toShort())
        invalidBuffer.put(stringBytes)
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("String size not validated") {
            Type.STRING.read(invalidBuffer)
        }
    }

    @Test
    fun testReadBytesSizeTooLarge() {
        val stringBytes = "foo".toByteArray()
        val invalidBuffer = ByteBuffer.allocate(4 + stringBytes.size)
        invalidBuffer.putInt(stringBytes.size * 5)
        invalidBuffer.put(stringBytes)
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Bytes size not validated") {
            Type.BYTES.read(invalidBuffer)
        }
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Bytes size not validated") {
            Type.NULLABLE_BYTES.read(invalidBuffer)
        }
    }

    @Test
    fun testReadNegativeBytesSize() {
        val stringBytes = "foo".toByteArray()
        val invalidBuffer = ByteBuffer.allocate(4 + stringBytes.size)
        invalidBuffer.putInt(-20)
        invalidBuffer.put(stringBytes)
        invalidBuffer.rewind()
        assertFailsWith<SchemaException>("Bytes size not validated") {
            Type.BYTES.read(invalidBuffer)
        }
    }

    @Test
    fun testToString() {
        val structStr = struct.toString()
        assertNotNull(structStr, "Struct string should not be null.")
        assertFalse(structStr.isEmpty(), "Struct string should not be empty.")
    }

    private fun roundtrip(type: Type, obj: Any?): Any? {
        val buffer = ByteBuffer.allocate(type.sizeOf(obj))
        type.write(buffer, obj)
        assertFalse(buffer.hasRemaining(), "The buffer should now be full.")
        buffer.rewind()
        val read = type.read(buffer)
        assertFalse(buffer.hasRemaining(), "All bytes should have been read.")
        return read
    }

    private fun check(type: Type, obj: Any?, expectedTypeName: String) {
        var obj = obj
        var result = roundtrip(type, obj)
        if (obj is Array<*>) {
            obj = obj.toList()
            result = (result as Array<*>?)?.toList()
        }
        assertEquals(expectedTypeName, type.toString())
        assertEquals(obj, result, "The object read back should be the same as what was written.")
    }

    @Test
    fun testStructEquals() {
        val schema = Schema(
            Field(name = "field1", type = Type.NULLABLE_STRING),
            Field(name = "field2", type = Type.NULLABLE_STRING),
        )
        val emptyStruct1 = Struct(schema)
        val emptyStruct2 = Struct(schema)
        assertEquals(emptyStruct1, emptyStruct2)
        val mostlyEmptyStruct = Struct(schema).set("field1", "foo")
        assertNotEquals(emptyStruct1, mostlyEmptyStruct)
        assertNotEquals(mostlyEmptyStruct, emptyStruct1)
    }

    @Test
    fun testReadIgnoringExtraDataAtTheEnd() {
        val oldSchema = Schema(
            Field("field1", Type.NULLABLE_STRING),
            Field("field2", Type.NULLABLE_STRING),
        )
        val newSchema = Schema(Field("field1", Type.NULLABLE_STRING))
        val value = "foo bar baz"
        val oldFormat = Struct(oldSchema).set("field1", value).set("field2", "fine to ignore")
        val buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat))
        oldFormat.writeTo(buffer)
        buffer.flip()
        val newFormat = newSchema.read(buffer)
        assertEquals(value, newFormat["field1"])
    }

    @Test
    fun testReadWhenOptionalDataMissingAtTheEndIsTolerated() {
        val oldSchema = Schema(Field("field1", Type.NULLABLE_STRING))
        val newSchema = Schema(
            true,
            Field(name = "field1", type = Type.NULLABLE_STRING),
            Field(
                name = "field2",
                type = Type.NULLABLE_STRING,
                docString = "",
                hasDefaultValue = true,
                defaultValue = "default",
            ),
            Field(
                name = "field3",
                type = Type.NULLABLE_STRING,
                docString = "",
                hasDefaultValue = true,
                defaultValue = null,
            ),
            Field(
                name = "field4",
                type = Type.NULLABLE_BYTES,
                docString = "",
                hasDefaultValue = true,
                defaultValue = ByteBuffer.allocate(0),
            ),
            Field(
                name = "field5",
                type = Type.INT64,
                docString = "doc",
                hasDefaultValue = true,
                defaultValue = Long.MAX_VALUE,
            ),
        )
        val value = "foo bar baz"
        val oldFormat = Struct(oldSchema).set("field1", value)
        val buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat))
        oldFormat.writeTo(buffer)
        buffer.flip()
        val newFormat = newSchema.read(buffer)
        assertEquals(value, newFormat["field1"])
        assertEquals("default", newFormat["field2"])
        assertNull(newFormat["field3"])
        assertEquals(ByteBuffer.allocate(0), newFormat["field4"])
        assertEquals(Long.MAX_VALUE, newFormat["field5"])
    }

    @Test
    fun testReadWhenOptionalDataMissingAtTheEndIsNotTolerated() {
        val oldSchema = Schema(Field("field1", Type.NULLABLE_STRING))
        val newSchema = Schema(
            Field(name = "field1", type = Type.NULLABLE_STRING),
            Field(
                name = "field2",
                type = Type.NULLABLE_STRING,
                docString = "",
                hasDefaultValue = true,
                defaultValue = "default",
            ),
        )
        val value = "foo bar baz"
        val oldFormat = Struct(oldSchema).set("field1", value)
        val buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat))
        oldFormat.writeTo(buffer)
        buffer.flip()
        val e = assertFailsWith<SchemaException> { newSchema.read(buffer) }
        assertContains(e.message!!, "Error reading field 'field2':")
    }

    @Test
    fun testReadWithMissingNonOptionalExtraDataAtTheEnd() {
        val oldSchema = Schema(Field("field1", Type.NULLABLE_STRING))
        val newSchema = Schema(
            true,
            Field("field1", Type.NULLABLE_STRING),
            Field("field2", Type.NULLABLE_STRING)
        )
        val value = "foo bar baz"
        val oldFormat = Struct(oldSchema).set("field1", value)
        val buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat))
        oldFormat.writeTo(buffer)
        buffer.flip()
        val e = assertFailsWith<SchemaException> { newSchema.read(buffer) }
        assertContains(e.message!!, "Missing value for field 'field2' which has no default value")
    }

    @Test
    fun testReadBytesBeyondItsSize() {
        val types = arrayOf<Type>(
            Type.BYTES,
            Type.COMPACT_BYTES,
            Type.NULLABLE_BYTES,
            Type.COMPACT_NULLABLE_BYTES,
        )
        for (type in types) {
            val buffer = ByteBuffer.allocate(20)
            type.write(buffer, ByteBuffer.allocate(4))
            buffer.rewind()
            val bytes = type.read(buffer) as ByteBuffer
            assertFailsWith<IllegalArgumentException> { bytes.limit(bytes.limit() + 1) }
        }
    }
}
