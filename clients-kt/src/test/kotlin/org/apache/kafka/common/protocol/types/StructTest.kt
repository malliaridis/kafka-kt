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

import org.apache.kafka.common.protocol.types.Field.Float64
import org.apache.kafka.common.protocol.types.Field.Int16
import org.apache.kafka.common.protocol.types.Field.Int32
import org.apache.kafka.common.protocol.types.Field.Int64
import org.apache.kafka.common.protocol.types.Field.Int8
import org.apache.kafka.common.protocol.types.Field.Str
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class StructTest {

    @Test
    fun testEquals() {
        var struct1 = Struct(FLAT_STRUCT_SCHEMA)
            .set("int8", 12.toByte())
            .set("int16", 12.toShort())
            .set("int32", 12)
            .set("int64", 12L)
            .set("boolean", true)
            .set("float64", 0.5)
            .set("string", "foobar")
        var struct2 = Struct(FLAT_STRUCT_SCHEMA)
            .set("int8", 12.toByte())
            .set("int16", 12.toShort())
            .set("int32", 12)
            .set("int64", 12L)
            .set("boolean", true)
            .set("float64", 0.5)
            .set("string", "foobar")
        var struct3 = Struct(FLAT_STRUCT_SCHEMA)
            .set("int8", 12.toByte())
            .set("int16", 12.toShort())
            .set("int32", 12)
            .set("int64", 12L)
            .set("boolean", true)
            .set("float64", 0.5)
            .set("string", "mismatching string")
        assertEquals(struct1, struct2)
        assertNotEquals(struct1, struct3)
        val array = arrayOf<Any>(1.toByte(), 2.toByte())
        struct1 = Struct(NESTED_SCHEMA)
            .set("array", array)
            .set("nested", Struct(NESTED_CHILD_SCHEMA).set("int8", 12.toByte()))
        val array2 = arrayOf<Any>(1.toByte(), 2.toByte())
        struct2 = Struct(NESTED_SCHEMA)
            .set("array", array2)
            .set("nested", Struct(NESTED_CHILD_SCHEMA).set("int8", 12.toByte()))
        val array3 = arrayOf<Any>(1.toByte(), 2.toByte(), 3.toByte())
        struct3 = Struct(NESTED_SCHEMA)
            .set("array", array3)
            .set("nested", Struct(NESTED_CHILD_SCHEMA).set("int8", 13.toByte()))
        assertEquals(struct1, struct2)
        assertNotEquals(struct1, struct3)
    }

    companion object {
        
        private val FLAT_STRUCT_SCHEMA = Schema(
            Int8("int8", ""),
            Int16("int16", ""),
            Int32("int32", ""),
            Int64("int64", ""),
            Field.Bool("boolean", ""),
            Float64("float64", ""),
            Str("string", ""),
        )
        
        private val ARRAY_SCHEMA = Schema(Field.Array("array", ArrayOf(Type.INT8), ""))
        
        private val NESTED_CHILD_SCHEMA = Schema(Int8("int8", ""))
        
        private val NESTED_SCHEMA = Schema(
            Field.Array("array", ARRAY_SCHEMA, ""),
            Field("nested", NESTED_CHILD_SCHEMA, ""),
        )
    }
}
