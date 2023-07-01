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

package org.apache.kafka.common.protocol

import java.nio.ByteBuffer
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.record.MemoryRecords

interface Writable {
    
    fun writeByte(value: Byte)
    
    fun writeShort(value: Short)
    
    fun writeInt(value: Int)
    
    fun writeLong(value: Long)
    
    fun writeDouble(value: Double)
    
    fun writeByteArray(arr: ByteArray)
    
    fun writeUnsignedVarint(i: Int)
    
    fun writeByteBuffer(buf: ByteBuffer)
    
    fun writeVarint(i: Int)
    
    fun writeVarlong(i: Long)
    
    fun writeRecords(records: BaseRecords) {
        if (records is MemoryRecords) writeByteBuffer(records.buffer())
        else throw UnsupportedOperationException("Unsupported record type " + records.javaClass)
    }

    fun writeUuid(uuid: Uuid) {
        writeLong(uuid.mostSignificantBits)
        writeLong(uuid.leastSignificantBits)
    }

    fun writeUnsignedShort(i: Int) {
        // The setter functions in the generated code prevent us from setting
        // ints outside the valid range of a short.
        writeShort(i.toShort())
    }

    fun writeUnsignedInt(i: Long) {
        writeInt(i.toInt())
    }
}
