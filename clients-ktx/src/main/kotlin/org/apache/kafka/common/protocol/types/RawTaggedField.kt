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

data class RawTaggedField(
    val tag: Int,
    val data: ByteArray
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("tag")
    )
    fun tag(): Int = tag

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("data")
    )
    fun data(): ByteArray = data

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("size")
    )
    fun size(): Int = data.size

    val size: Int
        get() = data.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RawTaggedField

        if (tag != other.tag) return false
        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int {
        var result = tag
        result = 31 * result + data.contentHashCode()
        return result
    }
}
