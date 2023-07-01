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

import org.apache.kafka.common.protocol.Writable

/**
 * The RawTaggedFieldWriter is used by Message subclasses to serialize their
 * lists of raw tags.
 */
class RawTaggedFieldWriter private constructor(private val fields: List<RawTaggedField>) {

    private val iter: ListIterator<RawTaggedField> = fields.listIterator()

    private var prevTag: Int = -1

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("numFields")
    )
    fun numFields(): Int = fields.size

    val numFields: Int
        get() = fields.size

    fun writeRawTags(writable: Writable, nextDefinedTag: Int) {
        while (iter.hasNext()) {
            val field = iter.next()
            val tag = field.tag

            if (tag >= nextDefinedTag) {
                if (tag == nextDefinedTag)
                    // We must not have a raw tag field that duplicates the tag of another field.
                    throw RuntimeException("Attempted to use tag $tag as an undefined tag.")

                iter.previous()
                return
            }

            if (tag <= prevTag)
                // The raw tag field list must be sorted by tag, and there must not be
                // any duplicate tags.
                throw RuntimeException(
                    "Invalid raw tag field list: tag $tag comes after tag $prevTag, but is not " +
                            "higher than it."
                )

            writable.writeUnsignedVarint(field.tag())
            writable.writeUnsignedVarint(field.data().size)
            writable.writeByteArray(field.data())
            prevTag = tag
        }
    }

    companion object {

        private val EMPTY_WRITER = RawTaggedFieldWriter(ArrayList(0))

        fun forFields(fields: List<RawTaggedField>?): RawTaggedFieldWriter {
            return if (fields == null) EMPTY_WRITER
            else RawTaggedFieldWriter(fields)
        }
    }
}
