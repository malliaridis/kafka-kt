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

package org.apache.kafka.message

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class StructSpec @JsonCreator constructor(
    @JsonProperty("name") val name: String,
    @JsonProperty("versions") versions: String?,
    @JsonProperty("fields") fields: List<FieldSpec>?,
) {

    val versions: Versions

    @JsonProperty
    val fields: List<FieldSpec>

    val hasKeys: Boolean

    init {
        this.versions = Versions.parse(versions, null)
            ?: throw RuntimeException("You must specify the version of the $name structure.")

        val newFields = ArrayList<FieldSpec>()
        if (fields != null) {
            // Each field should have a unique tag ID (if the field has a tag ID).
            val tags = hashSetOf<Int>()
            for (field in fields) {
                if (field.tag != null) {
                    if (tags.contains(field.tag)) throw RuntimeException(
                        "In $name, field ${field.name} has a duplicate tag ID ${field.tag}. " +
                                "All tags IDs must be unique."
                    )
                    tags.add(field.tag)
                }
                newFields.add(field)
            }
            // Tag IDs should be contiguous and start at 0. This optimizes space on the wire,
            // since larger numbers take more space.
            for (i in tags.indices) {
                if (!tags.contains(i)) throw RuntimeException(
                    "In $name, the tag IDs are not contiguous. Make use of tag $i before " +
                            "using any higher tag IDs."
                )
            }
        }
        this.fields = newFields.toList()
        hasKeys = this.fields.any { field -> field.mapKey }
    }

    @get:JsonProperty
    val versionsString: String
        get() = versions.toString()
}
