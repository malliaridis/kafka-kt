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

import org.apache.kafka.message.FieldType.StructType
import org.apache.kafka.message.MessageGenerator.firstIsCapitalized
import java.util.*

/**
 * Contains structure data for Kafka MessageData classes.
 */
class StructRegistry {

    private val structs: MutableMap<String, StructInfo> = TreeMap()

    private val commonStructNames: MutableSet<String> = TreeSet()

    /**
     * @property spec The specification for this structure.
     * @property parentVersions The versions which the parent(s) of this structure can have. If this
     * is a top-level structure, this will be equal to the versions which the overall message can
     * have.
     */
    data class StructInfo(
        val spec: StructSpec,
        val parentVersions: Versions
    ) {

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("spec"),
        )
        fun spec(): StructSpec = spec

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("parentVersions"),
        )
        fun parentVersions(): Versions = parentVersions
    }

    /**
     * Register all the structures contained a message spec.
     */
    @Throws(Exception::class)
    fun register(message: MessageSpec) {
        // Register common structures.
        for (struct in message.commonStructs()) {
            if (!firstIsCapitalized(struct.name())) throw RuntimeException(
                "Can't process structure ${struct.name()}: the first letter of structure names " +
                        "must be capitalized."
            )

            if (structs.containsKey(struct.name()))
                throw RuntimeException("Common struct ${struct.name()} was specified twice.")

            structs[struct.name()] = StructInfo(struct, struct.versions())
            commonStructNames.add(struct.name())
        }
        // Register inline structures.
        addStructSpecs(message.validVersions(), message.fields())
    }

    private fun addStructSpecs(parentVersions: Versions, fields: List<FieldSpec>) {
        for (field: FieldSpec in fields) {
            var typeName: String? = null
            if (field.type.isStructArray) {
                val arrayType = field.type as FieldType.ArrayType
                typeName = arrayType.elementName()
            } else if (field.type.isStruct) {
                val structType = field.type as StructType
                typeName = structType.typeName()
            }
            if (typeName != null) {
                if (commonStructNames.contains(typeName)) {
                    // If we're using a common structure, we can't specify its fields.
                    // The fields should be specified in the commonStructs area.
                    if (field.fields.isNotEmpty()) throw RuntimeException(
                        "Can't re-specify the common struct $typeName as an inline struct."
                    )
                } else if (structs.containsKey(typeName)) {
                    // Inline structures should only appear once.
                    throw RuntimeException("Struct $typeName was specified twice.")
                } else {
                    // Synthesize a StructSpec object out of the fields.
                    val spec = StructSpec(
                        typeName,
                        field.versions.toString(),
                        field.fields
                    )
                    structs[typeName] = StructInfo(spec, parentVersions)
                }
                addStructSpecs(parentVersions.intersect(field.versions!!), field.fields)
            }
        }
    }

    /**
     * Locate the struct corresponding to a field.
     */
    fun findStruct(field: FieldSpec): StructSpec {
        val structFieldName: String = if (field.type.isArray) {
            val arrayType = field.type as FieldType.ArrayType
            arrayType.elementName()
        } else if (field.type.isStruct) {
            val structType = field.type as StructType
            structType.typeName()
        } else throw RuntimeException(
            "Field ${field.name} cannot be treated as a structure."
        )

        val structInfo = structs[structFieldName] ?: throw RuntimeException(
            "Unable to locate a specification for the structure $structFieldName"
        )
        return structInfo.spec
    }

    /**
     * Return true if the field is a struct array with keys.
     */
    fun isStructArrayWithKeys(field: FieldSpec): Boolean {
        if (!field.type.isArray) return false

        val arrayType = field.type as FieldType.ArrayType
        if (!arrayType.isStructArray) return false

        val structInfo = structs[arrayType.elementName()] ?: throw RuntimeException(
            "Unable to locate a specification for the structure ${arrayType.elementName()}"
        )
        return structInfo.spec.hasKeys()
    }

    fun commonStructNames(): Set<String> = commonStructNames

    /**
     * Returns an iterator that will step through all the common structures.
     */
    fun commonStructs(): Iterator<StructSpec> {
        return object : Iterator<StructSpec> {

            private val iter: Iterator<String> = commonStructNames.iterator()

            override fun hasNext(): Boolean = iter.hasNext()

            override fun next(): StructSpec = structs[iter.next()]!!.spec
        }
    }

    fun structs(): Iterator<StructInfo> = structs.values.iterator()
}
