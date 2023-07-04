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

class Target internal constructor(
    val field: FieldSpec,
    val sourceVariable: String,
    val humanReadableName: String,
    private val assignmentStatementGenerator: (String) -> String,
) {

    fun assignmentStatement(rightHandSide: String): String =
        assignmentStatementGenerator(rightHandSide)

    fun nonNullableCopy(): Target {
        val nonNullableField = FieldSpec(
            name = field.name,
            versions = field.versionsString,
            fields = field.fields,
            type = field.typeString,
            mapKey = field.mapKey,
            nullableVersions = Versions.NONE.toString(),
            fieldDefault = field.fieldDefault,
            ignorable = field.ignorable,
            entityType = field.entityType,
            about = field.about,
            taggedVersions = field.taggedVersionsString,
            flexibleVersions = field.flexibleVersionsString,
            tag = field.tagInteger,
            zeroCopy = field.zeroCopy,
        )
        return Target(
            field = nonNullableField,
            sourceVariable = sourceVariable,
            humanReadableName = humanReadableName,
            assignmentStatementGenerator = assignmentStatementGenerator
        )
    }

    fun arrayElementTarget(assignmentStatementGenerator: (String) -> String): Target {
        if (!field.type.isArray) throw RuntimeException("Field $field is not an array.")

        val arrayType = field.type as FieldType.ArrayType
        val elementField = FieldSpec(
            name = field.name + "Element",
            versions = field.versions.toString(), fields = emptyList(),
            type = arrayType.elementType.toString(),
            mapKey = false,
            nullableVersions = Versions.NONE.toString(),
            fieldDefault = "",
            ignorable = false,
            entityType = EntityType.UNKNOWN,
            about = "",
            taggedVersions = Versions.NONE.toString(),
            flexibleVersions = field.flexibleVersionsString,
            tag = null,
            zeroCopy = field.zeroCopy,
        )
        return Target(
            field = elementField,
            sourceVariable = "_element",
            humanReadableName = "$humanReadableName element",
            assignmentStatementGenerator = assignmentStatementGenerator,
        )
    }
}
