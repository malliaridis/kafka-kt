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

/**
 * For versions of a field that are nullable, IsNullCondition creates a null check.
 */
class IsNullConditional private constructor(
    private val name: String,
    private val namePrefix: String? = null,
    private val isLocalName: Boolean = false,
) {

    private var nullableVersions = Versions.ALL

    private var possibleVersions = Versions.ALL

    private var ifNull: Runnable? = null

    private var ifShouldNotBeNull: Runnable? = null

    private var emitBlockScope = { false }

    private var inBlockScope: () -> Unit = {}

    private var conditionalGenerator: ConditionalGenerator = PrimitiveConditionalGenerator

    fun nullableVersions(nullableVersions: Versions): IsNullConditional {
        this.nullableVersions = nullableVersions
        return this
    }

    fun possibleVersions(possibleVersions: Versions): IsNullConditional {
        this.possibleVersions = possibleVersions
        return this
    }

    fun ifNull(ifNull: Runnable?): IsNullConditional {
        this.ifNull = ifNull
        return this
    }

    fun ifShouldNotBeNull(ifShouldNotBeNull: Runnable?): IsNullConditional {
        this.ifShouldNotBeNull = ifShouldNotBeNull
        return this
    }

    fun emitBlockScope(emit: () -> Boolean): IsNullConditional {
        this.emitBlockScope = emit
        return this
    }

    /**
     * Allows to set a [block] that is executed whenever a new block scope is opened, regardless if member or not.
     */
    fun ifInBlockScope(block: () -> Unit): IsNullConditional {
        this.inBlockScope = block
        return this
    }

    @Deprecated(
        message = "Use emitBlockScope() instead",
        replaceWith = ReplaceWith("emitBlockScope { alwaysEmitBlockScope }"),
    )
    fun alwaysEmitBlockScope(alwaysEmitBlockScope: Boolean): IsNullConditional {
        this.emitBlockScope = { alwaysEmitBlockScope }
        return this
    }

    fun conditionalGenerator(conditionalGenerator: ConditionalGenerator): IsNullConditional {
        this.conditionalGenerator = conditionalGenerator
        return this
    }

    fun generate(buffer: CodeBuffer) {
        val ifShouldNotBeNull = ifShouldNotBeNull

        if (emitBlockScope()) {
            buffer.printf("run<Unit> {%n")
            buffer.incrementIndent()
            inBlockScope()
        }

        if (nullableVersions.intersect(possibleVersions).isEmpty) ifShouldNotBeNull?.run()
        else {
            val ifNull = ifNull

            if (ifNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, false))
                buffer.incrementIndent()
                ifNull.run()
                buffer.decrementIndent()
                if (ifShouldNotBeNull != null) {
                    buffer.printf("} else {%n")
                    buffer.incrementIndent()
                    ifShouldNotBeNull.run()
                    buffer.decrementIndent()
                }
                buffer.printf("}%n")
            } else if (ifShouldNotBeNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, true))
                buffer.incrementIndent()
                ifShouldNotBeNull.run()
                buffer.decrementIndent()
                buffer.printf("}%n")
            }
        }

        if (emitBlockScope()) {
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    fun interface ConditionalGenerator {
        fun generate(name: String?, negated: Boolean): String?
    }

    object PrimitiveConditionalGenerator : ConditionalGenerator {

        override fun generate(name: String?, negated: Boolean): String {
            return if (negated) "$name != null"
            else "$name == null"
        }
    }

    companion object {

        fun forName(name: String, prefix: String? = null): IsNullConditional =
            IsNullConditional(
                name = name,
                namePrefix = prefix,
                isLocalName = true,
            )

        fun forField(field: FieldSpec, fieldPrefix: String? = null): IsNullConditional {
            val cond = IsNullConditional(
                name = field.camelCaseName(),
                namePrefix = fieldPrefix,
                field.type.isNullable,
            )
            cond.nullableVersions(field.nullableVersions)
            return cond
        }
    }
}
