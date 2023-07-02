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
class IsNullConditional private constructor(private val name: String) {

    private var nullableVersions = Versions.ALL

    private var possibleVersions = Versions.ALL

    private var ifNull: Runnable? = null

    private var ifShouldNotBeNull: Runnable? = null

    private var alwaysEmitBlockScope = false

    private var conditionalGenerator: ConditionalGenerator = PrimitiveConditionalGenerator.INSTANCE

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

    fun alwaysEmitBlockScope(alwaysEmitBlockScope: Boolean): IsNullConditional {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope
        return this
    }

    fun conditionalGenerator(conditionalGenerator: ConditionalGenerator): IsNullConditional {
        this.conditionalGenerator = conditionalGenerator
        return this
    }

    fun generate(buffer: CodeBuffer) {
        val ifShouldNotBeNull = ifShouldNotBeNull

        if (nullableVersions.intersect(possibleVersions).empty()) {
            if (ifShouldNotBeNull != null) {
                if (alwaysEmitBlockScope) {
                    buffer.printf("{%n")
                    buffer.incrementIndent()
                }
                ifShouldNotBeNull.run()
                if (alwaysEmitBlockScope) {
                    buffer.decrementIndent()
                    buffer.printf("}%n")
                }
            }
        } else {
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
    }

    fun interface ConditionalGenerator {
        fun generate(name: String?, negated: Boolean): String?
    }

    private class PrimitiveConditionalGenerator : ConditionalGenerator {

        override fun generate(name: String?, negated: Boolean): String {
            return if (negated) "$name != null"
            else "$name == null"
        }

        companion object {
            val INSTANCE = PrimitiveConditionalGenerator()
        }
    }

    companion object {

        fun forName(name: String): IsNullConditional = IsNullConditional(name)

        fun forField(field: FieldSpec): IsNullConditional {
            val cond = IsNullConditional(field.camelCaseName())
            cond.nullableVersions(field.nullableVersions)
            return cond
        }
    }
}
