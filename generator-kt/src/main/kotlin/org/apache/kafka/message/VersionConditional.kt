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
 * Creates an if statement based on whether the current version falls within a given range.
 */
class VersionConditional private constructor(
    private val containingVersions: Versions,
    private val possibleVersions: Versions,
) {
    
    private var ifMember: ClauseGenerator? = null
    
    private var ifNotMember: ClauseGenerator? = null
    
    private var alwaysEmitBlockScope = false
    
    private var allowMembershipCheckAlwaysFalse = true
    
    fun ifMember(ifMember: ClauseGenerator?): VersionConditional {
        this.ifMember = ifMember
        return this
    }

    fun ifNotMember(ifNotMember: ClauseGenerator?): VersionConditional {
        this.ifNotMember = ifNotMember
        return this
    }

    /**
     * If this is set, we will always create a new block scope, even if there are no 'if'
     * statements. This is useful for cases where we want to declare variables in the clauses
     * without worrying if they conflict with other variables of the same name.
     */
    fun alwaysEmitBlockScope(alwaysEmitBlockScope: Boolean): VersionConditional {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope
        return this
    }

    /**
     * If this is set, VersionConditional#generate will throw an exception if the 'ifMember' clause
     * is never used. This is useful as a sanity check in some cases where it doesn't make sense
     * for the condition to always be false. For example, when generating a Message#write function,
     * we might check that the version we're writing is supported. It wouldn't make sense for this
     * check to always be false, since that would mean that no versions at all were supported.
     */
    fun allowMembershipCheckAlwaysFalse(
        allowMembershipCheckAlwaysFalse: Boolean,
    ): VersionConditional {
        this.allowMembershipCheckAlwaysFalse = allowMembershipCheckAlwaysFalse
        return this
    }

    private fun generateFullRangeCheck(
        ifVersions: Versions,
        ifNotVersions: Versions,
        buffer: CodeBuffer,
    ) {
        val ifMember = ifMember
        val ifNotMember = ifNotMember
        if (ifMember != null) {
            buffer.printf(
                "if ((version >= %d) && (version <= %d)) {%n",
                containingVersions.lowest,
                containingVersions.highest
            )
            buffer.incrementIndent()
            ifMember.generate(ifVersions)
            buffer.decrementIndent()
            if (ifNotMember != null) {
                buffer.printf("} else {%n")
                buffer.incrementIndent()
                ifNotMember.generate(ifNotVersions)
                buffer.decrementIndent()
            }
            buffer.printf("}%n")
        } else if (ifNotMember != null) {
            buffer.printf(
                "if ((version < %d) || (version > %d)) {%n",
                containingVersions.lowest,
                containingVersions.highest
            )
            buffer.incrementIndent()
            ifNotMember.generate(ifNotVersions)
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    private fun generateLowerRangeCheck(
        ifVersions: Versions,
        ifNotVersions: Versions?,
        buffer: CodeBuffer,
    ) {
        val ifMember = ifMember
        val ifNotMember = ifNotMember
        if (ifMember != null) {
            buffer.printf("if (version >= %d) {%n", containingVersions.lowest)
            buffer.incrementIndent()
            ifMember.generate(ifVersions)
            buffer.decrementIndent()
            if (ifNotMember != null) {
                buffer.printf("} else {%n")
                buffer.incrementIndent()
                ifNotMember.generate(ifNotVersions!!)
                buffer.decrementIndent()
            }
            buffer.printf("}%n")
        } else if (ifNotMember != null) {
            buffer.printf("if (version < %d) {%n", containingVersions.lowest)
            buffer.incrementIndent()
            ifNotMember.generate(ifNotVersions!!)
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    private fun generateUpperRangeCheck(
        ifVersions: Versions,
        ifNotVersions: Versions?,
        buffer: CodeBuffer,
    ) {
        val ifMember = ifMember
        val ifNotMember = ifNotMember
        if (ifMember != null) {
            buffer.printf("if (version <= %d) {%n", containingVersions.highest)
            buffer.incrementIndent()
            ifMember.generate(ifVersions)
            buffer.decrementIndent()
            if (ifNotMember != null) {
                buffer.printf("} else {%n")
                buffer.incrementIndent()
                ifNotMember.generate(ifNotVersions!!)
                buffer.decrementIndent()
            }
            buffer.printf("}%n")
        } else if (ifNotMember != null) {
            buffer.printf("if (version > %d) {%n", containingVersions.highest)
            buffer.incrementIndent()
            ifNotMember.generate(ifNotVersions!!)
            buffer.decrementIndent()
            buffer.printf("}%n")
        }
    }

    private fun generateAlwaysTrueCheck(ifVersions: Versions, buffer: CodeBuffer) {
        ifMember?.let { ifMember ->
            if (alwaysEmitBlockScope) {
                buffer.printf("run {%n")
                buffer.incrementIndent()
            }
            ifMember.generate(ifVersions)
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent()
                buffer.printf("}%n")
            }
        }
    }

    private fun generateAlwaysFalseCheck(ifNotVersions: Versions?, buffer: CodeBuffer) {
        if (!allowMembershipCheckAlwaysFalse) throw RuntimeException(
            "Version ranges $containingVersions and $possibleVersions have no versions in common."
        )

        ifNotMember?.let { ifNotMember ->
            if (alwaysEmitBlockScope) {
                buffer.printf("run {%n")
                buffer.incrementIndent()
            }
            ifNotMember.generate((ifNotVersions)!!)
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent()
                buffer.printf("}%n")
            }
        }
    }

    fun generate(buffer: CodeBuffer) {
        val ifVersions = possibleVersions.intersect(containingVersions)
        var ifNotVersions = possibleVersions.subtract(containingVersions)

        // In the case where ifNotVersions would be two ranges rather than one, we just pass in the
        // original possibleVersions instead. This is slightly less optimal, but allows us to avoid
        // dealing with multiple ranges.
        if (ifNotVersions == null) {
            ifNotVersions = possibleVersions
        }
        if (possibleVersions.lowest < containingVersions.lowest) {
            if (possibleVersions.highest > containingVersions.highest)
                generateFullRangeCheck(ifVersions, ifNotVersions, buffer)
            else if (possibleVersions.highest >= containingVersions.lowest)
                generateLowerRangeCheck(ifVersions, ifNotVersions, buffer)
            else generateAlwaysFalseCheck(ifNotVersions, buffer)
        } else if (
            possibleVersions.highest >= containingVersions.lowest
            && possibleVersions.lowest <= containingVersions.highest
        ) {
            if (possibleVersions.highest > containingVersions.highest)
                generateUpperRangeCheck(ifVersions, ifNotVersions, buffer)
            else generateAlwaysTrueCheck(ifVersions, buffer)
        } else generateAlwaysFalseCheck(ifNotVersions, buffer)
    }

    companion object {

        /**
         * Create a version conditional.
         *
         * @param containingVersions The versions for which the conditional is true.
         * @param possibleVersions The range of possible versions.
         * @return The version conditional.
         */
        fun forVersions(
            containingVersions: Versions,
            possibleVersions: Versions,
        ): VersionConditional = VersionConditional(containingVersions, possibleVersions)
    }
}
