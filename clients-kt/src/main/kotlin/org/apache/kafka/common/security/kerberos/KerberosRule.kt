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

package org.apache.kafka.common.security.kerberos

import java.io.IOException
import java.util.regex.Pattern

/**
 * An encoding of a rule for translating kerberos names.
 */
class KerberosRule {

    private val defaultRealm: String

    private val isDefault: Boolean

    private val numOfComponents: Int

    private val format: String?

    private val match: Pattern?

    private val fromPattern: Pattern?

    private val toPattern: String?

    private val repeat: Boolean

    private val toLowerCase: Boolean

    private val toUpperCase: Boolean

    constructor(defaultRealm: String) {
        this.defaultRealm = defaultRealm
        isDefault = true
        numOfComponents = 0
        format = null
        match = null
        fromPattern = null
        toPattern = null
        repeat = false
        toLowerCase = false
        toUpperCase = false
    }

    constructor(
        defaultRealm: String,
        numOfComponents: Int,
        format: String?,
        match: String?,
        fromPattern: String?,
        toPattern: String?,
        repeat: Boolean,
        toLowerCase: Boolean,
        toUpperCase: Boolean
    ) {
        this.defaultRealm = defaultRealm
        this.isDefault = false
        this.numOfComponents = numOfComponents
        this.format = format
        this.match = if (match == null) null else Pattern.compile(match)
        this.fromPattern = if (fromPattern == null) null else Pattern.compile(fromPattern)
        this.toPattern = toPattern
        this.repeat = repeat
        this.toLowerCase = toLowerCase
        this.toUpperCase = toUpperCase
    }

    override fun toString(): String {
        val buf = StringBuilder()
        if (isDefault) buf.append("DEFAULT")
        else {
            buf.append("RULE:[")
            buf.append(numOfComponents)
            buf.append(':')
            buf.append(format)
            buf.append(']')
            if (match != null) {
                buf.append('(')
                buf.append(match)
                buf.append(')')
            }
            if (fromPattern != null) {
                buf.append("s/")
                buf.append(fromPattern)
                buf.append('/')
                buf.append(toPattern)
                buf.append('/')
                if (repeat) {
                    buf.append('g')
                }
            }
            if (toLowerCase) buf.append("/L")
            if (toUpperCase) buf.append("/U")
        }
        return buf.toString()
    }

    /**
     * Try to apply this rule to the given name represented as a parameter array.
     *
     * @param params first element is the realm, second and later elements are the components of the
     * name "a/b@FOO" -> {"FOO", "a", "b"}.
     * @return the short name if this rule applies or `null`.
     * @throws IOException throws if something is wrong with the rules.
     */
    @Throws(IOException::class)
    fun apply(params: Array<String?>): String? {
        var result: String? = null
        if (isDefault)
            if (defaultRealm == params[0]) result = params[1]
        else if (params.size - 1 == numOfComponents) {
            val base = replaceParameters(format, params)
            if (match == null || match.matcher(base).matches()) {
                result = fromPattern?.let {
                    replaceSubstitution(base, it, toPattern, repeat)
                } ?: base
            }
        }

        if (result != null && NON_SIMPLE_PATTERN.matcher(result).find())
            throw NoMatchingRule("Non-simple name $result after auth_to_local rule $this")

        if (toLowerCase && result != null) result = result.lowercase()
        else if (toUpperCase && result != null) result = result.uppercase()

        return result
    }

    companion object {

        /**
         * A pattern that matches a string without '$' and then a single parameter with $n.
         */
        private val PARAMETER_PATTERN = Pattern.compile("([^$]*)(\\$(\\d*))?")

        /**
         * A pattern that recognizes simple/non-simple names.
         */
        private val NON_SIMPLE_PATTERN = Pattern.compile("[/@]")

        /**
         * Replace the numbered parameters of the form $n where n is from 0 to the length of params
         * - 1. Normal text is copied directly and $n is replaced by the corresponding parameter.
         *
         * @param format The string to replace parameters again.
         * @param params The list of parameters.
         * @return The generated string with the parameter references replaced.
         * @throws BadFormatString
         */
        @Throws(BadFormatString::class)
        fun replaceParameters(
            format: String?,
            params: Array<String?>
        ): String {
            val match = PARAMETER_PATTERN.matcher(format)
            var start = 0
            val result = StringBuilder()
            while (start < format!!.length && match.find(start)) {
                result.append(match.group(1))
                val paramNum = match.group(3)
                if (paramNum != null) {
                    try {
                        val num = paramNum.toInt()
                        if (num < 0 || num >= params.size) throw BadFormatString(
                            "index $num from $format is outside of the valid range 0 " +
                                    "to ${params.size - 1}"
                        )

                        result.append(params[num])
                    } catch (nfe: NumberFormatException) {
                        throw BadFormatString(
                            message = "bad format in username mapping in $paramNum",
                            error = nfe
                        )
                    }
                }
                start = match.end()
            }
            return result.toString()
        }

        /**
         * Replace the matches of the from pattern in the base string with the value of the to
         * string.
         *
         * @param base The string to transform.
         * @param from The pattern to look for in the base string.
         * @param to The string to replace matches of the pattern with.
         * @param repeat Whether the substitution should be repeated.
         * @return
         */
        fun replaceSubstitution(
            base: String,
            from: Pattern,
            to: String?,
            repeat: Boolean,
        ): String {
            val match = from.matcher(base)

            return if (repeat) match.replaceAll(to)
            else match.replaceFirst(to)
        }
    }
}
