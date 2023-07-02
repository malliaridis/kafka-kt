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

package org.apache.kafka.common.security.ssl

import java.io.IOException
import java.util.regex.Pattern
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES

class SslPrincipalMapper(sslPrincipalMappingRules: String) {

    private val rules: List<Rule> = parseRules(splitRules(sslPrincipalMappingRules))

    @Throws(IOException::class)
    fun getName(distinguishedName: String): String {
        for (r in rules) {
            val principalName = r.apply(distinguishedName)
            if (principalName != null) {
                return principalName
            }
        }
        throw NoMatchingRule("No rules apply to $distinguishedName, rules $rules")
    }

    override fun toString(): String = "SslPrincipalMapper(rules = $rules)"

    class NoMatchingRule internal constructor(msg: String?) : IOException(msg)

    private class Rule {

        private val isDefault: Boolean

        private val pattern: Pattern?

        private val replacement: String?

        private val toLowerCase: Boolean

        private val toUpperCase: Boolean

        constructor() {
            isDefault = true
            pattern = null
            replacement = null
            toLowerCase = false
            toUpperCase = false
        }

        constructor(
            pattern: String?,
            replacement: String?,
            toLowerCase: Boolean,
            toUpperCase: Boolean
        ) {
            isDefault = false
            this.pattern = if (pattern == null) null else Pattern.compile(pattern)
            this.replacement = replacement
            this.toLowerCase = toLowerCase
            this.toUpperCase = toUpperCase
        }

        fun apply(distinguishedName: String): String? {
            if (isDefault) {
                return distinguishedName
            }
            var result: String? = null
            val m = pattern!!.matcher(distinguishedName)
            if (m.matches()) {
                result = distinguishedName.replace(
                    pattern.pattern().toRegex(),
                    escapeLiteralBackReferences(replacement!!, m.groupCount())
                )
            }
            if (toLowerCase && result != null) {
                result = result.lowercase()
            } else if (toUpperCase and (result != null)) {
                result = result!!.uppercase()
            }
            return result
        }

        //If we find a back reference that is not valid, then we will treat it as a literal string.
        // For example, if we have 3 capturing groups and the Replacement Value has the value is
        // "$1@$4", then we want to treat the $4 as a literal "$4", rather than attempting to use it
        // as a back reference. This method was taken from Apache Nifi project :
        // org.apache.nifi.authorization.util.IdentityMappingUtil
        private fun escapeLiteralBackReferences(
            unescaped: String,
            numCapturingGroups: Int
        ): String {
            if (numCapturingGroups == 0) return unescaped

            var value = unescaped
            val backRefMatcher = BACK_REFERENCE_PATTERN.matcher(value)

            while (backRefMatcher.find()) {
                val backRefNum = backRefMatcher.group(1)

                if (backRefNum.startsWith("0")) continue

                var backRefIndex = backRefNum.toInt()

                // if we have a replacement value like $123, and we have less than 123 capturing
                // groups, then we want to truncate the 3 and use capturing group 12; if we have
                // less than 12 capturing groups, then we want to truncate the 2 and use capturing
                // group 1; if we don't have a capturing group then we want to truncate the 1 and
                // get 0.
                while (backRefIndex > numCapturingGroups && backRefIndex >= 10)
                    backRefIndex /= 10

                if (backRefIndex > numCapturingGroups) {
                    val sb = StringBuilder(value.length + 1)
                    val groupStart = backRefMatcher.start(1)
                    sb.append(value.substring(0, groupStart - 1))
                    sb.append("\\")
                    sb.append(value.substring(groupStart - 1))
                    value = sb.toString()
                }
            }
            return value
        }

        override fun toString(): String {
            val buf = StringBuilder()

            if (isDefault) buf.append("DEFAULT")
            else {
                buf.append("RULE:")
                if (pattern != null) buf.append(pattern)

                if (replacement != null) {
                    buf.append("/")
                    buf.append(replacement)
                }

                if (toLowerCase) buf.append("/L")
                else if (toUpperCase) buf.append("/U")
            }
            return buf.toString()
        }

        companion object {
            private val BACK_REFERENCE_PATTERN = Pattern.compile("\\$(\\d+)")
        }
    }

    companion object {

        private const val RULE_PATTERN =
            "(DEFAULT)|RULE:((\\\\.|[^\\\\/])*)/((\\\\.|[^\\\\/])*)/([LU]?).*?|(.*?)"

        private val RULE_SPLITTER = Pattern.compile("\\s*($RULE_PATTERN)\\s*(,\\s*|$)")

        private val RULE_PARSER = Pattern.compile(RULE_PATTERN)

        fun fromRules(sslPrincipalMappingRules: String): SslPrincipalMapper {
            return SslPrincipalMapper(sslPrincipalMappingRules)
        }

        private fun splitRules(sslPrincipalMappingRules: String?): List<String> {
            val mappingRules = sslPrincipalMappingRules ?: DEFAULT_SSL_PRINCIPAL_MAPPING_RULES

            val result: MutableList<String> = ArrayList()
            val matcher = RULE_SPLITTER.matcher(mappingRules.trim { it <= ' ' })

            while (matcher.find()) result.add(matcher.group(1))
            return result
        }

        private fun parseRules(rules: List<String>): List<Rule> {
            val result: MutableList<Rule> = ArrayList()

            rules.forEach { rule ->
                val matcher = RULE_PARSER.matcher(rule)

                require(matcher.lookingAt()) { "Invalid rule: $rule" }
                require(rule.length == matcher.end()) {
                    "Invalid rule: `$rule`, unmatched substring: `${rule.substring(matcher.end())}`"
                }

                // empty rules are ignored
                if (matcher.group(1) != null) result.add(Rule())
                else if (matcher.group(2) != null) result.add(
                    Rule(
                        matcher.group(2),
                        matcher.group(4),
                        "L" == matcher.group(6),
                        "U" == matcher.group(6),
                    )
                )
            }
            return result
        }
    }
}
