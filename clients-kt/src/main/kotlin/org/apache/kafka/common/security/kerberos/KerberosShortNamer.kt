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
 * This class implements parsing and handling of Kerberos principal names. In particular, it splits
 * them apart and translates them down into local operating system names.
 *
 * @property principalToLocalRules Rules for the translation of the principal name into an operating
 * system name
 */
class KerberosShortNamer(private val principalToLocalRules: List<KerberosRule>) {

    /**
     * Get the translation of the principal name into an operating system user name.
     * @return the short name
     * @throws IOException
     */
    @Throws(IOException::class)
    fun shortName(kerberosName: KerberosName): String {
        val params: Array<String?> =
            if (kerberosName.hostName == null) {
                // if it is already simple, just return it
                if (kerberosName.realm == null) return kerberosName.serviceName

                arrayOf(kerberosName.realm, kerberosName.serviceName)
            } else arrayOf(kerberosName.realm, kerberosName.serviceName, kerberosName.hostName)

        principalToLocalRules.forEach { rule ->
            val result = rule.apply(params)
            if (result != null) return result
        }

        throw NoMatchingRule("No rules apply to $kerberosName, rules $principalToLocalRules")
    }

    override fun toString(): String =
        "KerberosShortNamer(principalToLocalRules = $principalToLocalRules)"

    companion object {

        /**
         * A pattern for parsing a auth_to_local rule.
         */
        private val RULE_PARSER = Pattern.compile(
            "((DEFAULT)|((RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?(s/([^/]*)/([^/]*)/(g)?)?/?(L|U)?)))"
        )

        fun fromUnparsedRules(
            defaultRealm: String,
            principalToLocalRules: List<String>?
        ): KerberosShortNamer {
            val rules: List<String> = principalToLocalRules ?: listOf("DEFAULT")
            return KerberosShortNamer(parseRules(defaultRealm, rules))
        }

        private fun parseRules(defaultRealm: String, rules: List<String>): List<KerberosRule> {
            val result = mutableListOf<KerberosRule>()

            rules.forEach { rule ->
                val matcher = RULE_PARSER.matcher(rule)

                require(matcher.lookingAt()) { "Invalid rule: $rule" }
                require(rule.length == matcher.end()) {
                    "Invalid rule: `$rule`, unmatched substring: `${rule.substring(matcher.end())}`"
                }

                if (matcher.group(2) != null) result.add(KerberosRule(defaultRealm))
                else result.add(
                    KerberosRule(
                        defaultRealm,
                        matcher.group(5).toInt(),
                        matcher.group(6),
                        matcher.group(8),
                        matcher.group(10),
                        matcher.group(11),
                        "g" == matcher.group(12),
                        "L" == matcher.group(13),
                        "U" == matcher.group(13)
                    )
                )
            }
            return result
        }
    }
}
