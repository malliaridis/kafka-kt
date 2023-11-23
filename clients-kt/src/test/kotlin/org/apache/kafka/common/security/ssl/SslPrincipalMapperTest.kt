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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SslPrincipalMapperTest {

    @Test
    fun testValidRules() {
        testValidRule("DEFAULT")
        testValidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/")
        testValidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L, DEFAULT")
        testValidRule("RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/")
        testValidRule("RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L")
        testValidRule("RULE:^cn=(.?),ou=(.?),dc=(.?),dc=(.?)$/$1@$2/U")
        testValidRule("RULE:^CN=([^,ADEFLTU,]+)(,.*|$)/$1/")
        testValidRule("RULE:^CN=([^,DEFAULT,]+)(,.*|$)/$1/")
    }

    private fun testValidRule(rules: String) {
        SslPrincipalMapper.fromRules(rules)
    }

    @Test
    fun testInvalidRules() {
        testInvalidRule("default")
        testInvalidRule("DEFAUL")
        testInvalidRule("DEFAULT/L")
        testInvalidRule("DEFAULT/U")
        testInvalidRule("RULE:CN=(.*?),OU=ServiceUsers.*/$1")
        testInvalidRule("rule:^CN=(.*?),OU=ServiceUsers.*$/$1/")
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L/U")
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/L")
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/U")
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/LU")
    }

    private fun testInvalidRule(rules: String) {
        assertFailsWith<IllegalArgumentException>(
            message = "should have thrown IllegalArgumentException",
        ) { println(SslPrincipalMapper.fromRules(rules)) }
    }

    @Test
    @Throws(Exception::class)
    fun testSslPrincipalMapper() {
        val rules = listOf(
            "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L",
            "RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L",
            "RULE:^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$/$1@$2/U",
            "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U",
            "DEFAULT"
        ).joinToString(", ")
        val mapper = SslPrincipalMapper.fromRules(rules)
        assertEquals(
            expected = "duke",
            actual = mapper.getName(distinguishedName = "CN=Duke,OU=ServiceUsers,O=Org,C=US"),
        )
        assertEquals(
            expected = "duke@sme",
            actual = mapper.getName(distinguishedName = "CN=Duke,OU=SME,O=mycp,L=Fulton,ST=MD,C=US"),
        )
        assertEquals(
            expected = "DUKE@SME",
            actual = mapper.getName(distinguishedName = "cn=duke,ou=sme,dc=mycp,dc=com"),
        )
        assertEquals(
            expected = "DUKE",
            actual = mapper.getName(distinguishedName = "cN=duke,OU=JavaSoft,O=Sun Microsystems"),
        )
        assertEquals(
            expected = "OU=JavaSoft,O=Sun Microsystems,C=US",
            actual = mapper.getName(distinguishedName = "OU=JavaSoft,O=Sun Microsystems,C=US"),
        )
    }

    private fun testRulesSplitting(expected: String, rules: String) {
        val mapper = SslPrincipalMapper.fromRules(rules)
        assertEquals("SslPrincipalMapper(rules = $expected)", mapper.toString())
    }

    @Test
    fun testRulesSplitting() {
        // seeing is believing
        testRulesSplitting(
            expected = "[]",
            rules = "",
        )
        testRulesSplitting(
            expected = "[DEFAULT]",
            rules = "DEFAULT",
        )
        testRulesSplitting(
            expected = "[RULE:/]",
            rules = "RULE://",
        )
        testRulesSplitting(
            expected = "[RULE:/.*]",
            rules = "RULE:/.*/",
        )
        testRulesSplitting(
            expected = "[RULE:/.*/L]",
            rules = "RULE:/.*/L",
        )
        testRulesSplitting(
            expected = "[RULE:/, DEFAULT]",
            rules = "RULE://,DEFAULT",
        )
        testRulesSplitting(
            expected = "[RULE:/, DEFAULT]",
            rules = "  RULE:// ,  DEFAULT  ",
        )
        testRulesSplitting(
            expected = "[RULE:   /     , DEFAULT]",
            rules = "  RULE:   /     / ,  DEFAULT  ",
        )
        testRulesSplitting(
            expected = "[RULE:  /     /U, DEFAULT]",
            rules = "  RULE:  /     /U   ,DEFAULT  ",
        )
        testRulesSplitting(
            expected = "[RULE:([A-Z]*)/$1/U, RULE:([a-z]+)/$1, DEFAULT]",
            rules = "  RULE:([A-Z]*)/$1/U   ,RULE:([a-z]+)/$1/,   DEFAULT  ",
        )

        // empty rules are ignored
        testRulesSplitting(
            expected = "[]",
            rules = ",   , , ,      , , ,   ",
        )
        testRulesSplitting(
            expected = "[RULE:/, DEFAULT]",
            rules = ",,RULE://,,,DEFAULT,,",
        )
        testRulesSplitting(
            expected = "[RULE: /   , DEFAULT]",
            rules = ",  , RULE: /   /    ,,,   DEFAULT, ,   ",
        )
        testRulesSplitting(
            expected = "[RULE:   /  /U, DEFAULT]",
            rules = "     ,  , RULE:   /  /U    ,,  ,DEFAULT, ,",
        )

        // escape sequences
        testRulesSplitting(
            expected = "[RULE:\\/\\\\\\(\\)\\n\\t/\\/\\/]",
            rules = "RULE:\\/\\\\\\(\\)\\n\\t/\\/\\//",
        )
        testRulesSplitting(
            expected = "[RULE:\\**\\/+/*/L, RULE:\\/*\\**/**]",
            rules = "RULE:\\**\\/+/*/L,RULE:\\/*\\**/**/",
        )

        // rules rule
        testRulesSplitting(
            expected =
            "[RULE:,RULE:,/,RULE:,\\//U, RULE:,/RULE:,, RULE:,RULE:,/L,RULE:,/L, RULE:, DEFAULT, /DEFAULT, DEFAULT]",
            rules =
            "RULE:,RULE:,/,RULE:,\\//U,RULE:,/RULE:,/,RULE:,RULE:,/L,RULE:,/L,RULE:, DEFAULT, /DEFAULT/,DEFAULT",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCommaWithWhitespace() {
        val rules = "RULE:^CN=((\\\\, *|\\w)+)(,.*|$)/$1/,DEFAULT"
        val mapper = SslPrincipalMapper.fromRules(rules)
        assertEquals(
            expected = "Tkac\\, Adam",
            actual = mapper.getName("CN=Tkac\\, Adam,OU=ITZ,DC=geodis,DC=cz"),
        )
    }
}
