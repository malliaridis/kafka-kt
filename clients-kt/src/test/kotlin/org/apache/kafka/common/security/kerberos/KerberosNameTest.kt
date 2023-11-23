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
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class KerberosNameTest {

    @Test
    @Throws(IOException::class)
    fun testParse() {
        val rules: List<String> = listOf(
            "RULE:[1:$1](App\\..*)s/App\\.(.*)/$1/g",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g",
            "DEFAULT",
        )
        val shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules)
        var name = KerberosName.parse("App.service-name/example.com@REALM.COM")
        assertEquals("App.service-name", name.serviceName)
        assertEquals("example.com", name.hostName)
        assertEquals("REALM.COM", name.realm)
        assertEquals("service-name", shortNamer.shortName(name))
        name = KerberosName.parse("App.service-name@REALM.COM")
        assertEquals("App.service-name", name.serviceName)
        assertNull(name.hostName)
        assertEquals("REALM.COM", name.realm)
        assertEquals("service-name", shortNamer.shortName(name))
        name = KerberosName.parse("user/host@REALM.COM")
        assertEquals("user", name.serviceName)
        assertEquals("host", name.hostName)
        assertEquals("REALM.COM", name.realm)
        assertEquals("user", shortNamer.shortName(name))
    }

    @Test
    @Throws(Exception::class)
    fun testToLowerCase() {
        val rules: List<String> = listOf(
            "RULE:[1:$1]/L",
            "RULE:[2:$1](Test.*)s/ABC///L",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/L",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/L",
            "RULE:[2:$1]/L",
            "DEFAULT"
        )
        val shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules)
        var name = KerberosName.parse("User@REALM.COM")
        assertEquals("user", shortNamer.shortName(name))
        name = KerberosName.parse("TestABC/host@FOO.COM")
        assertEquals("test", shortNamer.shortName(name))
        name = KerberosName.parse("ABC_User_ABC/host@FOO.COM")
        assertEquals("xyz_user_xyz", shortNamer.shortName(name))
        name = KerberosName.parse("App.SERVICE-name/example.com@REALM.COM")
        assertEquals("service-name", shortNamer.shortName(name))
        name = KerberosName.parse("User/root@REALM.COM")
        assertEquals("user", shortNamer.shortName(name))
    }

    @Test
    @Throws(Exception::class)
    fun testToUpperCase() {
        val rules: List<String> = listOf(
            "RULE:[1:$1]/U",
            "RULE:[2:$1](Test.*)s/ABC///U",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/U",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/U",
            "RULE:[2:$1]/U",
            "DEFAULT",
        )
        val shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules)
        var name = KerberosName.parse("User@REALM.COM")
        assertEquals("USER", shortNamer.shortName(name))
        name = KerberosName.parse("TestABC/host@FOO.COM")
        assertEquals("TEST", shortNamer.shortName(name))
        name = KerberosName.parse("ABC_User_ABC/host@FOO.COM")
        assertEquals("XYZ_USER_XYZ", shortNamer.shortName(name))
        name = KerberosName.parse("App.SERVICE-name/example.com@REALM.COM")
        assertEquals("SERVICE-NAME", shortNamer.shortName(name))
        name = KerberosName.parse("User/root@REALM.COM")
        assertEquals("USER", shortNamer.shortName(name))
    }

    @Test
    fun testInvalidRules() {
        testInvalidRule(listOf("default"))
        testInvalidRule(listOf("DEFAUL"))
        testInvalidRule(listOf("DEFAULT/L"))
        testInvalidRule(listOf("DEFAULT/g"))
        testInvalidRule(listOf("rule:[1:$1]"))
        testInvalidRule(listOf("rule:[1:$1]/L/U"))
        testInvalidRule(listOf("rule:[1:$1]/U/L"))
        testInvalidRule(listOf("rule:[1:$1]/LU"))
        testInvalidRule(listOf("RULE:[1:$1/L"))
        testInvalidRule(listOf("RULE:[1:$1]/l"))
        testInvalidRule(listOf("RULE:[2:$1](ABC.*)s/ABC/XYZ/L/g"))
    }

    private fun testInvalidRule(rules: List<String>) {
        assertFailsWith<IllegalArgumentException>(
            message = "should have thrown IllegalArgumentException",
        ) { KerberosShortNamer.fromUnparsedRules("REALM.COM", rules) }
    }
}
