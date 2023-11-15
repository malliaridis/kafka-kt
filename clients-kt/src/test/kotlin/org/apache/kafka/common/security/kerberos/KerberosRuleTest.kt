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

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class KerberosRuleTest {

    @Test
    @Throws(BadFormatString::class)
    fun testReplaceParameters() {
        // positive test cases
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "",
                params = arrayOfNulls(0),
            ),
            actual = ""
        )
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "hello",
                params = arrayOfNulls(0),
            ),
            actual = "hello"
        )
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "",
                params = arrayOf("too", "many", "parameters", "are", "ok"),
            ),
            actual = "",
        )
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "hello",
                params = arrayOf("too", "many", "parameters", "are", "ok"),
            ),
            actual = "hello"
        )
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "hello $0",
                params = arrayOf("too", "many", "parameters", "are", "ok"),
            ),
            actual = "hello too"
        )
        assertEquals(
            expected = KerberosRule.replaceParameters(
                format = "hello $0",
                params = arrayOf("no recursion $1"),
            ),
            actual = "hello no recursion $1"
        )

        // negative test cases
        assertFailsWith<BadFormatString>("An out-of-bounds parameter number should trigger an exception!") {
            KerberosRule.replaceParameters(format = "$0", params = arrayOf())
        }

        assertFailsWith<BadFormatString>("A malformed parameter name should trigger an exception!") {
            KerberosRule.replaceParameters(
                format = "hello \$a",
                params = arrayOf("does not matter"),
            )
        }
    }
}
