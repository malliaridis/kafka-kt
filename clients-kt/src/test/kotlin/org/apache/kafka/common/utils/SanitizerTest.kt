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

package org.apache.kafka.common.utils

import java.lang.management.ManagementFactory
import javax.management.MBeanException
import javax.management.MalformedObjectNameException
import javax.management.ObjectName
import javax.management.OperationsException
import org.apache.kafka.common.utils.Sanitizer.desanitize
import org.apache.kafka.common.utils.Sanitizer.jmxSanitize
import org.apache.kafka.common.utils.Sanitizer.sanitize
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class SanitizerTest {

    @Test
    fun testSanitize() {
        val principal = "CN=Some characters !@#$%&*()_-+=';:,/~"
        val sanitizedPrincipal = sanitize(principal)
        assertTrue(sanitizedPrincipal.replace('%', '_').matches("[a-zA-Z0-9._\\-]+".toRegex()))
        assertEquals(principal, desanitize(sanitizedPrincipal))
    }

    @Test
    @Throws(MalformedObjectNameException::class)
    fun testJmxSanitize() {
        var unquoted = 0
        for (i in 0..65535) {
            val c = i.toChar()
            val value = "value$c"
            val jmxSanitizedValue = jmxSanitize(value)
            if (jmxSanitizedValue == value) unquoted++
            verifyJmx(jmxSanitizedValue, i)
            val encodedValue = sanitize(value)
            verifyJmx(encodedValue, i)
            // jmxSanitize should not sanitize URL-encoded values
            assertEquals(encodedValue, jmxSanitize(encodedValue))
        }
        assertEquals(68, unquoted) // a-zA-Z0-9-_% space and tab
    }

    @Throws(MalformedObjectNameException::class)
    private fun verifyJmx(sanitizedValue: String, c: Int) {
        val mbean: Any = TestStat()
        val server = ManagementFactory.getPlatformMBeanServer()
        val objectName = ObjectName("test:key=$sanitizedValue")
        try {
            server.registerMBean(mbean, objectName)
            server.unregisterMBean(objectName)
        } catch (_: OperationsException) {
            fail("Could not register char=\\u$c")
        } catch (_: MBeanException) {
            fail("Could not register char=\\u$c")
        }
    }

    interface TestStatMBean {
        val value: Int
    }

    inner class TestStat : TestStatMBean {
        override val value: Int = 1
    }
}
