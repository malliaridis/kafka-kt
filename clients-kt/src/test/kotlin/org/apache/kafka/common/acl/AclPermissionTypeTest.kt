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

package org.apache.kafka.common.acl

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AclPermissionTypeTest {
    
    private data class AclPermissionTypeTestInfo(
        val ty: AclPermissionType,
        val code: Int,
        val name: String,
        val unknown: Boolean,
    )

    @Test
    @Throws(Exception::class)
    fun testIsUnknown() {
        for (info in INFOS) assertEquals(
            expected = info.unknown,
            actual = info.ty.isUnknown,
            message = "${info.ty} was supposed to have unknown == ${info.unknown}",
        )
    }

    @Test
    @Throws(Exception::class)
    fun testCode() {
        assertEquals(
            expected = AclPermissionType.values().size,
            actual = INFOS.size,
        )
        for (info in INFOS) {
            assertEquals(
                expected = info.code,
                actual = info.ty.code.toInt(),
                message = "${info.ty} was supposed to have code == ${info.code}",
            )
            assertEquals(
                expected = info.ty,
                actual = AclPermissionType.fromCode(info.code.toByte()),
                message = "AclPermissionType.fromCode(${info.code}) was supposed to be ${info.ty}",
            )
        }
        assertEquals(
            expected = AclPermissionType.UNKNOWN,
            actual = AclPermissionType.fromCode(120.toByte()),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testName() {
        for (info in INFOS) assertEquals(
            expected = info.ty,
            actual = AclPermissionType.fromString(info.name),
            message = "AclPermissionType.fromString(${info.name}) was supposed to be ${info.ty}",
        )

        assertEquals(AclPermissionType.UNKNOWN, AclPermissionType.fromString("something"))
    }

    @Test
    @Throws(Exception::class)
    fun testExhaustive() {
        assertEquals(INFOS.size, AclPermissionType.values().size)
        for (i in INFOS.indices) assertEquals(
            expected = INFOS[i].ty,
            actual = AclPermissionType.values()[i],
        )
    }

    companion object {

        private val INFOS = arrayOf(
            AclPermissionTypeTestInfo(
                ty = AclPermissionType.UNKNOWN,
                code = 0,
                name = "unknown",
                unknown = true,
            ),
            AclPermissionTypeTestInfo(
                ty = AclPermissionType.ANY,
                code = 1,
                name = "any",
                unknown = false,
            ),
            AclPermissionTypeTestInfo(
                ty = AclPermissionType.DENY,
                code = 2,
                name = "deny",
                unknown = false,
            ),
            AclPermissionTypeTestInfo(
                ty = AclPermissionType.ALLOW,
                code = 3,
                name = "allow",
                unknown = false,
            )
        )
    }
}
