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

class AclOperationTest {

    private data class AclOperationTestInfo(
        val operation: AclOperation,
        val code: Int,
        val name: String,
        val unknown: Boolean,
    )

    @Test
    fun testIsUnknown() {
        for (info in INFOS) {
            assertEquals(
                expected = info.unknown,
                actual = info.operation.isUnknown,
                message = "${info.operation} was supposed to have unknown == ${info.unknown}",
            )
        }
    }

    @Test
    fun testCode() {
        assertEquals(
            expected = AclOperation.values().size,
            actual = INFOS.size,
        )

        for (info in INFOS) {
            assertEquals(
                expected = info.code,
                actual = info.operation.code.toInt(),
                message = "${info.operation} was supposed to have code == ${info.code}",
            )
            assertEquals(
                expected = info.operation,
                actual = AclOperation.fromCode(info.code.toByte()),
                message = "AclOperation.fromCode(${info.code}) was supposed to be ${info.operation}",
            )
        }

        assertEquals(
            expected = AclOperation.UNKNOWN,
            actual = AclOperation.fromCode(120.toByte()),
        )
    }

    @Test
    @Throws(Exception::class)
    fun testName() {
        for (info in INFOS) assertEquals(
            expected = info.operation,
            actual = AclOperation.fromString(info.name),
            message = "AclOperation.fromString(${info.name}) was supposed to be ${info.operation}",
        )

        assertEquals(
            expected = AclOperation.UNKNOWN,
            actual = AclOperation.fromString("something"),
        )
    }

    @Test
    fun testExhaustive() {
        assertEquals(INFOS.size, AclOperation.values().size)
        for (i in INFOS.indices) assertEquals(
            expected = INFOS[i].operation,
            actual = AclOperation.values()[i],
        )
    }

    companion object {
        private val INFOS = arrayOf(
            AclOperationTestInfo(
                operation = AclOperation.UNKNOWN,
                code = 0,
                name = "unknown",
                unknown = true,
            ),
            AclOperationTestInfo(
                operation = AclOperation.ANY,
                code = 1,
                name = "any",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.ALL,
                code = 2,
                name = "all",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.READ,
                code = 3,
                name = "read",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.WRITE,
                code = 4,
                name = "write",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.CREATE,
                code = 5,
                name = "create",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.DELETE,
                code = 6,
                name = "delete",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.ALTER,
                code = 7,
                name = "alter",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.DESCRIBE,
                code = 8,
                name = "describe",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.CLUSTER_ACTION,
                code = 9,
                name = "cluster_action",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.DESCRIBE_CONFIGS,
                code = 10,
                name = "describe_configs",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.ALTER_CONFIGS,
                code = 11,
                name = "alter_configs",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.IDEMPOTENT_WRITE,
                code = 12,
                name = "idempotent_write",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.CREATE_TOKENS,
                code = 13,
                name = "create_tokens",
                unknown = false,
            ),
            AclOperationTestInfo(
                operation = AclOperation.DESCRIBE_TOKENS,
                code = 14,
                name = "describe_tokens",
                unknown = false,
            )
        )
    }
}
