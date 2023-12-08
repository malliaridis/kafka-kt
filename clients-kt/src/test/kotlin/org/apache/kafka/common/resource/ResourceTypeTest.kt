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

package org.apache.kafka.common.resource

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ResourceTypeTest {
    
    private class AclResourceTypeTestInfo(
        val resourceType: ResourceType,
        val code: Int,
        val name: String,
        val unknown: Boolean,
    )

    @Test
    fun testIsUnknown() {
        for (info in INFOS) assertEquals(
            expected = info.unknown,
            actual = info.resourceType.isUnknown,
            message = "${info.resourceType} was supposed to have unknown == ${info.unknown}",
        )
    }

    @Test
    fun testCode() {
        assertEquals(ResourceType.values().size, INFOS.size)
        for (info in INFOS) {
            assertEquals(
                expected = info.code,
                actual = info.resourceType.code.toInt(),
                message = "${info.resourceType} was supposed to have code == ${info.code}",
            )
            assertEquals(
                expected = info.resourceType,
                actual = ResourceType.fromCode(info.code.toByte()),
                message = "AclResourceType.fromCode(${info.code}) was supposed to be ${info.resourceType}",
            )
        }
        assertEquals(
            expected = ResourceType.UNKNOWN,
            actual = ResourceType.fromCode(120.toByte()),
        )
    }

    @Test
    fun testName() {
        for (info in INFOS) assertEquals(
            expected = info.resourceType,
            actual = ResourceType.fromString(info.name),
            message = "ResourceType.fromString(${info.name}) was supposed to be ${info.resourceType}",
        )

        assertEquals(
            expected = ResourceType.UNKNOWN,
            actual = ResourceType.fromString("something"),
        )
    }

    @Test
    fun testExhaustive() {
        assertEquals(expected = INFOS.size, actual = ResourceType.values().size)
        for (i in INFOS.indices) assertEquals(
            expected = INFOS[i].resourceType,
            actual = ResourceType.values()[i],
        )
    }

    companion object {

        private val INFOS = arrayOf(
            AclResourceTypeTestInfo(
                resourceType = ResourceType.UNKNOWN,
                code = 0,
                name = "unknown",
                unknown = true,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.ANY,
                code = 1,
                name = "any",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.TOPIC,
                code = 2,
                name = "topic",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.GROUP,
                code = 3,
                name = "group",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.CLUSTER,
                code = 4,
                name = "cluster",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.TRANSACTIONAL_ID,
                code = 5,
                name = "transactional_id",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.DELEGATION_TOKEN,
                code = 6,
                name = "delegation_token",
                unknown = false,
            ),
            AclResourceTypeTestInfo(
                resourceType = ResourceType.USER,
                code = 7,
                name = "user",
                unknown = false,
            )
        )
    }
}
