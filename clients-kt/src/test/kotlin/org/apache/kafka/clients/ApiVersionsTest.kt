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

package org.apache.kafka.clients

import org.apache.kafka.common.message.ApiVersionsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.RecordBatch
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ApiVersionsTest {

    @Test
    fun testMaxUsableProduceMagic() {
        val apiVersions = ApiVersions()
        assertEquals(
            expected = RecordBatch.CURRENT_MAGIC_VALUE,
            actual = apiVersions.maxUsableProduceMagic(),
        )
        apiVersions.update("0", NodeApiVersions.create())
        assertEquals(
            expected = RecordBatch.CURRENT_MAGIC_VALUE,
            actual = apiVersions.maxUsableProduceMagic(),
        )
        apiVersions.update(
            nodeId = "1",
            nodeApiVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, 0.toShort(), 2.toShort())
        )
        assertEquals(
            expected = RecordBatch.MAGIC_VALUE_V1,
            actual = apiVersions.maxUsableProduceMagic(),
        )
        apiVersions.remove("1")
        assertEquals(
            expected = RecordBatch.CURRENT_MAGIC_VALUE,
            actual = apiVersions.maxUsableProduceMagic(),
        )
    }

    @Test
    fun testMaxUsableProduceMagicWithRaftController() {
        val apiVersions = ApiVersions()
        assertEquals(
            expected = RecordBatch.CURRENT_MAGIC_VALUE,
            actual = apiVersions.maxUsableProduceMagic(),
        )

        // something that doesn't support PRODUCE, which is the case with Raft-based controllers
        apiVersions.update(
            nodeId = "2",
            nodeApiVersions = NodeApiVersions.create(
                setOf(
                    ApiVersionsResponseData.ApiVersion()
                        .setApiKey(ApiKeys.FETCH.id)
                        .setMinVersion(0.toShort())
                        .setMaxVersion(2.toShort())
                )
            )
        )
        assertEquals(
            expected = RecordBatch.CURRENT_MAGIC_VALUE,
            actual = apiVersions.maxUsableProduceMagic(),
        )
    }
}
