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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DeleteTopicsResultTest {

    @Test
    fun testDeleteTopicsResultWithNames() {
        val future = KafkaFutureImpl<Unit>()
        future.complete(Unit)
        val topicNames = mapOf("foo" to future)
        val topicNameFutures = DeleteTopicsResult.ofTopicNames(topicNames)
        assertEquals(topicNames, topicNameFutures.nameFutures)
        assertNull(topicNameFutures.topicIdFutures)
        assertTrue(topicNameFutures.all().isDone())
    }

    @Test
    fun testDeleteTopicsResultWithIds() {
        val future = KafkaFutureImpl<Unit>()
        future.complete(Unit)
        val topicIds = mapOf(Uuid.randomUuid() to future)
        val topicIdFutures = DeleteTopicsResult.ofTopicIds(topicIds)
        assertEquals(topicIds, topicIdFutures.topicIdFutures)
        assertNull(topicIdFutures.nameFutures)
        assertTrue(topicIdFutures.all().isDone())
    }

    @Test
    fun testInvalidConfigurations() {
        assertFailsWith<IllegalArgumentException> {
            DeleteTopicsResult(topicIdFutures = null, nameFutures = null)
        }
        assertFailsWith<IllegalArgumentException> {
            DeleteTopicsResult(topicIdFutures = emptyMap(), nameFutures = emptyMap())
        }
    }
}
