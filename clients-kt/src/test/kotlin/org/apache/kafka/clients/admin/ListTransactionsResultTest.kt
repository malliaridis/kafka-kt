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

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ListTransactionsResultTest {
    
    private val future = KafkaFutureImpl<Map<Int, KafkaFutureImpl<Collection<TransactionListing>>>>()
    
    private val result = ListTransactionsResult(future)
    
    @Test
    fun testAllFuturesFailIfLookupFails() {
        future.completeExceptionally(KafkaException())
        assertFutureThrows(result.all(), KafkaException::class.java)
        assertFutureThrows(result.allByBrokerId(), KafkaException::class.java)
        assertFutureThrows(result.byBrokerId(), KafkaException::class.java)
    }

    @Test
    @Throws(Exception::class)
    fun testAllFuturesSucceed() {
        val future1 = KafkaFutureImpl<Collection<TransactionListing>>()
        val future2 = KafkaFutureImpl<Collection<TransactionListing>>()
        val brokerFutures = mutableMapOf<Int, KafkaFutureImpl<Collection<TransactionListing>>>()
        brokerFutures[1] = future1
        brokerFutures[2] = future2
        future.complete(brokerFutures)
        val broker1Listings = listOf(
            TransactionListing(transactionalId = "foo", producerId = 12345L, state = TransactionState.ONGOING),
            TransactionListing(transactionalId = "bar", producerId = 98765L, state = TransactionState.PREPARE_ABORT),
        )
        future1.complete(broker1Listings)
        val broker2Listings = listOf(
            TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
        )
        future2.complete(broker2Listings)
        val resultBrokerFutures = result.byBrokerId().get()
        assertEquals(setOf(1, 2), resultBrokerFutures.keys)
        assertEquals(broker1Listings, resultBrokerFutures[1]!!.get())
        assertEquals(broker2Listings, resultBrokerFutures[2]!!.get())
        assertEquals(broker1Listings, result.allByBrokerId().get()[1])
        assertEquals(broker2Listings, result.allByBrokerId().get()[2])
        val allExpected: MutableSet<TransactionListing> = HashSet()
        allExpected.addAll(broker1Listings)
        allExpected.addAll(broker2Listings)
        assertEquals(allExpected, HashSet(result.all().get()))
    }

    @Test
    @Throws(Exception::class)
    fun testPartialFailure() {
        val future1 = KafkaFutureImpl<Collection<TransactionListing>>()
        val future2 = KafkaFutureImpl<Collection<TransactionListing>>()
        val brokerFutures = mutableMapOf<Int, KafkaFutureImpl<Collection<TransactionListing>>>()
        brokerFutures[1] = future1
        brokerFutures[2] = future2
        future.complete(brokerFutures)
        val broker1Listings = listOf(
            TransactionListing("foo", 12345L, TransactionState.ONGOING),
            TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT)
        )
        future1.complete(broker1Listings)
        future2.completeExceptionally(KafkaException())
        val resultBrokerFutures = result.byBrokerId().get()

        // Ensure that the future for broker 1 completes successfully
        assertEquals(setOf(1, 2), resultBrokerFutures.keys)
        assertEquals(broker1Listings, resultBrokerFutures[1]!!.get())

        // Everything else should fail
        assertFutureThrows(result.all(), KafkaException::class.java)
        assertFutureThrows(result.allByBrokerId(), KafkaException::class.java)
        assertFutureThrows(resultBrokerFutures[2]!!, KafkaException::class.java)
    }
}
