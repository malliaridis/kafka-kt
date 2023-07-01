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

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.KafkaFuture.BaseFunction
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.internals.KafkaFutureImpl

/**
 * The result of the [Admin.listConsumerGroups] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ListConsumerGroupsResult internal constructor(future: KafkaFuture<Collection<Any>>) {
    val all: KafkaFutureImpl<Collection<ConsumerGroupListing>> = KafkaFutureImpl()
    val valid: KafkaFutureImpl<Collection<ConsumerGroupListing>> = KafkaFutureImpl()
    val errors: KafkaFutureImpl<Collection<Throwable>> = KafkaFutureImpl()

    init {
        future.thenApply { results ->
            val curErrors = ArrayList<Throwable>()
            val curValid = ArrayList<ConsumerGroupListing>()
            for (resultObject in results) {
                if (resultObject is Throwable) curErrors.add(resultObject)
                else curValid.add(resultObject as ConsumerGroupListing)
            }
            if (curErrors.isNotEmpty()) all.completeExceptionally(curErrors[0])
            else all.complete(curValid)

            valid.complete(curValid)
            errors.complete(curErrors)
        }
    }

    /**
     * Returns a future that yields either an exception, or the full set of consumer group listings.
     *
     * In the event of a failure, the future yields nothing but the first exception which occurred.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("all"),
    )
    fun all(): KafkaFuture<Collection<ConsumerGroupListing>> = all

    /**
     * Returns a future which yields just the valid listings.
     *
     * This future never fails with an error, no matter what happens. Errors are completely
     * ignored. If nothing can be fetched, an empty collection is yielded. If there is an error, but
     * some results can be returned, this future will yield those partial results. When using this
     * future, it is a good idea to also check the errors future so that errors can be displayed and
     * handled.
     */
    fun valid(): KafkaFuture<Collection<ConsumerGroupListing>> = valid

    /**
     * Returns a future which yields just the errors which occurred.
     *
     * If this future yields a non-empty collection, it is very likely that elements are missing
     * from the `valid()` set.
     *
     * This future itself never fails with an error. In the event of an error, this future will
     * successfully yield a collection containing at least one exception.
     */
    fun errors(): KafkaFuture<Collection<Throwable>> = errors
}
