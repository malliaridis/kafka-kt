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
import org.apache.kafka.common.annotation.InterfaceStability.Evolving

/**
 * The result of the [Admin.listTopics] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class ListTopicsResult internal constructor(val future: KafkaFuture<Map<String, TopicListing>>) {

    /**
     * Return a future which yields a map of topic names to TopicListing objects.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("namesToListings"),
    )
    fun namesToListings(): KafkaFuture<Map<String, TopicListing>> = future

    /**
     * A future which yields a map of topic names to TopicListing objects.
     */
    val namesToListings: KafkaFuture<Map<String, TopicListing>> = future

    /**
     * Return a future which yields a collection of TopicListing objects.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("listings"),
    )
    fun listings(): KafkaFuture<Collection<TopicListing>> =
        future.thenApply { namesToDescriptions -> namesToDescriptions.values }

    /**
     * A future which yields a collection of TopicListing objects.
     */
    val listings: KafkaFuture<Collection<TopicListing>> =
        future.thenApply { namesToDescriptions -> namesToDescriptions.values }

    /**
     * Return a future which yields a collection of topic names.
     */
    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("names"),
    )
    fun names(): KafkaFuture<Set<String>> =
        future.thenApply { namesToListings -> namesToListings.keys }

    /**
     * A future which yields a collection of topic names.
     */
    val names: KafkaFuture<Set<String>> =
        future.thenApply { namesToListings -> namesToListings.keys }
}
