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

package org.apache.kafka.clients.consumer.internals

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

/**
 * This class manages the fetching process with the brokers.
 *
 * Thread-safety:
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 *
 * - If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 * all access to that state must be synchronized on the Fetcher instance.
 * - If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 * it is assumed that all access to that state is synchronized on the coordinator instance by
 * the caller.
 * - At most one request is pending for each node at any time. Nodes with pending requests are
 * tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 * updated while processing responses on one thread are visible while creating the subsequent request
 * on a different thread.
 */
open class Fetcher<K, V>(
    logContext: LogContext,
    client: ConsumerNetworkClient,
    metadata: ConsumerMetadata,
    subscriptions: SubscriptionState,
    fetchConfig: FetchConfig<K, V>,
    metricsManager: FetchMetricsManager,
    time: Time,
) : AbstractFetch<K, V>(
    logContext = logContext,
    client = client,
    metadata = metadata,
    subscriptions = subscriptions,
    fetchConfig = fetchConfig,
    metricsManager = metricsManager,
    time = time,
) {

    private val log: Logger = logContext.logger(Fetcher::class.java)

    private val isClosed = AtomicBoolean(false)

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     *
     * @return number of fetches sent
     */
    @Synchronized
    fun sendFetches(): Int {
        val fetchRequestMap = prepareFetchRequests()
        for ((fetchTarget, data) in fetchRequestMap) {
            val request = createFetchRequest(fetchTarget, data)
            val listener = object : RequestFutureListener<ClientResponse> {

                override fun onSuccess(result: ClientResponse) = synchronized(this@Fetcher) {
                    handleFetchResponse(fetchTarget, data, result)
                }

                override fun onFailure(exception: RuntimeException) = synchronized(this@Fetcher) {
                    handleFetchResponse(fetchTarget, exception)
                }
            }
            val future = client.send(fetchTarget, request)
            future.addListener(listener)
        }
        return fetchRequestMap.size
    }

    override fun close(timer: Timer) {
        if (!isClosed.compareAndSet(false, true)) {
            log.info("Fetcher {} is already closed.", this)
            return
        }

        // Shared states (e.g. sessionHandlers) could be accessed by multiple threads (such as heartbeat thread), hence,
        // it is necessary to acquire a lock on the fetcher instance before modifying the states.
        synchronized(this) {
            super.close(timer)
        }
    }
}
