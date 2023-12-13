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

package org.apache.kafka.clients.admin.internals

import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse

/**
 * This lookup strategy is used when we already know the destination broker ID, and we have no need
 * for an explicit lookup. By setting [ApiRequestScope.destinationBrokerId] in the returned value
 * for [lookupScope], the driver will skip the lookup.
 */
class StaticBrokerStrategy<K>(brokerId: Int) : AdminApiLookupStrategy<K> {

    private val scope: SingleBrokerScope = SingleBrokerScope(brokerId)

    override fun lookupScope(key: K): ApiRequestScope = scope

    override fun buildRequest(keys: Set<K>): AbstractRequest.Builder<*> =
        throw UnsupportedOperationException()

    override fun handleResponse(
        keys: Set<K>,
        response: AbstractResponse,
    ): AdminApiLookupStrategy.LookupResult<K> = throw UnsupportedOperationException()

    private class SingleBrokerScope(private val brokerId: Int?) : ApiRequestScope {
        override fun destinationBrokerId(): Int? = brokerId
    }
}
