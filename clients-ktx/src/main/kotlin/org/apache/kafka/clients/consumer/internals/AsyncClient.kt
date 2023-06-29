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

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

abstract class AsyncClient<T1, Req : AbstractRequest, Resp : AbstractResponse, T2> internal constructor(
    private val client: ConsumerNetworkClient, logContext: LogContext
) {
    private val log: Logger = logContext.logger(javaClass)

    fun sendAsyncRequest(node: Node, requestData: T1): RequestFuture<T2> {
        val requestBuilder = prepareRequest(node, requestData)
        return client.send(node, requestBuilder).compose(
            object : RequestFutureAdapter<ClientResponse, T2>() {
                override fun onSuccess(value: ClientResponse, future: RequestFuture<T2>) {
                    val resp: Resp = try {
                        value.responseBody as Resp
                    } catch (cce: ClassCastException) {
                        log.error("Could not cast response body", cce)
                        future.raise(cce)
                        return
                    }
                    log.trace(
                        "Received {} {} from broker {}",
                        resp.javaClass.simpleName,
                        resp,
                        node,
                    )
                    try {
                        future.complete(handleResponse(node, requestData, resp))
                    } catch (e: RuntimeException) {
                        if (!future.isDone) future.raise(e)
                    }
                }

                override fun onFailure(exception: RuntimeException, future: RequestFuture<T2>) {
                    future.raise(exception)
                }
            }
        )
    }

    protected fun logger(): Logger = log

    protected abstract fun prepareRequest(node: Node, equestData: T1): AbstractRequest.Builder<Req>

    protected abstract fun handleResponse(node: Node, requestData: T1, response: Resp?): T2
}
