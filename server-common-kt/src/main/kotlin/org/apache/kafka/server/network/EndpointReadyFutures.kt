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

package org.apache.kafka.server.network

import java.util.TreeSet
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.authorizer.AuthorizerServerInfo
import org.slf4j.Logger

/**
 * Manages a set of per-endpoint futures.
 */
class EndpointReadyFutures private constructor(
    logContext: LogContext,
    endpointStages: Map<Endpoint, MutableList<EndpointCompletionStage>>,
) {

    private val log: Logger = logContext.logger(EndpointReadyFutures::class.java)

    val futures: Map<Endpoint, CompletableFuture<Unit>>

    init {
        val newFutures = mutableMapOf<Endpoint, CompletableFuture<Unit>>()
        endpointStages.forEach(action = { (endpoint, stages) ->
            val stageNames = mutableListOf<String>()
            stages.forEach { stage -> stageNames.add(stage.name) }
            val readyFuture = EndpointReadyFuture(endpoint, stageNames)
            newFutures[endpoint] = readyFuture.future
            stages.forEach { stage ->
                stage.future.whenComplete { _, exception ->
                    if (exception != null) readyFuture.failStage(stage.name, exception)
                    else readyFuture.completeStage(stage.name)
                }
            }
        })
        futures = newFutures
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("futures"),
    )
    fun futures(): Map<Endpoint, CompletableFuture<Unit>> = futures

    class Builder {

        private var logContext: LogContext? = null

        private val endpointStages: MutableMap<Endpoint, MutableList<EndpointCompletionStage>> = HashMap()

        private val stages: MutableList<EndpointCompletionStage> = ArrayList()

        /**
         * Add a readiness future that will block all endpoints.
         *
         * @param name The future name.
         * @param future The future object.
         *
         * @return This builder object.
         */
        fun addReadinessFuture(
            name: String,
            future: CompletableFuture<Unit>,
        ): Builder {
            stages.add(EndpointCompletionStage(name, future))
            return this
        }

        /**
         * Add readiness futures for individual endpoints.
         *
         * @param name The future name.
         * @param newFutures A map from endpoints to futures.
         *
         * @return This builder object.
         */
        fun addReadinessFutures(
            name: String,
            newFutures: Map<Endpoint, CompletionStage<Unit>>,
        ): Builder {
            newFutures.forEach { (endpoint, future) ->
                endpointStages.computeIfAbsent(endpoint) { ArrayList() }
                    .add(EndpointCompletionStage(name, future))
            }
            return this
        }

        /**
         * Build the EndpointReadyFutures object.
         *
         * @param authorizer The authorizer to use, if any. Will be started.
         * @param info Server information to be passed to the authorizer.
         *
         * @return The new futures object.
         */
        fun build(
            authorizer: Authorizer?,
            info: AuthorizerServerInfo,
        ): EndpointReadyFutures {
            return if (authorizer != null) build(authorizer.start(info), info)
            else build(emptyMap(), info)
        }

        fun build(
            authorizerStartFutures: Map<Endpoint, CompletionStage<Unit>>,
            info: AuthorizerServerInfo,
        ): EndpointReadyFutures {
            if (logContext == null) logContext = LogContext()
            val effectiveStartFutures = authorizerStartFutures.toMutableMap()
            for (endpoint in info.endpoints()) {
                if (!effectiveStartFutures.containsKey(endpoint)) {
                    val completedFuture = CompletableFuture.completedFuture(Unit)
                    effectiveStartFutures[endpoint] = completedFuture
                }
            }
            if (info.endpoints().size != effectiveStartFutures.size) {
                val notInInfo = mutableListOf<String>()
                for (endpoint in effectiveStartFutures.keys) {
                    if (!info.endpoints().contains(endpoint))
                        notInInfo.add(endpoint.listenerName ?: "[none]")
                }
                throw RuntimeException(
                    "Found authorizer futures that weren't included in AuthorizerServerInfo: $notInInfo"
                )
            }
            addReadinessFutures("authorizerStart", effectiveStartFutures)
            stages.forEach { stage ->
                val newReadinessFutures = mutableMapOf<Endpoint, CompletionStage<Unit>>()
                info.endpoints() .forEach { endpoint -> newReadinessFutures[endpoint] = stage.future }
                addReadinessFutures(stage.name, newReadinessFutures)
            }
            return EndpointReadyFutures(
                logContext!!,
                endpointStages
            )
        }
    }

    internal class EndpointCompletionStage(val name: String, val future: CompletionStage<Unit>)

    internal inner class EndpointReadyFuture(endpoint: Endpoint, stageNames: Collection<String>) {

        val endpointName: String = endpoint.listenerName ?: "UNNAMED"

        val incomplete: TreeSet<String> = TreeSet(stageNames)

        val future: CompletableFuture<Unit> = CompletableFuture()

        fun completeStage(stageName: String?) {
            var done = false
            synchronized(this@EndpointReadyFuture) {
                if (incomplete.remove(stageName)) {
                    if (incomplete.isEmpty()) {
                        done = true
                    } else log.info(
                        "{} completed for endpoint {}. Still waiting for {}.",
                        stageName, endpointName, incomplete
                    )
                }
            }
            if (done) {
                if (future.complete(null)) log.info(
                    "{} completed for endpoint {}. Endpoint is now READY.",
                    stageName, endpointName
                )
            }
        }

        fun failStage(what: String, exception: Throwable) {
            if (future.completeExceptionally(exception)) {
                synchronized(this@EndpointReadyFuture) { incomplete.clear() }
                log.warn(
                    "Endpoint {} will never become ready because we encountered an {} exception",
                    endpointName, what, exception
                )
            }
        }
    }
}
