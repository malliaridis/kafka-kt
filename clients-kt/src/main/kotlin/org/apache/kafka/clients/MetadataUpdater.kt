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

import java.io.Closeable
import java.util.*
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.RequestHeader

/**
 * The interface used by `NetworkClient` to request cluster metadata info to be updated and to retrieve the cluster
 * nodes from such metadata. This is an internal class.
 *
 *
 * This class is not thread-safe!
 */
interface MetadataUpdater : Closeable {
    /**
     * Gets the current cluster info without blocking.
     */
    fun fetchNodes(): List<Node?>

    /**
     * Returns true if an update to the cluster metadata info is due.
     */
    fun isUpdateDue(now: Long): Boolean

    /**
     * Starts a cluster metadata update if needed and possible. Returns the time until the metadata update (which would
     * be 0 if an update has been started as a result of this call).
     *
     * If the implementation relies on `NetworkClient` to send requests, `handleSuccessfulResponse` will be
     * invoked after the metadata response is received.
     *
     * The semantics of `needed` and `possible` are implementation-dependent and may take into account a number of
     * factors like node availability, how long since the last metadata update, etc.
     */
    fun maybeUpdate(now: Long): Long

    /**
     * Handle a server disconnect.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     *
     * @param now Current time in milliseconds
     * @param nodeId The id of the node that disconnected
     * @param maybeAuthException Optional authentication error
     */
    fun handleServerDisconnect(now: Long, nodeId: String?, maybeAuthException: AuthenticationException?)

    /**
     * Handle a metadata request failure.
     *
     * @param now Current time in milliseconds
     * @param maybeFatalException Optional fatal error (e.g. [UnsupportedVersionException])
     */
    fun handleFailedRequest(now: Long, maybeFatalException: KafkaException?)

    /**
     * Handle responses for metadata requests.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     */
    fun handleSuccessfulResponse(
        requestHeader: RequestHeader?,
        now: Long,
        response: MetadataResponse,
    )

    /**
     * Close this updater.
     */
    override fun close()
}
