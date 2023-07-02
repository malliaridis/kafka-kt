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

package org.apache.kafka.test

import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.network.ChannelState
import org.apache.kafka.common.network.NetworkReceive
import org.apache.kafka.common.network.NetworkSend
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.requests.ByteBufferChannel
import org.apache.kafka.common.utils.Time
import java.io.IOException
import java.net.InetSocketAddress
import java.util.function.Predicate

/**
 * A fake selector to use for testing
 */
class MockSelector @JvmOverloads constructor(
    private val time: Time,
    private val canConnect: Predicate<InetSocketAddress>? = null,
) : Selectable {

    private val initiatedSends: MutableList<NetworkSend> = ArrayList()

    private val completedSends: MutableList<NetworkSend> = ArrayList()

    private val completedSendBuffers: MutableList<ByteBufferChannel> = ArrayList()

    private val completedReceives: MutableList<NetworkReceive> = ArrayList()

    private val disconnected: MutableMap<String, ChannelState> = HashMap()

    private val connected: MutableList<String> = ArrayList()

    private val delayedReceives: MutableList<DelayedReceive> = ArrayList()

    private val ready: MutableSet<String> = HashSet()

    @Throws(IOException::class)
    override fun connect(
        id: String,
        address: InetSocketAddress,
        sendBufferSize: Int,
        receiveBufferSize: Int
    ) {
        if (canConnect == null || canConnect.test(address)) {
            connected.add(id)
            ready.add(id)
        }
    }

    override fun wakeup() = Unit

    override fun close() = Unit

    override fun close(id: String) {
        // Note that there are no notifications for client-side disconnects
        removeSendsForNode(id, completedSends)
        removeSendsForNode(id, initiatedSends)
        ready.remove(id)
        for (i in connected.indices) if (connected[i] == id) {
            connected.removeAt(i)
            break
        }
    }

    /**
     * Since MockSelector.connect will always succeed and add the connection id to the Set
     * connected, we can only simulate that the connection is still pending by removing the
     * connection id from the Set connected.
     *
     * @param id connection id
     */
    fun serverConnectionBlocked(id: String) {
        connected.remove(id)
    }

    /**
     * Simulate a server disconnect. This id will be present in [.disconnected] on
     * the next [.poll].
     */
    fun serverDisconnect(id: String) {
        disconnected[id] = ChannelState.READY
        close(id)
    }

    fun serverAuthenticationFailed(id: String) {
        val authFailed = ChannelState(
            state = ChannelState.State.AUTHENTICATION_FAILED,
            exception = AuthenticationException("Authentication failed"),
        )
        disconnected[id] = authFailed
        close(id)
    }

    private fun removeSendsForNode(id: String, sends: MutableCollection<NetworkSend>) {
        sends.removeIf { send -> id == send.destinationId }
    }

    fun clear() {
        completedSends.clear()
        completedReceives.clear()
        completedSendBuffers.clear()
        disconnected.clear()
        connected.clear()
    }

    override fun send(send: NetworkSend) {
        initiatedSends.add(send)
    }

    @Throws(IOException::class)
    override fun poll(timeout: Long) {
        completeInitiatedSends()
        completeDelayedReceives()
        time.sleep(timeout)
    }

    @Throws(IOException::class)
    private fun completeInitiatedSends() {
        for (send in initiatedSends) completeSend(send)
        initiatedSends.clear()
    }

    @Throws(IOException::class)
    private fun completeSend(send: NetworkSend) {
        // Consume the send so that we will be able to send more requests to the destination
        ByteBufferChannel(send.size()).use { discardChannel ->
            while (!send.completed()) send.writeTo(discardChannel)

            completedSends.add(send)
            completedSendBuffers.add(discardChannel)
        }
    }

    private fun completeDelayedReceives() {
        for (completedSend in completedSends) {
            val delayedReceiveIterator = delayedReceives.iterator()

            while (delayedReceiveIterator.hasNext()) {
                val delayedReceive = delayedReceiveIterator.next()
                if (delayedReceive.source == completedSend.destinationId) {
                    completedReceives.add(delayedReceive.receive)
                    delayedReceiveIterator.remove()
                }
            }
        }
    }

    override fun completedSends(): List<NetworkSend> = completedSends

    fun completedSendBuffers(): List<ByteBufferChannel> = completedSendBuffers

    override fun completedReceives(): List<NetworkReceive> = completedReceives

    fun completeReceive(receive: NetworkReceive) {
        completedReceives.add(receive)
    }

    fun delayedReceive(receive: DelayedReceive) {
        delayedReceives.add(receive)
    }

    override fun disconnected(): Map<String, ChannelState> {
        return disconnected
    }

    override fun connected(): List<String> {
        val currentConnected: List<String> = ArrayList(connected)
        connected.clear()
        return currentConnected
    }

    override fun mute(id: String) = Unit

    override fun unmute(id: String) = Unit

    override fun muteAll() = Unit

    override fun unmuteAll() = Unit

    fun channelNotReady(id: String) {
        ready.remove(id)
    }

    override fun isChannelReady(id: String): Boolean = ready.contains(id)

    fun reset() {
        clear()
        initiatedSends.clear()
        delayedReceives.clear()
    }
}
