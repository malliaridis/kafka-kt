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

import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.slf4j.Logger

/**
 * A helper class for managing the heartbeat to the coordinator
 */
class Heartbeat(
    private val rebalanceConfig: GroupRebalanceConfig,
    private val time: Time,
) {

    private val maxPollIntervalMs: Int = rebalanceConfig.rebalanceTimeoutMs

    private val heartbeatTimer: Timer = time.timer(rebalanceConfig.heartbeatIntervalMs.toLong())

    private val sessionTimer: Timer = time.timer(rebalanceConfig.sessionTimeoutMs.toLong())

    private val pollTimer: Timer = time.timer(maxPollIntervalMs.toLong())

    private val log: Logger = LogContext(
        logPrefix = "[Heartbeat groupID=${rebalanceConfig.groupId}] ",
    ).logger(javaClass)

    @Volatile
    var lastHeartbeatSend = 0L
        private set

    @Volatile
    private var heartbeatInFlight = false

    init {
        require(rebalanceConfig.heartbeatIntervalMs < rebalanceConfig.sessionTimeoutMs) {
            "Heartbeat must be set lower than the session timeout"
        }
    }

    private fun update(now: Long) {
        heartbeatTimer.update(now)
        sessionTimer.update(now)
        pollTimer.update(now)
    }

    fun poll(now: Long) {
        update(now)
        pollTimer.reset(maxPollIntervalMs.toLong())
    }

    fun hasInflight(): Boolean = heartbeatInFlight

    fun sentHeartbeat(now: Long) {
        lastHeartbeatSend = now
        heartbeatInFlight = true
        update(now)
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs.toLong())

        if (log.isTraceEnabled) log.trace(
            "Sending heartbeat request with {}ms remaining on timer",
            heartbeatTimer.remainingMs,
        )
    }

    fun failHeartbeat() {
        update(time.milliseconds())
        heartbeatInFlight = false
        heartbeatTimer.reset(rebalanceConfig.retryBackoffMs)

        log.trace(
            "Heartbeat failed, reset the timer to {}ms remaining",
            heartbeatTimer.remainingMs,
        )
    }

    fun receiveHeartbeat() {
        update(time.milliseconds())
        heartbeatInFlight = false
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs.toLong())
    }

    fun shouldHeartbeat(now: Long): Boolean {
        update(now)
        return heartbeatTimer.isExpired
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("lastHeartbeatSend"),
    )
    fun lastHeartbeatSend(): Long = lastHeartbeatSend

    fun timeToNextHeartbeat(now: Long): Long {
        update(now)
        return heartbeatTimer.remainingMs
    }

    fun sessionTimeoutExpired(now: Long): Boolean {
        update(now)
        return sessionTimer.isExpired
    }

    fun resetTimeouts() {
        update(time.milliseconds())
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs.toLong())
        pollTimer.reset(maxPollIntervalMs.toLong())
        heartbeatTimer.reset(rebalanceConfig.heartbeatIntervalMs.toLong())
    }

    fun resetSessionTimeout() {
        update(time.milliseconds())
        sessionTimer.reset(rebalanceConfig.sessionTimeoutMs.toLong())
    }

    fun pollTimeoutExpired(now: Long): Boolean {
        update(now)
        return pollTimer.isExpired
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("lastPollTime"),
    )
    fun lastPollTime(): Long = pollTimer.currentTimeMs

    val lastPollTime: Long
        get() = pollTimer.currentTimeMs
}
