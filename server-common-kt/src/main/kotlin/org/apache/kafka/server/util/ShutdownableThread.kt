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

package org.apache.kafka.server.util

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.Exit.exit
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger
import kotlin.concurrent.Volatile

abstract class ShutdownableThread(
    name: String,
    private val isInterruptible: Boolean = true,
    val logPrefix: String = "[$name]: ",
) : Thread(name) {

    protected val log: Logger = LogContext(logPrefix).logger(this.javaClass)

    private val shutdownInitiated = CountDownLatch(1)

    private val shutdownComplete = CountDownLatch(1)

    @Volatile
    private var isStarted = false

    init {
        setDaemon(false)
    }

    @Throws(InterruptedException::class)
    open fun shutdown() {
        initiateShutdown()
        awaitShutdown()
    }

    val isShutdownInitiated: Boolean
        get() = shutdownInitiated.count == 0L

    val isShutdownComplete: Boolean
        get() = shutdownComplete.count == 0L

    val isRunning: Boolean
        get() = !isShutdownInitiated

    /**
     * `true` if there has been an unexpected error and the thread shut down
     */
    val isThreadFailed: Boolean
        get() = isShutdownComplete && !isShutdownInitiated

    /**
     * @return true if the thread hasn't initiated shutdown already
     */
    fun initiateShutdown(): Boolean {
        synchronized(this) {
            return if (isRunning) {
                log.info("Shutting down")
                shutdownInitiated.countDown()
                if (isInterruptible) interrupt()
                true
            } else false
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete.
     */
    @Throws(InterruptedException::class)
    fun awaitShutdown() {
        check(isShutdownInitiated) { "initiateShutdown was not called before awaitShutdown()" }
        if (isStarted) shutdownComplete.await()
        log.info("Shutdown completed")
    }

    /**
     * Causes the current thread to wait until the shutdown is initiated,
     * or the specified waiting time elapses.
     *
     * @param timeout wait time in units.
     * @param unit TimeUnit value for the wait time.
     */
    @Throws(InterruptedException::class)
    fun pause(timeout: Long, unit: TimeUnit?) {
        if (shutdownInitiated.await(timeout, unit))
            log.trace("shutdownInitiated latch count reached zero. Shutdown called.")
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    abstract fun doWork()

    override fun run() {
        isStarted = true
        log.info("Starting")
        try {
            while (isRunning) doWork()
        } catch (e: FatalExitError) {
            shutdownInitiated.countDown()
            shutdownComplete.countDown()
            log.info("Stopped")
            exit(e.statusCode)
        } catch (e: Throwable) {
            if (isRunning) log.error("Error due to", e)
        } finally {
            shutdownComplete.countDown()
        }
        log.info("Stopped")
    }
}
