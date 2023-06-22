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

package org.apache.kafka.common.requests

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @property sessionId The fetch session ID.
 * @property epoch The fetch session epoch.
 */
data class FetchMetadata(
    val sessionId: Int,
    val epoch: Int
) {

    /**
     * Returns true if this is a full fetch request.
     */
    val isFull: Boolean
        get() = epoch == INITIAL_EPOCH || epoch == FINAL_EPOCH

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("sessionId"),
    )
    fun sessionId(): Int = sessionId

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("epoch"),
    )
    fun epoch(): Int = epoch

    /**
     * Return the metadata for the next error response.
     */
    fun nextCloseExisting(): FetchMetadata = FetchMetadata(sessionId, INITIAL_EPOCH)

    /**
     * Return the metadata for the next incremental response.
     */
    fun nextIncremental(): FetchMetadata = FetchMetadata(sessionId, nextEpoch(epoch))

    override fun toString(): String {
        val bld = StringBuilder()
        if (sessionId == INVALID_SESSION_ID) bld.append("(sessionId=INVALID, ")
        else bld.append("(sessionId=").append(sessionId).append(", ")
        when (epoch) {
            INITIAL_EPOCH -> bld.append("epoch=INITIAL)")
            FINAL_EPOCH -> bld.append("epoch=FINAL)")
            else -> bld.append("epoch=").append(epoch).append(")")
        }
        return bld.toString()
    }

    companion object {

        val log: Logger = LoggerFactory.getLogger(FetchMetadata::class.java)

        /**
         * The session ID used by clients with no session.
         */
        const val INVALID_SESSION_ID = 0

        /**
         * The first epoch.  When used in a fetch request, indicates that the client wants to
         * create or recreate a session.
         */
        const val INITIAL_EPOCH = 0

        /**
         * An invalid epoch.  When used in a fetch request, indicates that the client wants to close
         * any existing session, and not create a new one.
         */
        const val FINAL_EPOCH = -1

        /**
         * The FetchMetadata that is used when initializing a new FetchSessionHandler.
         */
        val INITIAL = FetchMetadata(INVALID_SESSION_ID, INITIAL_EPOCH)

        /**
         * The FetchMetadata that is implicitly used for handling older FetchRequests that don't
         * include fetch metadata.
         */
        val LEGACY = FetchMetadata(INVALID_SESSION_ID, FINAL_EPOCH)

        /**
         * Returns the next epoch.
         *
         * @param prevEpoch The previous epoch.
         * @return The next epoch.
         */
        fun nextEpoch(prevEpoch: Int): Int {
            // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
            return if (prevEpoch < 0) FINAL_EPOCH
            else if (prevEpoch == Int.MAX_VALUE) 1
            else prevEpoch + 1
        }

        /**
         * Return the metadata for the next full fetch request.
         */
        fun newIncremental(sessionId: Int): FetchMetadata =
            FetchMetadata(sessionId, nextEpoch(INITIAL_EPOCH))
    }
}
