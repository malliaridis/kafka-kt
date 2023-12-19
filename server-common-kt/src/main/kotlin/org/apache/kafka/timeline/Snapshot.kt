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

package org.apache.kafka.timeline

import java.util.IdentityHashMap

/**
 * A snapshot of some timeline data structures.
 *
 * The snapshot contains historical data for several timeline data structures. We use an [IdentityHashMap]
 * to store this data. This way, we can easily drop all the snapshot data.
 */
class Snapshot internal constructor(val epoch: Long) {

    private var map: IdentityHashMap<Revertable, Delta>? = IdentityHashMap(4)

    private var prev = this

    private var next = this

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("epoch"),
    )
    fun epoch(): Long = epoch

    fun <T : Delta?> getDelta(owner: Revertable): T? {
        return map!![owner] as T?
    }

    fun setDelta(owner: Revertable, delta: Delta) {
        map!![owner] = delta
    }

    fun handleRevert() {
        for ((key, value) in map!!)
            key.executeRevert(epoch, value)
    }

    fun mergeFrom(source: Snapshot) {
        // Merge the deltas from the source snapshot into this snapshot.
        for ((key, value) in source.map!!) {
            // We first try to just copy over the object reference.  That will work if
            //we have no entry at all for the given Revertable.
            val destinationDelta = map!!.putIfAbsent(key, value)
            destinationDelta?.mergeFrom(epoch, value)
        }
        // Delete the source snapshot to make sure nobody tries to reuse it.  We might now
        // share some delta entries with it.
        source.erase()
    }

    fun prev(): Snapshot = prev

    operator fun next(): Snapshot = next

    fun appendNext(newNext: Snapshot) {
        newNext.prev = this
        newNext.next = next
        next.prev = newNext
        next = newNext
    }

    fun erase() {
        map = null
        next.prev = prev
        prev.next = next
        prev = this
        next = this
    }
}
