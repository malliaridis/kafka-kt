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

package org.apache.kafka.common.memory

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.utils.Utils.formatBytes

/**
 * An extension of SimpleMemoryPool that tracks allocated buffers and logs an error when they "leak"
 * (when they are garbage-collected without having been release()ed).
 * THIS IMPLEMENTATION IS A DEVELOPMENT/DEBUGGING AID AND IS NOT MEANT PRO PRODUCTION USE.
 */
class GarbageCollectedMemoryPool(
    sizeBytes: Long,
    maxSingleAllocationSize: Int,
    strict: Boolean,
    oomPeriodSensor: Sensor?
) : SimpleMemoryPool(
    sizeInBytes = sizeBytes,
    maxSingleAllocationBytes = maxSingleAllocationSize,
    strict = strict,
    oomPeriodSensor = oomPeriodSensor,
), AutoCloseable {

    private val garbageCollectedBuffers = ReferenceQueue<ByteBuffer>()

    //serves 2 purposes - 1st it maintains the ref objects reachable (which is a requirement for them
    //to ever be enqueued), 2nd keeps some (small) metadata for every buffer allocated
    private val buffersInFlight: MutableMap<BufferReference, BufferMetadata?> = ConcurrentHashMap()

    private val gcListener = GarbageCollectionListener()

    private val gcListenerThread: Thread

    @Volatile
    private var alive = true

    init {
        alive = true
        gcListenerThread = Thread(gcListener, "memory pool GC listener")
        gcListenerThread.isDaemon = true //so we dont need to worry about shutdown
        gcListenerThread.start()
    }

    override fun bufferToBeReturned(justAllocated: ByteBuffer) {
        val ref = BufferReference(justAllocated, garbageCollectedBuffers)
        val metadata = BufferMetadata(justAllocated.capacity())
        check(buffersInFlight.put(ref, metadata) == null) {
            //this is a bug. it means either 2 different co-existing buffers got
            //the same identity or we failed to register a released/GC'ed buffer
            "allocated buffer identity " + ref.hashCode + " already registered as in use?!"
        }
        log.trace("allocated buffer of size {} and identity {}", sizeBytes, ref.hashCode)
    }

    override fun bufferToBeReleased(justReleased: ByteBuffer) {
        val ref = BufferReference(justReleased) //used ro lookup only

        val metadata = buffersInFlight.remove(ref)
            ?: //its impossible for the buffer to have already been GC'ed (because we have a hard ref to it
            //in the function arg) so this means either a double free or not our buffer.
            throw IllegalArgumentException("returned buffer ${ref.hashCode} was never allocated by this pool")

        check(metadata.sizeBytes == justReleased.capacity()) {
            //this is a bug
            "buffer ${ref.hashCode} has capacity ${justReleased.capacity()} but recorded as ${metadata.sizeBytes}"
        }

        log.trace("released buffer of size {} and identity {}", metadata.sizeBytes, ref.hashCode)
    }

    override fun close() {
        alive = false
        gcListenerThread.interrupt()
    }

    private inner class GarbageCollectionListener : Runnable {
        override fun run() {
            while (alive) {
                try {
                    val ref = garbageCollectedBuffers.remove() as BufferReference //blocks
                    ref.clear()
                    //this cannot race with a release() call because an object is either reachable or not,
                    //release() can only happen before its GC'ed, and enqueue can only happen after.
                    //if the ref was enqueued it must then not have been released
                    val metadata = buffersInFlight.remove(ref)
                        ?: //it can happen rarely that the buffer was release()ed properly (so no metadata) and yet
                        //the reference object to it remains reachable for a short period of time after release()
                        //and hence gets enqueued. this is because we keep refs in a ConcurrentHashMap which cleans
                        //up keys lazily.
                        continue
                    availableMemory.addAndGet(metadata.sizeBytes.toLong())
                    log.error(
                        "Reclaimed buffer of size {} and identity {} that was not properly release()ed. This is a bug.",
                        metadata.sizeBytes,
                        ref.hashCode
                    )
                } catch (e: InterruptedException) {
                    log.debug("interrupted", e)
                    //ignore, we're a daemon thread
                }
            }
            log.info("GC listener shutting down")
        }
    }

    private class BufferMetadata(val sizeBytes: Int)

    private class BufferReference(
        referent: ByteBuffer,
        q: ReferenceQueue<in ByteBuffer>? = null
    ) : WeakReference<ByteBuffer>(referent, q) {

        val hashCode: Int

        init {
            hashCode = System.identityHashCode(referent)
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) { //this is important to find leaked buffers (by ref identity)
                return true
            }
            if (other == null || javaClass != other.javaClass) {
                return false
            }
            val that = other as BufferReference
            if (hashCode != that.hashCode) {
                return false
            }
            val thisBuf = get()
                ?: //our buffer has already been GC'ed, yet "that" is not us. so not same buffer
                return false
            val thatBuf = that.get()
            return thisBuf === thatBuf
        }

        override fun hashCode(): Int {
            return hashCode
        }
    }

    override fun toString(): String {
        val allocated = sizeBytes - availableMemory.get()
        return "GarbageCollectedMemoryPool{${formatBytes(allocated)}/${formatBytes(sizeBytes)} " +
                "used in ${buffersInFlight.size} buffers}"
    }
}
