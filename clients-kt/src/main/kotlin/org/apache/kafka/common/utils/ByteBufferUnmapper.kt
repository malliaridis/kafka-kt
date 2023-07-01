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

package org.apache.kafka.common.utils

import java.io.IOException
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.nio.ByteBuffer

/**
 * Provides a mechanism to unmap mapped and direct byte buffers.
 *
 * The implementation was inspired by the one in Lucene's MMapDirectory.
 */
object ByteBufferUnmapper {

    // null if unmap is not supported
    private val UNMAP: MethodHandle?

    // null if unmap is supported
    private val UNMAP_NOT_SUPPORTED_EXCEPTION: RuntimeException?

    init {
        var unmap: Any? = null
        var exception: RuntimeException? = null
        try {
            unmap = lookupUnmapMethodHandle()
        } catch (e: RuntimeException) {
            exception = e
        }
        if (unmap != null) {
            UNMAP = unmap as? MethodHandle
            UNMAP_NOT_SUPPORTED_EXCEPTION = null
        } else {
            UNMAP = null
            UNMAP_NOT_SUPPORTED_EXCEPTION = exception
        }
    }

    /**
     * Unmap the provided mapped or direct byte buffer.
     *
     * This buffer cannot be referenced after this call, so it's highly recommended that any fields
     * referencing it should be set to `null`.
     *
     * @throws IllegalArgumentException if buffer is not mapped or direct.
     */
    @Throws(IOException::class)
    fun unmap(resourceDescription: String, buffer: ByteBuffer) {
        require(buffer.isDirect) { "Unmapping only works with direct buffers" }
        if (UNMAP == null) throw UNMAP_NOT_SUPPORTED_EXCEPTION!!
        try {
            UNMAP.invokeExact(buffer)
        } catch (throwable: Throwable) {
            throw IOException("Unable to unmap the mapped buffer: $resourceDescription", throwable)
        }
    }

    private fun lookupUnmapMethodHandle(): MethodHandle {
        val lookup = MethodHandles.lookup()
        return try {
            if (Java.IS_JAVA9_COMPATIBLE) unmapJava9(lookup)
            else unmapJava7Or8(lookup)
        } catch (e1: ReflectiveOperationException) {
            throw UnsupportedOperationException(
                "Unmapping is not supported on this platform, because internal " +
                        "Java APIs are not compatible with this Kafka version",
                e1
            )
        } catch (e1: RuntimeException) {
            throw UnsupportedOperationException(
                "Unmapping is not supported on this platform, because internal " +
                        "Java APIs are not compatible with this Kafka version",
                e1
            )
        }
    }

    @Throws(ReflectiveOperationException::class)
    private fun unmapJava7Or8(lookup: MethodHandles.Lookup): MethodHandle {
        /* "Compile" a MethodHandle that is roughly equivalent to the following lambda:
         *
         * (ByteBuffer buffer) -> {
         *   sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
         *   if (nonNull(cleaner))
         *     cleaner.clean();
         *   else
         *     noop(cleaner); // the noop is needed because MethodHandles#guardWithTest always needs both if and else
         * }
         */
        val directBufferClass = Class.forName("java.nio.DirectByteBuffer")
        val m = directBufferClass.getMethod("cleaner")

        m.isAccessible = true

        val directBufferCleanerMethod = lookup.unreflect(m)
        val cleanerClass = directBufferCleanerMethod.type().returnType()
        val cleanMethod =
            lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(Void.TYPE))

        val nonNullTest = lookup.findStatic(
            ByteBufferUnmapper::class.java,
            "nonNull",
            MethodType.methodType(
                Boolean::class.javaPrimitiveType,
                Any::class.java
            )
        ).asType(MethodType.methodType(Boolean::class.javaPrimitiveType, cleanerClass))

        val noop = MethodHandles.dropArguments(
            MethodHandles.constant(Void::class.java, null)
                .asType(MethodType.methodType(Void.TYPE)),
            0,
            cleanerClass
        )

        return MethodHandles.filterReturnValue(
            directBufferCleanerMethod,
            MethodHandles.guardWithTest(nonNullTest, cleanMethod, noop)
        ).asType(MethodType.methodType(Void.TYPE, ByteBuffer::class.java))
    }

    @Throws(ReflectiveOperationException::class)
    private fun unmapJava9(lookup: MethodHandles.Lookup): MethodHandle {
        val unsafeClass = Class.forName("sun.misc.Unsafe")
        val unmapper = lookup.findVirtual(
            unsafeClass,
            "invokeCleaner",
            MethodType.methodType(Void.TYPE, ByteBuffer::class.java),
        )
        val f = unsafeClass.getDeclaredField("theUnsafe")

        f.isAccessible = true
        val theUnsafe = f[null]

        return unmapper.bindTo(theUnsafe)
    }

    private fun nonNull(o: Any?): Boolean = o != null
}
