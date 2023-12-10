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
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.stream.Stream
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.spy
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class ChunkedBytesStreamTest {

    private val supplier = BufferSupplier.NO_CACHING

    @Test
    @Throws(IOException::class)
    fun testEofErrorForMethodReadFully() {
        val input = ByteBuffer.allocate(8)
        val lengthGreaterThanInput = input.capacity() + 1
        val got = ByteArray(lengthGreaterThanInput)
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(input),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            assertEquals(
                expected = 8,
                actual = inputStream.read(got, 0, got.size),
                message = "Should return 8 signifying end of input",
            )
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    @Throws(IOException::class)
    fun testCorrectnessForMethodReadFully(input: ByteBuffer) {
        val got = ByteArray(input.array().size)
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(input),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            // perform a 2 pass read. this tests the scenarios where one pass may lead to partially consumed
            // intermediate buffer
            val toRead = RANDOM.nextInt(got.size)
            inputStream.read(got, 0, toRead)
            inputStream.read(got, toRead, got.size - toRead)
        }
        assertContentEquals(input.array(), got)
    }

    @ParameterizedTest
    @MethodSource("provideCasesWithInvalidInputsForMethodRead")
    @Throws(IOException::class)
    fun testInvalidInputsForMethodRead(b: ByteArray?, off: Int, len: Int) {
        val buffer = ByteBuffer.allocate(16)
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(buffer.duplicate()),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            assertFailsWith<IndexOutOfBoundsException> { inputStream.read(b!!, off, len) }
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    @Throws(IOException::class)
    fun testCorrectnessForMethodReadByte(input: ByteBuffer) {
        val got = ByteArray(input.array().size)
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(input),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            var i = 0
            while (i < got.size) got[i++] = inputStream.read().toByte()
        }
        assertContentEquals(input.array(), got)
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    @Throws(IOException::class)
    fun testCorrectnessForMethodRead(inputBuf: ByteBuffer) {
        val inputArr = IntArray(inputBuf.capacity())
        for (i in inputArr.indices) {
            inputArr[i] = java.lang.Byte.toUnsignedInt(inputBuf.get())
        }
        val got = IntArray(inputArr.size)
        inputBuf.rewind()
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(inputBuf),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            var i = 0
            while (i < got.size) got[i++] = inputStream.read()
        }
        assertContentEquals(inputArr, got)
    }

    @Test
    @Throws(IOException::class)
    fun testEndOfFileForMethodRead() {
        val inputBuf = ByteBuffer.allocate(2)
        val lengthGreaterThanInput = inputBuf.capacity() + 1
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(inputBuf),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = false,
        ).use { inputStream ->
            var cnt = 0
            while (cnt++ < lengthGreaterThanInput) {
                val res = inputStream.read()
                if (cnt > inputBuf.capacity()) assertEquals(
                    expected = -1,
                    actual = res,
                    message = "end of file for read should be -1"
                )
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Throws(IOException::class)
    fun testEndOfSourceForMethodSkip(pushSkipToSourceStream: Boolean) {
        val inputBuf = ByteBuffer.allocate(16)
        RANDOM.nextBytes(inputBuf.array())
        inputBuf.rewind()
        val sourcestream: InputStream = spy(ByteBufferInputStream(inputBuf))
        ChunkedBytesStream(
            inputStream = sourcestream,
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = pushSkipToSourceStream,
        ).use { inputStream ->
            val res = inputStream.skip((inputBuf.capacity() + 1).toLong())
            assertEquals(inputBuf.capacity().toLong(), res)
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceSkipValuesForTest")
    @Throws(IOException::class)
    fun testCorrectnessForMethodSkip(
        bytesToPreRead: Int,
        inputBuf: ByteBuffer,
        numBytesToSkip: Int,
        pushSkipToSourceStream: Boolean,
    ) {
        val expectedInpLeftAfterSkip = inputBuf.remaining() - bytesToPreRead - numBytesToSkip
        val expectedSkippedBytes = (inputBuf.remaining() - bytesToPreRead).coerceAtMost(numBytesToSkip)
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(inputBuf.duplicate()),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = pushSkipToSourceStream,
        ).use { inputStream ->
            var cnt = 0
            while (cnt++ < bytesToPreRead) {
                val r = inputStream.read()
                assertNotEquals(-1, r, "Unexpected end of data.")
            }
            val res = inputStream.skip(numBytesToSkip.toLong())
            assertEquals(expectedSkippedBytes.toLong(), res)

            // verify that we are able to read rest of the input
            cnt = 0
            while (cnt++ < expectedInpLeftAfterSkip) {
                val readRes = inputStream.read()
                assertNotEquals(-1, readRes, "Unexpected end of data.")
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideEdgeCaseInputForMethodSkip")
    @Throws(IOException::class)
    fun testEdgeCaseInputForMethodSkip(
        bufferLength: Int,
        toSkip: Long,
        delegateSkipToSourceStream: Boolean,
        expected: Long,
    ) {
        val inputBuf = ByteBuffer.allocate(bufferLength)
        RANDOM.nextBytes(inputBuf.array())
        inputBuf.rewind()
        ChunkedBytesStream(
            inputStream = ByteBufferInputStream(inputBuf.duplicate()),
            bufferSupplier = supplier,
            intermediateBufSize = 10,
            delegateSkipToSourceStream = delegateSkipToSourceStream,
        ).use { inputStream -> assertEquals(expected, inputStream.skip(toSkip)) }
    }

    companion object {

        private val RANDOM = Random(1337)

        @JvmStatic
        private fun provideEdgeCaseInputForMethodSkip(): Stream<Arguments> {
            val bufferLength = 16
            // Test toSkip larger than int and negative for both delegateToSourceStream true and false
            return Stream.of(
                Arguments.of(bufferLength, Int.MAX_VALUE + 1L, true, bufferLength),
                Arguments.of(bufferLength, -1, true, 0),
                Arguments.of(bufferLength, Int.MAX_VALUE + 1L, false, bufferLength),
                Arguments.of(bufferLength, -1, false, 0)
            )
        }

        @JvmStatic
        private fun provideCasesWithInvalidInputsForMethodRead(): Stream<Arguments> {
            val b = ByteArray(16)
            return Stream.of(
                // negative off
                Arguments.of(b, -1, b.size),
                // negative len
                Arguments.of(b, 0, -1),
                // overflow off + len
                Arguments.of(b, Int.MAX_VALUE, 10),
                // len greater than size of target array
                Arguments.of(b, 0, b.size + 1),
                // off + len greater than size of target array
                Arguments.of(b, b.size - 1, 2)
            )
        }

        @JvmStatic
        private fun provideSourceSkipValuesForTest(): List<Arguments> {
            val bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16)
            RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array())
            bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity())
            bufGreaterThanIntermediateBuf.flip()
            val bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100)
            RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array())
            bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity())
            bufMuchGreaterThanIntermediateBuf.flip()
            val emptyBuffer = ByteBuffer.allocate(2)
            val oneByteBuf = ByteBuffer.allocate(1).put(1.toByte())
            oneByteBuf.flip()
            val testInputs = listOf(
                // empty source byte array
                listOf(0, emptyBuffer, 0),
                listOf(0, emptyBuffer, 1),
                listOf(1, emptyBuffer, 1),
                listOf(1, emptyBuffer, 0),
                // byte source array with 1 byte
                listOf(0, oneByteBuf, 0),
                listOf(0, oneByteBuf, 1),
                listOf(1, oneByteBuf, 0),
                listOf(1, oneByteBuf, 1),
                // byte source array with full read from intermediate buf
                listOf(0, bufGreaterThanIntermediateBuf.duplicate(), bufGreaterThanIntermediateBuf.capacity()),
                listOf(bufGreaterThanIntermediateBuf.capacity(), bufGreaterThanIntermediateBuf.duplicate(), 0),
                listOf(2, bufGreaterThanIntermediateBuf.duplicate(), 10),
                listOf(2, bufGreaterThanIntermediateBuf.duplicate(), 8),
            )
            val tailArgs = arrayOf(true, false)
            val finalArguments: MutableList<Arguments> = ArrayList(2 * testInputs.size)
            for (args in testInputs) {
                for (aBoolean in tailArgs) {
                    val expandedArgs: MutableList<Any> = ArrayList(args)
                    expandedArgs.add(aBoolean)
                    finalArguments.add(Arguments.of(*expandedArgs.toTypedArray()))
                }
            }
            return finalArguments
        }

        @JvmStatic
        private fun provideSourceBytebuffersForTest(): Stream<Arguments> {
            val bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16)
            RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array())
            bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity())
            val bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100)
            RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array())
            bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity())
            return Stream.of(
                // empty byte array
                Arguments.of(ByteBuffer.allocate(2)),
                // byte array with 1 byte
                Arguments.of(ByteBuffer.allocate(1).put(1.toByte()).flip()),
                // byte array with size < intermediate buffer
                Arguments.of(ByteBuffer.allocate(8).put("12345678".toByteArray()).flip()),
                // byte array with size > intermediate buffer
                Arguments.of(bufGreaterThanIntermediateBuf.flip()),
                // byte array with size >> intermediate buffer
                Arguments.of(bufMuchGreaterThanIntermediateBuf.flip())
            )
        }
    }
}
