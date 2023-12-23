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

package org.apache.kafka.trogdor.workload

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class PayloadGeneratorTest {

    @Test
    fun testConstantPayloadGenerator() {
        val alphabet = ByteArray(26)
        for (i in alphabet.indices) alphabet[i] = ('a'.code + i).toByte()
        
        val expectedSuperset = ByteArray(512)
        for (i in expectedSuperset.indices) expectedSuperset[i] = ('a'.code + i % 26).toByte()
        
        for (i in intArrayOf(1, 5, 10, 100, 511, 512)) {
            val generator = ConstantPayloadGenerator(i, alphabet)
            assertArrayContains(expectedSuperset, generator.generate(0))
            assertArrayContains(expectedSuperset, generator.generate(10))
            assertArrayContains(expectedSuperset, generator.generate(100))
        }
    }

    @Test
    fun testSequentialPayloadGenerator() {
        val g4 = SequentialPayloadGenerator(4, 1)
        assertLittleEndianArrayEquals(1, g4.generate(0))
        assertLittleEndianArrayEquals(2, g4.generate(1))
        
        val g8 = SequentialPayloadGenerator(8, 0)
        assertLittleEndianArrayEquals(0, g8.generate(0))
        assertLittleEndianArrayEquals(1, g8.generate(1))
        assertLittleEndianArrayEquals(123123123123L, g8.generate(123123123123L))
        
        val g2 = SequentialPayloadGenerator(2, 0)
        assertLittleEndianArrayEquals(0, g2.generate(0))
        assertLittleEndianArrayEquals(1, g2.generate(1))
        assertLittleEndianArrayEquals(1, g2.generate(1))
        assertLittleEndianArrayEquals(1, g2.generate(131073))
    }

    @Test
    fun testUniformRandomPayloadGenerator() {
        val iter = PayloadIterator(
            UniformRandomPayloadGenerator(size = 1234, seed = 456, padding = 0)
        )
        val prev = iter.next()
        var uniques = 0
        while (uniques < 1000) {
            val cur = iter.next()
            assertEquals(prev!!.size, cur!!.size)
            if (!prev.contentEquals(cur)) uniques++
        }
        testReproducible(UniformRandomPayloadGenerator(1234, 456, 0))
        testReproducible(UniformRandomPayloadGenerator(1, 0, 0))
        testReproducible(UniformRandomPayloadGenerator(10, 6, 5))
        testReproducible(UniformRandomPayloadGenerator(512, 123, 100))
    }

    @Test
    fun testUniformRandomPayloadGeneratorPaddingBytes() {
        val generator = UniformRandomPayloadGenerator(1000, 456, 100)
        val val1 = generator.generate(0)
        val val1End = ByteArray(100)
        System.arraycopy(val1, 900, val1End, 0, 100)
        val val2 = generator.generate(100)
        val val2End = ByteArray(100)
        System.arraycopy(val2, 900, val2End, 0, 100)
        val val3 = generator.generate(200)
        val val3End = ByteArray(100)
        System.arraycopy(val3, 900, val3End, 0, 100)
        assertContentEquals(val1End, val2End)
        assertContentEquals(val1End, val3End)
    }

    @Test
    fun testRandomComponentPayloadGenerator() {
        val nullGenerator = NullPayloadGenerator()
        val nullConfig = RandomComponent(50, nullGenerator)

        val uniformGenerator = UniformRandomPayloadGenerator(5, 123, 0)
        val uniformConfig = RandomComponent(50, uniformGenerator)

        val sequentialGenerator = SequentialPayloadGenerator(4, 10)
        val sequentialConfig = RandomComponent(75, sequentialGenerator)

        val constantGenerator = ConstantPayloadGenerator(4, ByteArray(0))
        val constantConfig = RandomComponent(25, constantGenerator)

        val components1 = listOf(nullConfig, uniformConfig)
        val components2 = listOf(sequentialConfig, constantConfig)
        val expected = ByteArray(4)

        var iter = PayloadIterator(RandomComponentPayloadGenerator(4, components1))
        var notNull = 0
        var isNull = 0
        while (notNull < 1000 || isNull < 1000) {
            val cur = iter.next()
            if (cur == null) isNull++
            else notNull++
        }

        iter = PayloadIterator(RandomComponentPayloadGenerator(123, components2))
        var isZeroBytes = 0
        var isNotZeroBytes = 0
        while (isZeroBytes < 500 || isNotZeroBytes < 1500) {
            val cur = iter.next()
            if (expected.contentEquals(cur)) isZeroBytes++
            else isNotZeroBytes++
        }

        val uniformConfig2 = RandomComponent(25, uniformGenerator)
        val sequentialConfig2 = RandomComponent(25, sequentialGenerator)

        val nullConfig2 = RandomComponent(25, nullGenerator)
        val components3 = listOf(sequentialConfig2, uniformConfig2, nullConfig)
        val components4 = listOf(uniformConfig2, sequentialConfig2, constantConfig, nullConfig2)

        testReproducible(RandomComponentPayloadGenerator(4, components1))
        testReproducible(RandomComponentPayloadGenerator(123, components2))
        testReproducible(RandomComponentPayloadGenerator(50, components3))
        testReproducible(RandomComponentPayloadGenerator(0, components4))
    }

    @Test
    fun testRandomComponentPayloadGeneratorErrors() {
        val nullGenerator = NullPayloadGenerator()
        val nullConfig = RandomComponent(25, nullGenerator)

        val uniformGenerator = UniformRandomPayloadGenerator(5, 123, 0)
        val uniformConfig = RandomComponent(25, uniformGenerator)

        val constantGenerator = ConstantPayloadGenerator(4, ByteArray(0))
        val constantConfig = RandomComponent(-25, constantGenerator)

        val components1 = listOf(nullConfig, uniformConfig)
        val components2 = listOf(nullConfig, constantConfig, uniformConfig, nullConfig, uniformConfig, uniformConfig)

        assertFailsWith<IllegalArgumentException> {
            PayloadIterator(RandomComponentPayloadGenerator(1, emptyList()))
        }
        assertFailsWith<IllegalArgumentException> {
            PayloadIterator(RandomComponentPayloadGenerator(13, components2))
        }
        assertFailsWith<IllegalArgumentException> {
            PayloadIterator(RandomComponentPayloadGenerator(123, components1))
        }
    }

    @Test
    fun testPayloadIterator() {
        val expectedSize = 50
        val iter = PayloadIterator(ConstantPayloadGenerator(expectedSize, ByteArray(0)))
        val expected = ByteArray(expectedSize)
        assertEquals(0, iter.position())
        assertContentEquals(expected, iter.next())
        assertEquals(1, iter.position())
        assertContentEquals(expected, iter.next())
        assertContentEquals(expected, iter.next())
        assertEquals(3, iter.position())

        iter.seek(0)
        assertEquals(0, iter.position())
    }

    @Test
    fun testNullPayloadGenerator() {
        val generator = NullPayloadGenerator()
        assertNull(generator.generate(0))
        assertNull(generator.generate(1))
        assertNull(generator.generate(100))
    }

    companion object {

        private fun assertArrayContains(expectedSuperset: ByteArray, actual: ByteArray) {
            val expected = ByteArray(actual.size)
            System.arraycopy(expectedSuperset, 0, expected, 0, expected.size)
            assertContentEquals(expected, actual)
        }

        private fun assertLittleEndianArrayEquals(expected: Long, actual: ByteArray) {
            val longActual = ByteArray(8)
            System.arraycopy(actual, 0, longActual, 0, actual.size.coerceAtMost(longActual.size))
            val buf = ByteBuffer.wrap(longActual).order(ByteOrder.LITTLE_ENDIAN)
            assertEquals(expected, buf.getLong())
        }

        private fun testReproducible(generator: PayloadGenerator) {
            val value = generator.generate(123)
            generator.generate(456)
            val val2 = generator.generate(123)

            if (value == null) assertNull(val2)
            else assertContentEquals(value, val2)
        }
    }
}
