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

import java.lang.Long.numberOfLeadingZeros
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

/**
 * A hash table which uses separate chaining.
 *
 * In order to optimize memory consumption a bit, the common case where there is
 * one element per slot is handled by simply placing the element in the slot,
 * and the case where there are multiple elements is handled by creating an
 * array and putting that in the slot.  Java is storing type info in memory
 * about every object whether we want it or not, so let's get some benefit
 * out of it.
 *
 * Arrays and null values cannot be inserted.
 */
open class BaseHashTable<T> internal constructor(expectedSize: Int) {

    private var elements: Array<Any?>

    private var size = 0

    init {
        elements = Array(expectedSizeToCapacity(expectedSize)) { null } //  Array of Unit at the beginning
    }

    fun baseSize(): Int = size

    fun baseElements(): Array<Any?> = elements

    fun baseGet(key: Any): T? {
        val slot = findSlot(key, elements.size)
        when (val value = elements[slot]) {
            null -> return null
            is Array<*> -> {
                val array = value as Array<T>
                for (obj in array)
                    if (obj == key) return obj

                return null
            }
            else -> return if (value == key) value as T else null
        }
    }

    fun baseAddOrReplace(newObject: T): T? {
        if ((size + 1) * MAX_LOAD_FACTOR > elements.size && elements.size < MAX_CAPACITY) {
            val newSize = elements.size * 2
            rehash(newSize)
        }
        val slot = findSlot(newObject as Any, elements.size)
        val cur = elements[slot]
        when (cur) {
            null -> {
                size++
                elements[slot] = newObject
                return null
            }
            is Array<*> -> {
                val curArray = cur as Array<T>
                for (i in curArray.indices) {
                    val value = curArray[i]
                    if (value == newObject) {
                        curArray[i] = newObject
                        return value
                    }
                }
                size++
                val newArray = curArray.copyOf(curArray.size + 1)
                newArray[curArray.size] = newObject
                elements[slot] = newArray
                return null
            }

            newObject -> {
                elements[slot] = newObject
                return cur as T
            }

            else -> {
                size++
                elements[slot] = arrayOf(cur, newObject)
                return null
            }
        }
    }

    fun baseRemove(key: Any): T? {
        val slot = findSlot(key, elements.size)
        when (val obj = elements[slot]) {
            null -> return null
            is Array<*> -> {
                val curArray = obj
                for (i in curArray.indices) {
                    if (curArray[i] == key) {
                        size--
                        if (curArray.size <= 2) {
                            val j = if (i == 0) 1 else 0
                            elements[slot] = curArray[j]
                        } else {
                            val newArray = arrayOfNulls<Any>(curArray.size - 1)
                            System.arraycopy(curArray, 0, newArray, 0, i)
                            System.arraycopy(curArray, i + 1, newArray, i, curArray.size - 1 - i)
                            elements[slot] = newArray
                        }
                        return curArray[i] as T
                    }
                }
                return null
            }

            key -> {
                size--
                elements[slot] = null
                return obj as T
            }

            else -> return null
        }
    }

    /**
     * Expand the hash table to a new size. Existing elements will be copied to new slots.
     */
    private fun rehash(newSize: Int) {
        val prevElements = elements
        elements = arrayOfNulls(newSize)
        val ready = ArrayList<Any>()

        for (slot in prevElements.indices) {
            unpackSlot(ready, prevElements, slot)
            for (obj in ready) {
                val newSlot = findSlot(obj, elements.size)
                val cur = elements[newSlot]
                elements[newSlot] = when (cur) {
                    null -> obj
                    is Array<*> -> {
                        val curArray = cur
                        val newArray = arrayOfNulls<Any>(curArray.size + 1)
                        System.arraycopy(curArray, 0, newArray, 0, curArray.size)
                        newArray[curArray.size] = obj
                        newArray
                    }

                    else -> arrayOf(cur, obj)
                }
            }
            ready.clear()
        }
    }

    fun baseToDebugString(): String {
        val bld = StringBuilder()
        bld.append("BaseHashTable{")

        for (i in elements.indices) {
            val slotObject = elements[i]
            bld.append(String.format("%n%d: ", i))
            when (slotObject) {
                null -> bld.append("null")
                is Array<*> -> {
                    var prefix = ""
                    for (obj in slotObject) {
                        bld.append(prefix)
                        prefix = ", "
                        bld.append(obj)
                    }
                }

                else -> bld.append(slotObject)
            }
        }
        bld.append(String.format("%n}"))
        return bld.toString()
    }

    companion object {

        /**
         * The maximum load factor we will allow the hash table to climb to before expanding.
         */
        private val MAX_LOAD_FACTOR = 0.75

        /**
         * The minimum number of slots we can have in the hash table.
         */
        val MIN_CAPACITY = 2

        /**
         * The maximum number of slots we can have in the hash table.
         */
        val MAX_CAPACITY = 1 shl 30

        /**
         * Calculate the capacity we should provision, given the expected size.
         *
         * Our capacity must always be a power of 2, and never less than 2 or more than [MAX_CAPACITY].
         * We use 64-bit numbers here to avoid overflow concerns.
         */
        fun expectedSizeToCapacity(expectedSize: Int): Int {
            val minCapacity =
                ceil(expectedSize.toFloat() / MAX_LOAD_FACTOR).toLong()
            return max(
                MIN_CAPACITY.toDouble(),
                min(MAX_CAPACITY.toDouble(), roundUpToPowerOfTwo(minCapacity).toDouble())
                    .toInt().toDouble()
            ).toInt()
        }

        private fun roundUpToPowerOfTwo(i: Long): Long {
            return if (i <= 0) 0
            else if (i > (1L shl 62))
                throw ArithmeticException("There are no 63-bit powers of 2 higher than or equal to $i")
            else 1L shl - numberOfLeadingZeros(i - 1)
        }

        /**
         * Find the slot in the array that an element should go into.
         */
        fun findSlot(obj: Any, numElements: Int): Int {
            // This performs a secondary hash using Knuth's multiplicative Fibonacci
            // hashing.  Then, we choose some of the highest bits.  The number of bits
            // we choose is based on the table size.  If the size is 2, we need 1 bit;
            // if the size is 4, we need 2 bits, etc.
            val objectHashCode = obj.hashCode()
            val log2size = 32 - Integer.numberOfLeadingZeros(numElements)
            val shift = 65 - log2size
            return ((objectHashCode * -7046029254386353131L) ushr shift).toInt()
        }

        /**
         * Copy any elements in the given slot into the output list.
         */
        fun <T> unpackSlot(out: MutableList<T>, elements: Array<Any?>, slot: Int) {
            val value = elements[slot]
            when (value) {
                null -> return
                is Array<*> -> for (obj in value) out.add(obj as T)
                else -> out.add(value as T)
            }
        }
    }
}
