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

/**
 * A memory-efficient hash multiset which tracks the order of insertion of elements.
 * See org.apache.kafka.common.utils.ImplicitLinkedHashCollection for implementation details.
 *
 * This class is a multi-set because it allows multiple elements to be inserted that
 * have equivalent keys.
 *
 * We use reference equality when adding elements to the set.  A new element A can
 * be added if there is no existing element B such that A == B.  If an element B
 * exists such that A.elementKeysAreEqual(B), A will still be added.
 *
 * When deleting an element A from the set, we will try to delete the element B such
 * that A == B.  If no such element can be found, we will try to delete an element B
 * such that A.elementKeysAreEqual(B).
 *
 * contains() and find() are unchanged from the base class-- they will look for element
 * based on object equality via elementKeysAreEqual, not reference equality.
 *
 * This multiset does not allow null elements.  It does not have internal synchronization.
 */
open class ImplicitLinkedHashMultiCollection<E : ImplicitLinkedHashCollection.Element> :
    ImplicitLinkedHashCollection<E> {

    constructor(expectedNumElements: Int = 0) : super(expectedNumElements)

    constructor(iter: Iterator<E>) : super(iter)

    /**
     * Adds a new element to the appropriate place in the elements array.
     *
     * @param newElement The new element to add.
     * @param addElements The elements array.
     * @return The index at which the element was inserted, or INVALID_INDEX if the element could
     * not be inserted.
     */
    override fun addInternal(newElement: Element?, addElements: Array<Element?>): Int {
        var slot = slot(addElements, newElement)

        repeat(addElements.count()) {
            val element = addElements[slot]

            if (element == null) {
                addElements[slot] = newElement
                return slot
            }

            if (element === newElement) return INVALID_INDEX

            slot = (slot + 1) % addElements.size
        }
        throw RuntimeException("Not enough hash table slots to add a new element.")
    }

    /**
     * Find an element matching an example element.
     *
     * @param key The element to match.
     * @return The match index, or INVALID_INDEX if no match was found.
     */
    override fun findElementToRemove(key: Any?): Int {
        if (key == null || size == 0) return INVALID_INDEX

        var slot = slot(elements, key)
        var bestSlot = INVALID_INDEX

        repeat(elements.count()) {
            val element = elements[slot] ?: return bestSlot
            if (key === element) return slot
            else if (element.elementKeysAreEqual(key)) bestSlot = slot

            slot = (slot + 1) % elements.size
        }
        return INVALID_INDEX
    }

    /**
     * Returns all the elements e in the collection such that
     * key.elementKeysAreEqual(e) and key.hashCode() == e.hashCode().
     *
     * @param key The element to match.
     * @return All the matching elements.
     */
    fun findAll(key: E?): List<E> {
        if (key == null || size == 0) return emptyList()

        val results = ArrayList<E>()
        var slot = slot(elements, key)

        repeat(elements.count()) {
            val element = elements[slot] ?: return@repeat

            if (key.elementKeysAreEqual(element)) {
                val result = elements[slot] as E
                results.add(result)
            }

            slot = (slot + 1) % elements.size
        }
        return results
    }
}
