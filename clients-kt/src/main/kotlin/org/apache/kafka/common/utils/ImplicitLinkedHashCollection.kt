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

import java.util.AbstractSequentialList
import kotlin.Comparator
import kotlin.NoSuchElementException
import kotlin.collections.ArrayList

/**
 * A memory-efficient hash set which tracks the order of insertion of elements.
 *
 * Like java.util.LinkedHashSet, this collection maintains a linked list of elements. However,
 * rather than using a separate linked list, this collection embeds the next and previous fields
 * into the elements themselves. This reduces memory consumption, because it means that we only
 * have to store one Java object per element, rather than multiple.
 *
 * The next and previous fields are stored as array indices rather than pointers. This ensures that
 * the fields only take 32 bits, even when pointers are 64 bits. It also makes the garbage
 * collector's job easier, because it reduces the number of pointers that it must chase.
 *
 * This class uses linear probing. Unlike HashMap (but like HashTable), we don't force the size to
 * be a power of 2. This saves memory.
 *
 * This set does not allow null elements. It does not have internal synchronization.
 */
open class ImplicitLinkedHashCollection<E : ImplicitLinkedHashCollection.Element> :
    AbstractMutableCollection<E> {

    private var head: Element? = null

    lateinit var elements: Array<Element?>

    private var _size = 0

    override val size: Int
        get() = _size

    /**
     * Create a new ImplicitLinkedHashCollection.
     *
     * @param expectedNumElements   The number of elements we expect to have in this set. This is
     * used to optimize by setting the capacity ahead of time rather than growing incrementally.
     *
     * Create a new ImplicitLinkedHashCollection.
     */
    constructor(expectedNumElements: Int = 0) {
        clear(expectedNumElements)
    }

    /**
     * Create a new ImplicitLinkedHashCollection.
     *
     * @param iter We will add all the elements accessible through this iterator to the set.
     */
    constructor(iter: Iterator<E>) {
        clear(0)
        while (iter.hasNext()) mustAdd(iter.next())
    }

    /**
     * Returns an iterator that will yield every element in the set. The elements will be returned
     * in the order that they were inserted in.
     *
     * Do not modify the set while you are iterating over it (except by calling remove on the
     * iterator itself, of course).
     */
    override fun iterator(): MutableIterator<E> = listIterator(0)

    private fun listIterator(index: Int): MutableListIterator<E> =
        ImplicitLinkedHashCollectionIterator(index)

    fun slot(curElements: Array<Element?>, e: Any?): Int =
        (e.hashCode() and 0x7fffffff) % curElements.size

    /**
     * Find an element matching an example element.
     *
     * Using the element's hash code, we can look up the slot where it belongs. However, it may not
     * have ended up in exactly this slot, due to a collision. Therefore, we must search forward in
     * the array until we hit a null, before concluding that the element is not present.
     *
     * @param key The element to match.
     * @return The match index, or `INVALID_INDEX` if no match was found.
     */
    private fun findIndexOfEqualElement(key: Any?): Int {
        if (key == null || size == 0) return INVALID_INDEX

        var slot = slot(elements, key)

        repeat(elements.count()) {
            val element = elements[slot] ?: return INVALID_INDEX
            if (element.elementKeysAreEqual(key)) return slot

            slot = (slot + 1) % elements.size
        }
        return INVALID_INDEX
    }

    /**
     * An element e in the collection such that e.elementKeysAreEqual(key) and
     * `e.hashCode() == key.hashCode()`.
     *
     * @param key The element to match.
     * @return The matching element, or `null` if there were none.
     */
    fun find(key: E): E? {
        val index = findIndexOfEqualElement(key)
        return if (index == INVALID_INDEX) null
        else elements[index] as E
    }

    /**
     * Returns true if there is at least one element e in the collection such that
     * `key.elementKeysAreEqual(e)` and `key.hashCode() == e.hashCode()`.
     *
     * @param element The object to try to match.
     */
    override operator fun contains(element: E): Boolean =
        findIndexOfEqualElement(element) != INVALID_INDEX

    /**
     * Add a new element to the collection.
     *
     * @param element The new element.
     *
     * @return `true` if the element was added to the collection; `false` if it was not, because
     * there was an existing equal element.
     */
    override fun add(element: E): Boolean {
        if (element.prev() != INVALID_INDEX || element.next() != INVALID_INDEX) return false

        if (size + 1 >= elements.size / 2) changeCapacity(calculateCapacity(elements.size))

        val slot = addInternal(element, elements)

        return if (slot >= 0) {
            addToListTail(head, elements, slot)
            _size++
            true
        } else false
    }

    fun mustAdd(newElement: E) {
        if (!add(newElement)) throw RuntimeException("Unable to add $newElement")
    }

    /**
     * Adds a new element to the appropriate place in the elements array.
     *
     * @param newElement The new element to add.
     * @param addElements The elements array.
     * @return The index at which the element was inserted, or INVALID_INDEX if the element could
     * not be inserted.
     */
    open fun addInternal(newElement: Element?, addElements: Array<Element?>): Int {
        var slot = slot(addElements, newElement)

        repeat(addElements.count()) {
            val element = addElements[slot]
            if (element == null) {
                addElements[slot] = newElement
                return slot
            }
            if (element.elementKeysAreEqual(newElement)) return INVALID_INDEX

            slot = (slot + 1) % addElements.size
        }

        throw RuntimeException("Not enough hash table slots to add a new element.")
    }

    private fun changeCapacity(newCapacity: Int) {
        val newElements = arrayOfNulls<Element>(newCapacity)
        val newHead = HeadElement()
        val oldSize = size
        val iter = iterator()

        while (iter.hasNext()) {
            val element: Element = iter.next()
            iter.remove()
            val newSlot = addInternal(element, newElements)
            addToListTail(newHead, newElements, newSlot)
        }

        elements = newElements
        head = newHead
        _size = oldSize
    }

    /**
     * Remove the first element e such that `key.elementKeysAreEqual(e)` and
     * `key.hashCode == e.hashCode`.
     *
     * @param element The object to try to match.
     * @return `true` if an element was removed; `false` otherwise.
     */
    override fun remove(element: E): Boolean {
        val slot = findElementToRemove(element)
        if (slot == INVALID_INDEX) return false

        removeElementAtSlot(slot)
        return true
    }

    open fun findElementToRemove(key: Any?): Int = findIndexOfEqualElement(key)

    /**
     * Remove an element in a particular slot.
     *
     * @param slot The slot of the element to remove.
     *
     * @return `true` if an element was removed; `false` otherwise.
     */
    private fun removeElementAtSlot(slot: Int): Boolean {
        var slot = slot
        _size--

        removeFromList(head, elements, slot)
        slot = (slot + 1) % elements.size

        // Find the next empty slot
        var endSlot = slot

        repeat(elements.count()) {
            elements[endSlot] ?: return@repeat
            endSlot = (endSlot + 1) % elements.size
        }

        // We must preserve the denseness invariant. The denseness invariant says that any element
        // is either in the slot indicated by its hash code, or a slot which is not separated from
        // that slot by any nulls. Reseat all elements in between the deleted element and the next
        // empty slot.
        while (slot != endSlot) {
            reseat(slot)
            slot = (slot + 1) % elements.size
        }
        return true
    }

    private fun reseat(prevSlot: Int) {
        val element = elements[prevSlot]
        var newSlot = slot(elements, element)

        repeat(elements.count()) {
            val e = elements[newSlot]
            if (e == null || e === element) return@repeat

            newSlot = (newSlot + 1) % elements.size
        }

        if (newSlot == prevSlot) return

        val prev = indexToElement(head!!, elements, element!!.prev())!!
        prev.setNext(newSlot)
        val next = indexToElement(head!!, elements, element.next())!!
        next.setPrev(newSlot)
        elements[prevSlot] = null
        elements[newSlot] = element
    }

    /**
     * Removes all the elements from this set.
     */
    override fun clear() = clear(elements.size)

    /**
     * Moves an element which is already in the collection so that it comes last in iteration order.
     */
    fun moveToEnd(element: E) {
        if (element.prev() == INVALID_INDEX || element.next() == INVALID_INDEX)
            throw RuntimeException("Element $element is not in the collection.")

        val prevElement = indexToElement(head!!, elements, element.prev())!!
        val nextElement = indexToElement(head!!, elements, element.next())!!
        val slot = prevElement.next()
        prevElement.setNext(element.next())
        nextElement.setPrev(element.prev())
        addToListTail(head, elements, slot)
    }

    /**
     * Removes all the elements from this set, and resets the set capacity based on the provided
     * expected number of elements.
     */
    fun clear(expectedNumElements: Int) {
        if (expectedNumElements == 0) {
            // Optimize away object allocations for empty sets.
            head = HeadElement.EMPTY
            elements = EMPTY_ELEMENTS
            _size = 0
        } else {
            head = HeadElement()
            elements = arrayOfNulls(calculateCapacity(expectedNumElements))
            _size = 0
        }
    }

    /**
     * Compares the specified object with this collection for equality. Two
     * `ImplicitLinkedHashCollection` objects are equal if they contain the same elements (as
     * determined by the element's `equals` method), and those elements were inserted in the same
     * order. Because `ImplicitLinkedHashCollectionListIterator` iterates over the elements in
     * insertion order, it is sufficient to call `valuesList.equals`.
     *
     * Note that [ImplicitLinkedHashMultiCollection] does not override `equals` and uses this method
     * as well. This means that two `ImplicitLinkedHashMultiCollection` objects will be considered
     * equal even if they each contain two elements A and B such that A.equals(B) but A != B and
     * A and B have switched insertion positions between the two collections. This is an acceptable
     * definition of equality, because the collections are still equal in terms of the order and
     * value of each element.
     *
     * @param other object to be compared for equality with this collection
     * @return true is the specified object is equal to this collection
     */
    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is ImplicitLinkedHashCollection<*>) return false
        return valuesList() == other.valuesList()
    }

    /**
     * Returns the hash code value for this collection. Because
     * `ImplicitLinkedHashCollection.equals` compares the `valuesList` of two
     * `ImplicitLinkedHashCollection` objects to determine equality, this method uses the
     * [valuesList] to compute the has code value as well.
     *
     * @return the hash code value for this collection
     */
    override fun hashCode(): Int = valuesList().hashCode()

    // Visible for testing
    fun numSlots(): Int = elements.size

    /**
     * Returns a [List] view of the elements contained in the collection, ordered by order of
     * insertion into the collection. The list is backed by the collection, so changes to the
     * collection are reflected in the list and vice-versa. The list supports element removal, which
     * removes the corresponding element from the collection, but does not support the `add` or
     * `set` operations.
     *
     * The list is implemented as a circular linked list, so all index-based operations, such as
     * `List.get`, run in O(n) time.
     *
     * @return a list view of the elements contained in this collection
     */
    fun valuesList(): MutableList<E> = ImplicitLinkedHashCollectionListView()

    /**
     * Returns a [Set] view of the elements contained in the collection. The set is backed by the
     * collection, so changes to the collection are reflected in the set, and vice versa. The set
     * supports element removal and addition, which removes from or adds to the collection,
     * respectively.
     *
     * @return a set view of the elements contained in this collection
     */
    fun valuesSet(): MutableSet<E> = ImplicitLinkedHashCollectionSetView()

    fun sort(comparator: Comparator<E>) {
        val array = ArrayList<E>(size)
        val iterator = iterator()
        while (iterator.hasNext()) {
            val e = iterator.next()
            iterator.remove()
            array.add(e)
        }

        array.sortWith(comparator)
        for (e in array) add(e)
    }

    /**
     * The interface which elements of this collection must implement. The prev, setPrev, next, and
     * setNext functions handle manipulating the implicit linked list which these elements reside in
     * inside the collection. elementKeysAreEqual() is the function which this collection uses to
     * compare elements.
     */
    interface Element {

        fun prev(): Int

        fun setPrev(prev: Int)

        operator fun next(): Int

        fun setNext(next: Int)

        fun elementKeysAreEqual(other: Any?): Boolean = equals(other)
    }

    private class HeadElement : Element {

        private var prev = HEAD_INDEX

        private var next = HEAD_INDEX

        override fun prev(): Int = prev

        override fun setPrev(prev: Int) {
            this.prev = prev
        }

        override fun next(): Int = next

        override fun setNext(next: Int) {
            this.next = next
        }

        companion object {
            val EMPTY = HeadElement()
        }
    }

    private inner class ImplicitLinkedHashCollectionIterator(index: Int) : MutableListIterator<E> {

        private var index = 0

        private var cur: Element? = null

        private var lastReturned: Element?

        init {
            cur = indexToElement(head!!, elements, head!!.next())
            for (i in 0 until index) next()

            lastReturned = null
        }

        override fun hasNext(): Boolean = cur !== head

        override fun hasPrevious(): Boolean = indexToElement(head!!, elements, cur!!.prev()) !== head

        override fun next(): E {
            if (!hasNext()) throw NoSuchElementException()

            val returnValue = cur as E
            lastReturned = cur
            cur = indexToElement(head!!, elements, cur!!.next())
            index++
            return returnValue
        }

        override fun previous(): E {
            val prev = indexToElement(head!!, elements, cur!!.prev())

            if (prev === head) throw NoSuchElementException()

            cur = prev
            index--
            lastReturned = cur
            return cur as E
        }

        override fun nextIndex(): Int = index

        override fun previousIndex(): Int = index - 1

        override fun remove() {
            checkNotNull(lastReturned)
            val nextElement = indexToElement(head!!, elements, lastReturned!!.next())!!

            removeElementAtSlot(nextElement.prev())

            // If the element we are removing was cur, set cur to cur->next.
            if (lastReturned === cur) cur = nextElement
            // If the element we are removing comes before cur, decrement the index,
            // since there are now fewer entries before cur.
            else index--

            lastReturned = null
        }

        override fun set(e: E) {
            throw UnsupportedOperationException()
        }

        override fun add(e: E) {
            throw UnsupportedOperationException()
        }
    }

    private inner class ImplicitLinkedHashCollectionListView : AbstractSequentialList<E>() {

        override fun listIterator(index: Int): MutableListIterator<E> {
            if (index < 0 || index > size) throw IndexOutOfBoundsException()
            return this@ImplicitLinkedHashCollection.listIterator(index)
        }

        override val size: Int
            get() = _size
    }

    private inner class ImplicitLinkedHashCollectionSetView : AbstractMutableSet<E>() {

        override fun iterator(): MutableIterator<E> = this@ImplicitLinkedHashCollection.iterator()

        override val size: Int
            get() = _size

        override fun add(element: E): Boolean = this@ImplicitLinkedHashCollection.add(element)

        override fun remove(element: E): Boolean = this@ImplicitLinkedHashCollection.remove(element)

        override operator fun contains(element: E): Boolean =
            this@ImplicitLinkedHashCollection.contains(element)

        override fun clear() = this@ImplicitLinkedHashCollection.clear()
    }

    companion object {
        /**
         * A special index value used to indicate that the next or previous field is
         * the head.
         */
        private const val HEAD_INDEX = -1

        /**
         * A special index value used for next and previous indices which have not
         * been initialized.
         */
        const val INVALID_INDEX = -2

        /**
         * The minimum new capacity for a non-empty implicit hash set.
         */
        private const val MIN_NONEMPTY_CAPACITY = 5

        /**
         * A static empty array used to avoid object allocations when the capacity is zero.
         */
        private val EMPTY_ELEMENTS = arrayOfNulls<Element>(0)

        private fun indexToElement(head: Element, elements: Array<Element?>, index: Int): Element? {
            return if (index == HEAD_INDEX) head
            else elements[index]
        }

        private fun addToListTail(head: Element?, elements: Array<Element?>, elementIdx: Int) {
            val oldTailIdx = head!!.prev()
            val element = indexToElement(head, elements, elementIdx)!!
            val oldTail = indexToElement(head, elements, oldTailIdx)!!

            head.setPrev(elementIdx)
            oldTail.setNext(elementIdx)
            element.setPrev(oldTailIdx)
            element.setNext(HEAD_INDEX)
        }

        private fun removeFromList(head: Element?, elements: Array<Element?>, elementIdx: Int) {
            val element = indexToElement(head!!, elements, elementIdx)!!
            elements[elementIdx] = null

            val prevIdx = element.prev()
            val nextIdx = element.next()
            val prev = indexToElement(head, elements, prevIdx)!!
            val next = indexToElement(head, elements, nextIdx)!!

            prev.setNext(nextIdx)
            next.setPrev(prevIdx)
            element.setNext(INVALID_INDEX)
            element.setPrev(INVALID_INDEX)
        }

        private fun calculateCapacity(expectedNumElements: Int): Int {
            // Avoid using even-sized capacities, to get better key distribution.
            val newCapacity = 2 * expectedNumElements + 1
            // Don't use a capacity that is too small.
            return newCapacity.coerceAtLeast(MIN_NONEMPTY_CAPACITY)
        }
    }
}
