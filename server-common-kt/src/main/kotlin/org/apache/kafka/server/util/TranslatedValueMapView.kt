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

/**
 * A map which presents a lightweight view of another "underlying" map. Values in the
 * underlying map will be translated by a callback before they are returned.
 *
 * This class is not internally synchronized. (Typically the underlyingMap is treated as
 * immutable.)
 */
class TranslatedValueMapView<K, V, B>(
    private val underlyingMap: Map<K, B>,
    private val valueMapping: (B?) -> V,
) : AbstractMap<K, V>() {

    private val set: TranslatedValueSetView = TranslatedValueSetView()

    override fun containsKey(key: K): Boolean = underlyingMap.containsKey(key)

    override fun get(key: K): V? {
        if (!underlyingMap.containsKey(key)) return null
        val value = underlyingMap[key]
        return valueMapping(value)
    }

    override val entries: Set<Map.Entry<K, V>>
        get() = set

    override fun isEmpty(): Boolean = underlyingMap.isEmpty()

    internal inner class TranslatedValueSetView : AbstractSet<Map.Entry<K, V>>() {

        override fun iterator(): MutableIterator<Map.Entry<K, V>> {
            return TranslatedValueEntryIterator(underlyingMap.iterator())
        }

        override fun contains(element: Map.Entry<K, V>): Boolean {
            val (key, value1) = element
            if (!underlyingMap.containsKey(key)) return false
            val value = underlyingMap[key]
            val translatedValue = valueMapping(value)
            return translatedValue == value1
        }

        override fun isEmpty(): Boolean = underlyingMap.isEmpty()

        override val size: Int
            get() = underlyingMap.size
    }

    internal inner class TranslatedValueEntryIterator(private val underlyingIterator: Iterator<Map.Entry<K, B>>) :
        MutableIterator<Map.Entry<K, V>> {

        override fun hasNext(): Boolean = underlyingIterator.hasNext()

        override fun next(): Map.Entry<K, V> {
            val (key, value) = underlyingIterator.next()
            return object: Map.Entry<K,V> {
                override val key: K = key
                override val value: V = valueMapping(value)
            }
        }

        override fun remove() = throw UnsupportedOperationException("remove")
    }
}
