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

package org.apache.kafka.server.immutable.pcollections

import java.util.function.BiConsumer
import java.util.function.BiFunction
import java.util.function.Function
import org.apache.kafka.server.immutable.ImmutableMap
import org.pcollections.HashPMap
import org.pcollections.HashTreePMap

@Suppress("Deprecation")
class PCollectionsImmutableMap<K, V>(internal val underlying: HashPMap<K, V>) : ImmutableMap<K, V> {

    override fun updated(key: K, value: V): ImmutableMap<K, V> =
        PCollectionsImmutableMap(underlying.plus(key, value))

    override fun removed(key: K): ImmutableMap<K, V> =
        PCollectionsImmutableMap(underlying.minus(key))

    override val size: Int
        get() = underlying.size

    override fun isEmpty(): Boolean = underlying.isEmpty()

    override fun containsKey(key: K): Boolean = underlying.containsKey(key)

    override fun containsValue(value: V): Boolean = underlying.containsValue(value)

    override operator fun get(key: K): V? = underlying()[key]

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun put(key: K, value: V): V? = underlying.put(key, value)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun remove(key: K): V? = underlying.remove(key)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun putAll(from: Map<out K, V>) = underlying.putAll(from)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun clear() = underlying.clear()

    override val keys: MutableSet<K>
        get() = underlying.keys

    override val values: MutableCollection<V>
        get() = underlying.values

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = underlying.entries

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as PCollectionsImmutableMap<*, *>
        return underlying() == that.underlying()
    }

    override fun hashCode(): Int = underlying.hashCode()

    override fun getOrDefault(key: K, defaultValue: V): V = underlying.getOrDefault(key, defaultValue)

    override fun forEach(action: BiConsumer<in K, in V>) = underlying.forEach(action)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun replaceAll(function: BiFunction<in K, in V, out V>) = underlying.replaceAll(function)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun putIfAbsent(key: K, value: V): V? = underlying.putIfAbsent(key, value)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun remove(key: K, value: V): Boolean = underlying.remove(key, value)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun replace(key: K, oldValue: V, newValue: V): Boolean = underlying.replace(key, oldValue, newValue)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun replace(key: K, value: V): V? = underlying.replace(key, value)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun computeIfAbsent(key: K, mappingFunction: Function<in K, out V>): V =
        underlying.computeIfAbsent(key, mappingFunction)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun computeIfPresent(key: K, remappingFunction: BiFunction<in K, in V & Any, out V?>): V? =
        underlying.computeIfPresent(key, remappingFunction)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun compute(key: K, remappingFunction: BiFunction<in K, in V?, out V>): V? =
        underlying.compute(key, remappingFunction)

    // will throw UnsupportedOperationException; delegate anyway for testability
    override fun merge(key: K, value: V & Any, remappingFunction: BiFunction<in V & Any, in V & Any, out V?>): V? =
        underlying.merge(key, value, remappingFunction)

    override fun toString(): String = "PCollectionsImmutableMap{underlying=${underlying()}}"

    // package-private for testing
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("underlying"),
    )
    internal fun underlying(): HashPMap<K, V> = underlying

    companion object {

        /**
         * @param K the key type
         * @param V the value type
         * @return a wrapped hash-based persistent map that is empty
         */
        fun <K, V> empty(): PCollectionsImmutableMap<K, V> = PCollectionsImmutableMap(HashTreePMap.empty())

        /**
         * @param key the key
         * @param value the value
         * @param K the key type
         * @param V the value type
         * @return a wrapped hash-based persistent map that has a single mapping
         */
        fun <K, V> singleton(key: K, value: V): PCollectionsImmutableMap<K, V> =
            PCollectionsImmutableMap(HashTreePMap.singleton(key, value))
    }
}
