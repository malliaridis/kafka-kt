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

import java.util.concurrent.ConcurrentMap

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on
 * each modification.
 */
class CopyOnWriteMap<K, V>(map: Map<K, V> = emptyMap()) : ConcurrentMap<K, V> {

    @Volatile
    private var map: Map<K, V>

    init {
        this.map = map.toMap()
    }

    override fun containsKey(key: K): Boolean = map.containsKey(key)

    override fun containsValue(value: V): Boolean = map.containsValue(value)

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = map.toMutableMap().entries

    override fun get(key: K): V? = map[key]

    override fun isEmpty(): Boolean = map.isEmpty()

    override val keys: MutableSet<K>
        get() = map.keys.toMutableSet()

    override val size: Int
        get() = map.size

    override val values: MutableCollection<V>
        get() = map.values.toMutableList()

    @Synchronized
    override fun clear() {
        map = emptyMap()
    }

    @Synchronized
    override fun put(key: K, value: V): V? {
        val copy = map.toMutableMap()
        val prev = copy.put(key, value)
        map = copy.toMap()
        return prev
    }

    @Synchronized
    override fun putAll(from: Map<out K, V>) {
        val copy = map.toMutableMap()
        copy.putAll(from)
        map = copy.toMap()
    }

    @Synchronized
    override fun remove(key: K): V? {
        val copy = map.toMutableMap()
        val prev = copy.remove(key)
        map = copy.toMap()
        return prev
    }

    @Synchronized
    override fun putIfAbsent(k: K, v: V): V? {
        return if (!containsKey(k)) put(k, v) else get(k)
    }

    @Synchronized
    override fun remove(key: K, value: V): Boolean {
        return if (containsKey(key) && get(key) == value) {
            remove(key)
            true
        } else false
    }

    @Synchronized
    override fun replace(k: K, original: V, replacement: V): Boolean {
        return if (containsKey(k) && get(k) == original) {
            put(k, replacement)
            true
        } else false
    }

    @Synchronized
    override fun replace(k: K, v: V): V? {
        return if (containsKey(k)) put(k, v)
        else null
    }
}
