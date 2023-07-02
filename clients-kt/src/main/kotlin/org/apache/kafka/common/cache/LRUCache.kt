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

package org.apache.kafka.common.cache

/**
 * A cache implementing a least recently used policy.
 */
class LRUCache<K, V>(maxSize: Int) : Cache<K, V> {

    private val cache: LinkedHashMap<K, V> = object : LinkedHashMap<K, V>(
        initialCapacity = 16,
        loadFactor = .75f,
        accessOrder = true,
    ) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean {
            return size() > maxSize
        }
    }

    override fun get(key: K): V? {
        return cache[key]
    }

    override fun put(key: K, value: V) {
        cache[key] = value
    }

    override fun remove(key: K): Boolean {
        return cache.remove(key) != null
    }

    override fun size(): Long {
        return cache.size.toLong()
    }
}
