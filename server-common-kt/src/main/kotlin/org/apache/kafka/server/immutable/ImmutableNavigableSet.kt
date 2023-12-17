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

package org.apache.kafka.server.immutable

import java.util.NavigableSet
import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableNavigableSet

/**
 * A persistent Tree-based NavigableSet wrapper java.util.Set methods that mutate in-place will
 * throw [UnsupportedOperationException].
 *
 * @param E the element type
 */
interface ImmutableNavigableSet<E> : ImmutableSet<E>, NavigableSet<E> {

    /**
     * @param e the element
     * @return a wrapped persistent navigable set that differs from this one in that the given element
     * is added (if necessary).
     */
    override fun added(e: E): ImmutableNavigableSet<E>

    /**
     * @param e the element
     * @return a wrapped persistent navigable set that differs from this one in that the given element
     * is added (if necessary).
     */
    override fun removed(e: E): ImmutableNavigableSet<E>

    companion object {

        /**
         * @param E the element type
         * @return a wrapped tree-based persistent navigable set that is empty
         */
        fun <E : Comparable<E>> empty(): ImmutableNavigableSet<E> = PCollectionsImmutableNavigableSet.empty()

        /**
         * @param e the element
         * @return a wrapped tree-based persistent set that is empty
         * @param E the element type
         */
        fun <E : Comparable<E>> singleton(e: E): ImmutableNavigableSet<E> =
            PCollectionsImmutableNavigableSet.singleton(e)
    }
}
