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

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonCreator
import org.apache.kafka.trogdor.common.StringExpander.expand
import org.apache.kafka.trogdor.rest.Message

/**
 * TopicsSpec maps topic names to descriptions of the partitions in them.
 *
 * In JSON form, this is serialized as a map whose keys are topic names, and whose entries are partition descriptions.
 * Keys may also refer to multiple partitions. For example, this specification refers to 3 topics foo1, foo2, and foo3:
 *
 * ```json
 * {
 *     "foo[1-3]" : {
 *         "numPartitions": 3
 *         "replicationFactor": 3
 *     }
 * }
 * ```
 */
class TopicsSpec : Message {

    private val map: MutableMap<String, PartitionsSpec>

    @JsonCreator
    constructor() {
        map = mutableMapOf()
    }

    private constructor(map: MutableMap<String, PartitionsSpec>) {
        this.map = map
    }

    @JsonAnyGetter
    fun get(): Map<String, PartitionsSpec> = map

    @JsonAnySetter
    operator fun set(name: String, value: PartitionsSpec) {
        map[name] = value
    }

    fun immutableCopy(): TopicsSpec {
        return TopicsSpec(map.toMap() as MutableMap<String, PartitionsSpec>)
    }

    /**
     * Enumerate the partitions inside this TopicsSpec.
     *
     * @return A map from topic names to PartitionsSpec objects.
     */
    fun materialize(): Map<String, PartitionsSpec> {
        val all: HashMap<String, PartitionsSpec> = HashMap<String, PartitionsSpec>()
        for ((topicName, partitions) in map) {
            for (expandedTopicName in expand(topicName))
                all[expandedTopicName] = partitions
        }
        return all
    }

    companion object {
        val EMPTY = TopicsSpec().immutableCopy()
    }
}
