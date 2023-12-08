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

package org.apache.kafka.common

/**
 * A template for a MetricName. It contains a name, group, and description, as
 * well as all the tags that will be used to create the mBean name. Tag values
 * are omitted from the template, but are filled in at runtime with their
 * specified values. The order of the tags is maintained, if an ordered set
 * is provided, so that the mBean names can be compared and sorted lexicographically.
 */
class MetricNameTemplate {

    val name: String

    val group: String

    val tags: Set<String>

    val description: String

    /**
     * Create a new template. Note that the order of the tags will be preserved if the supplied
     * `tagsNames` set has an order.
     *
     * @param name the name of the metric; may not be null
     * @param group the name of the group; may not be null
     * @param description the description of the metric; may not be null
     * @param tagsNames the set of metric tag names, which can/should be a set that maintains order;
     * may not be null
     */
    constructor(
        name: String,
        group: String,
        description: String,
        tagsNames: Set<String>,
    ) {
        this.name = name
        this.group = group
        this.description = description
        this.tags = tagsNames.toSet()
    }

    /**
     * Create a new template. Note that the order of the tags will be preserved.
     *
     * @param name the name of the metric; may not be null
     * @param group the name of the group; may not be null
     * @param description the description of the metric; may not be null
     * @param tagsNames the names of the metric tags in the preferred order; none of the tag names
     * should be null
     */
    constructor(
        name: String,
        group: String,
        description: String,
        vararg tagsNames: String,
    ) : this(
        name = name,
        group = group,
        description = description,
        tagsNames = tagsNames.toSet()
    )

    /**
     * Get the name of the metric.
     *
     * @return the metric name; never null
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("name"),
    )
    fun name(): String = name

    /**
     * Get the name of the group.
     *
     * @return the group name; never null
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("group"),
    )
    fun group(): String = group

    /**
     * Get the description of the metric.
     *
     * @return the metric description; never null
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("description"),
    )
    fun description(): String = description

    /**
     * Get the set of tag names for the metric.
     *
     * @return the ordered set of tag names; never null but possibly empty
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("tags"),
    )
    fun tags(): Set<String> = tags

    override fun toString(): String = "name=$name, group=$group, tags=$tags"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MetricNameTemplate

        if (name != other.name) return false
        if (group != other.group) return false
        if (tags != other.tags) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + group.hashCode()
        result = 31 * result + tags.hashCode()
        return result
    }
}
