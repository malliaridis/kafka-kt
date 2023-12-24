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

package org.apache.kafka.trogdor.rest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.trogdor.task.TaskSpec

/**
 * A request to the Trogdor coordinator to create a task.
 */
class CreateTaskRequest @JsonCreator constructor(
    @param:JsonProperty("id") val id: String,
    @param:JsonProperty("spec") val spec: TaskSpec,
) : Message() {

    @JsonProperty
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("id"),
    )
    fun id(): String = id

    @JsonProperty
    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("spec"),
    )
    fun spec(): TaskSpec = spec
}
