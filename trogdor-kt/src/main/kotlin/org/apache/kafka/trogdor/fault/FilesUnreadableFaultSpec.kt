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

package org.apache.kafka.trogdor.fault

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.kafka.trogdor.task.TaskController
import org.apache.kafka.trogdor.task.TaskSpec
import org.apache.kafka.trogdor.task.TaskWorker

/**
 * The specification for a fault that makes files unreadable.
 */
class FilesUnreadableFaultSpec @JsonCreator constructor(
    @JsonProperty("startMs") startMs: Long,
    @JsonProperty("durationMs") durationMs: Long,
    @JsonProperty("nodeNames") nodeNames: Set<String>?,
    @JsonProperty("mountPath") mountPath: String?,
    @JsonProperty("prefix") prefix: String?,
    @param:JsonProperty("errorCode") private val errorCode: Int,
) : TaskSpec(startMs, durationMs) {

    private val nodeNames: Set<String> = nodeNames ?: emptySet()

    private val mountPath: String = mountPath ?: ""

    private val prefix: String = prefix ?: ""

    @JsonProperty
    fun nodeNames(): Set<String> = nodeNames

    @JsonProperty
    fun mountPath(): String = mountPath

    @JsonProperty
    fun prefix(): String = prefix

    @JsonProperty
    fun errorCode(): Int = errorCode

    override fun newController(id: String?): TaskController {
        return KiboshFaultController(nodeNames)
    }

    override fun newTaskWorker(id: String): TaskWorker = KiboshFaultWorker(
        id,
        Kibosh.KiboshFilesUnreadableFaultSpec(prefix, errorCode),
        mountPath,
    )
}
