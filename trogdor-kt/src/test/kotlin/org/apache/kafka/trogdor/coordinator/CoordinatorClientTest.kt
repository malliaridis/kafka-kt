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

package org.apache.kafka.trogdor.coordinator

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import org.apache.kafka.trogdor.rest.TaskDone
import org.apache.kafka.trogdor.rest.TaskPending
import org.apache.kafka.trogdor.rest.TaskRunning
import org.apache.kafka.trogdor.rest.TaskStopping
import org.apache.kafka.trogdor.task.NoOpTaskSpec
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class CoordinatorClientTest {

    @Test
    fun testPrettyPrintTaskInfo() {
        assertEquals(
            "Will start at 2019-01-08T07:05:59.85Z",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskPending(NoOpTaskSpec(1546931159850L, 9000)),
                zoneOffset = ZoneOffset.UTC,
            )
        )
        assertEquals(
            "Started 2009-07-07T01:45:59.85Z; will stop after 9s",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskRunning(
                    spec = NoOpTaskSpec(1146931159850L, 9000),
                    startedMs = 1246931159850L,
                    status = JsonNodeFactory.instance.objectNode(),
                ),
                zoneOffset = ZoneOffset.UTC,
            )
        )
        assertEquals(
            "Started 2009-07-07T01:45:59.85Z",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskStopping(
                    spec = NoOpTaskSpec(1146931159850L, 9000),
                    startedMs = 1246931159850L,
                    status = JsonNodeFactory.instance.objectNode(),
                ),
                zoneOffset = ZoneOffset.UTC,
            )
        )
        assertEquals(
            "FINISHED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskDone(
                    spec = NoOpTaskSpec(0, 1000),
                    startedMs = 1546981159850L,
                    doneMs = 1546981169850L,
                    error = "",
                    cancelled = false,
                    status = JsonNodeFactory.instance.objectNode(),
                ),
                zoneOffset = ZoneOffset.UTC,
            )
        )
        assertEquals(
            "CANCELLED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskDone(
                    spec = NoOpTaskSpec(0, 1000),
                    startedMs = 1546981159850L,
                    doneMs = 1546981169850L,
                    error = "",
                    cancelled = true,
                    status = JsonNodeFactory.instance.objectNode(),
                ),
                zoneOffset = ZoneOffset.UTC,
            )
        )
        assertEquals(
            "FAILED at 2019-01-08T20:59:29.85Z after 10s",
            CoordinatorClient.prettyPrintTaskInfo(
                taskState = TaskDone(
                    spec = NoOpTaskSpec(0, 1000),
                    startedMs = 1546981159850L,
                    doneMs = 1546981169850L,
                    error = "foobar",
                    cancelled = true,
                    status = JsonNodeFactory.instance.objectNode(),
                ),
                zoneOffset = ZoneOffset.UTC,
            )
        )
    }
}
