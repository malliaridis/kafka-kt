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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.OperatingSystem
import org.apache.kafka.test.TestUtils.tempFile
import org.apache.kafka.trogdor.task.AgentWorkerStatusTracker
import org.apache.kafka.trogdor.task.WorkerStatusTracker
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class ExternalCommandWorkerTest {

    /**
     * Test running a process which exits successfully-- in this case, /bin/true.
     */
    @Test
    @Throws(Exception::class)
    fun testProcessWithNormalExit() {
        if (OperatingSystem.IS_WINDOWS) return
        val worker = ExternalCommandWorkerBuilder("trueTask")
            .command("true")
            .build()
        val doneFuture = KafkaFutureImpl<String>()
        worker.start(platform = null, status = AgentWorkerStatusTracker(), haltFuture = doneFuture)
        assertEquals(expected = "", actual = doneFuture.get())
        worker.stop(platform = null)
    }

    /**
     * Test running a process which exits unsuccessfully-- in this case, /bin/false.
     */
    @Test
    @Throws(Exception::class)
    fun testProcessWithFailedExit() {
        if (OperatingSystem.IS_WINDOWS) return
        val worker = ExternalCommandWorkerBuilder("falseTask")
            .command("false")
            .build()
        val doneFuture = KafkaFutureImpl<String>()
        worker.start(platform = null, status = AgentWorkerStatusTracker(), haltFuture = doneFuture)
        assertEquals(expected = "exited with return code 1", actual = doneFuture.get())
        worker.stop(platform = null)
    }

    /**
     * Test attempting to run an executable which doesn't exist.
     * We use a path which starts with /dev/null, since that should never be a
     * directory in UNIX.
     */
    @Test
    @Throws(Exception::class)
    fun testProcessNotFound() {
        val worker = ExternalCommandWorkerBuilder("notFoundTask")
            .command("/dev/null/non/existent/script/path")
            .build()
        val doneFuture = KafkaFutureImpl<String>()
        worker.start(platform = null, status = AgentWorkerStatusTracker(), haltFuture = doneFuture)
        val errorString = doneFuture.get()
        assertTrue(errorString.startsWith("Unable to start process"))
        worker.stop(platform = null)
    }

    /**
     * Test running a process which times out.  We will send it a SIGTERM.
     */
    @Test
    @Throws(Exception::class)
    fun testProcessStop() {
        if (OperatingSystem.IS_WINDOWS) return
        val worker = ExternalCommandWorkerBuilder("testStopTask")
            .command("sleep", "3600000")
            .build()
        val doneFuture = KafkaFutureImpl<String>()
        worker.start(platform = null, status = AgentWorkerStatusTracker(), haltFuture = doneFuture)
        worker.stop(platform = null)
        // We don't check the numeric return code, since that will vary based on platform.
        assertTrue(doneFuture.get().startsWith("exited with return code "))
    }

    /**
     * Test running a process which needs to be force-killed.
     */
    @Test
    @Throws(Exception::class)
    fun testProcessForceKillTimeout() {
        if (OperatingSystem.IS_WINDOWS) return
        var tempFile: File? = null
        try {
            tempFile = tempFile()
            Files.newOutputStream(tempFile.toPath()).use { stream ->
                for (line in arrayOf(
                    "echo hello world\n",
                    "# Test that the initial message is sent correctly.\n",
                    "read -r line\n",
                    "[[ \$line == '{\"id\":\"testForceKillTask\",\"workload\":{\"foo\":\"value1\",\"bar\":123}}' ]] || exit 0\n",
                    "\n",
                    "# Ignore SIGTERM signals.  This ensures that we test SIGKILL delivery.\n",
                    "trap 'echo SIGTERM' SIGTERM\n",
                    "\n",
                    "# Update the process status.  This will also unblock the junit test.\n",
                    "# It is important that we do this after we disabled SIGTERM, to ensure\n",
                    "# that we are testing SIGKILL.\n",
                    "echo '{\"status\": \"green\", \"log\": \"my log message.\"}'\n",
                    "\n",
                    "# Wait for the SIGKILL.\n",
                    "while true; do sleep 0.01; done\n"
                )) stream.write(line.toByteArray())
            }

            val statusFuture = CompletableFuture<String>()
            val statusTracker = WorkerStatusTracker { status -> statusFuture.complete(status.textValue()) }
            val worker = ExternalCommandWorkerBuilder("testForceKillTask")
                .shutdownGracePeriodMs(1)
                .command("bash", tempFile.absolutePath)
                .build()
            val doneFuture = KafkaFutureImpl<String>()
            worker.start(null, statusTracker, doneFuture)
            assertEquals("green", statusFuture.get())
            worker.stop(null)
            assertTrue(doneFuture.get().startsWith("exited with return code "))
        } finally {
            tempFile?.let { file -> Files.delete(file.toPath()) }
        }
    }

    internal class ExternalCommandWorkerBuilder(private val id: String) {

        private var shutdownGracePeriodMs = 3000000

        private var command = emptyList<String>()

        private val workload = ObjectNode(JsonNodeFactory.instance).apply {
            set<JsonNode>("foo", TextNode("value1"))
            set<JsonNode>("bar", IntNode(123))
        }

        fun build(): ExternalCommandWorker {
            val spec = ExternalCommandSpec(
                startMs = 0,
                durationMs = 30000,
                commandNode = "node0",
                command = command,
                workload = workload,
                shutdownGracePeriodMs = shutdownGracePeriodMs,
            )
            return ExternalCommandWorker(id, spec)
        }

        fun command(vararg command: String): ExternalCommandWorkerBuilder {
            this.command = command.toList()
            return this
        }

        fun shutdownGracePeriodMs(shutdownGracePeriodMs: Int): ExternalCommandWorkerBuilder {
            this.shutdownGracePeriodMs = shutdownGracePeriodMs
            return this
        }
    }
}
