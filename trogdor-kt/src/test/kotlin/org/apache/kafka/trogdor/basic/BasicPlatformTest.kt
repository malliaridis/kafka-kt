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

package org.apache.kafka.trogdor.basic

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.utils.Utils.join
import org.apache.kafka.test.TestUtils.tempFile
import org.apache.kafka.trogdor.common.Platform.Config.parse
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@Timeout(value = 120000, unit = TimeUnit.MILLISECONDS)
class BasicPlatformTest {

    @Test
    @Throws(Exception::class)
    fun testCreateBasicPlatform() {
        val configFile = tempFile()
        try {
            OutputStreamWriter(Files.newOutputStream(configFile.toPath()), Charsets.UTF_8).use { writer ->
                writer.write("{\n")
                writer.write("  \"platform\": \"org.apache.kafka.trogdor.basic.BasicPlatform\",\n")
                writer.write("  \"nodes\": {\n")
                writer.write("    \"bob01\": {\n")
                writer.write("      \"hostname\": \"localhost\",\n")
                writer.write("      \"trogdor.agent.port\": 8888\n")
                writer.write("    },\n")
                writer.write("    \"bob02\": {\n")
                writer.write("      \"hostname\": \"localhost\",\n")
                writer.write("      \"trogdor.agent.port\": 8889\n")
                writer.write("    }\n")
                writer.write("  }\n")
                writer.write("}\n")
            }
            val platform = parse("bob01", configFile.path)
            Assertions.assertEquals("BasicPlatform", platform.name())
            Assertions.assertEquals(2, platform.topology().nodes().size)
            Assertions.assertEquals("bob01, bob02", platform.topology().nodes().keys.joinToString())
        } finally {
            Files.delete(configFile.toPath())
        }
    }
}
