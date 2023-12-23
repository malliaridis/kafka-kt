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

package org.apache.kafka.trogdor.common

import java.io.IOException
import java.util.LinkedList
import org.apache.kafka.trogdor.basic.BasicPlatform.CommandRunner
import org.slf4j.LoggerFactory

class CapturingCommandRunner : CommandRunner {

    private val commands = mutableMapOf<String, MutableList<String>>()

    @Synchronized
    private fun getOrCreate(nodeName: String): MutableList<String> {
        var lines = commands[nodeName]
        if (lines != null) return lines

        lines = LinkedList()
        commands[nodeName] = lines
        return lines
    }

    @Throws(IOException::class)
    override fun run(curNode: Node, command: Array<String>): String {
        val line = command.joinToString(" ")
        synchronized(this) { getOrCreate(curNode.name()).add(line) }
        log.debug("RAN {}: {}", curNode, command.joinToString(" "))
        return ""
    }

    @Synchronized
    fun lines(nodeName: String): List<String> = getOrCreate(nodeName).toList()

    companion object {
        private val log = LoggerFactory.getLogger(CapturingCommandRunner::class.java)
    }
}
