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

package org.apache.kafka.message

import java.io.IOException
import java.io.Writer

class CodeBuffer {

    private val lines: ArrayList<String> = ArrayList()

    private var indent = 0

    fun incrementIndent() {
        indent++
    }

    fun decrementIndent() {
        indent--
        if (indent < 0) throw RuntimeException("Indent < 0")
    }

    fun printf(format: String, vararg args: Any?) {
        lines.add(String.format(indentSpaces() + format, *args))
    }

    @Throws(IOException::class)
    fun write(writer: Writer) {
        for (line in lines) writer.write(line)
    }

    fun write(other: CodeBuffer) {
        for (line in lines) other.lines.add(other.indentSpaces() + line)
    }

    private fun indentSpaces(): String {
        val bld = StringBuilder()
        repeat(indent) { bld.append("    ") }
        return bld.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (other !is CodeBuffer) return false
        return lines == other.lines
    }

    override fun hashCode(): Int = lines.hashCode()
}
