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

import java.util.*

/**
 * The Kafka header generator.
 */
class HeaderGenerator(private val packageName: String) {

    val buffer: CodeBuffer = CodeBuffer()

    private val imports: TreeSet<String> = TreeSet()

    private val staticImports: TreeSet<String> = TreeSet()

    fun addImport(newImport: String) {
        imports.add(newImport)
    }

    @Deprecated("Kotlin does not have static imports")
    fun addStaticImport(newImport: String) {
        staticImports.add(newImport)
    }

    fun generate() {
        buffer.printf(
            """$HEADER
            package $packageName
            
            ${ imports.joinToString(separator = "%n") { "import $it"}}
            
            """.trimIndent()
        )
    }

    companion object {

        /**
         * Default license note with a "generated code" message at the end.
         */
        private val HEADER =
            """
            /*
             * Licensed to the Apache Software Foundation (ASF) under one or more
             * contributor license agreements. See the NOTICE file distributed with
             * this work for additional information regarding copyright ownership.
             * The ASF licenses this file to You under the Apache License, Version 2.0
             * (the \"License\"); you may not use this file except in compliance with
             * the License. You may obtain a copy of the License at
             *
             *   http://www.apache.org/licenses/LICENSE-2.0
             *
             * Unless required by applicable law or agreed to in writing, software
             * distributed under the License is distributed on an \"AS IS\" BASIS,
             * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
             * See the License for the specific language governing permissions and
             * limitations under the License.
             */
            
            // THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.
            """
    }
}
