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

package org.apache.kafka.common.protocol

import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.message.ResponseHeaderData
import org.apache.kafka.common.protocol.types.BoundField
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.TaggedFields
import org.apache.kafka.common.protocol.types.Type

object Protocol {

    private fun indentString(size: Int): String {
        val b = StringBuilder(size)
        for (i in 0 until size) b.append(" ")
        return b.toString()
    }

    private fun schemaToBnfHtml(schema: Schema, b: StringBuilder, indentSize: Int) {
        val indentStr = indentString(indentSize)
        val subTypes: MutableMap<String, Type> = LinkedHashMap()

        // Top level fields
        for (field in schema.fields()) {
            val type = field.def.type
            if (type.isArray) {
                b.append("[")
                b.append(field.def.name)
                b.append("] ")
                if (!subTypes.containsKey(field.def.name))
                    subTypes[field.def.name] = type.arrayElementType().get()

            } else if (type is TaggedFields) b.append("TAG_BUFFER ")
            else {
                b.append(field.def.name)
                b.append(" ")
                if (!subTypes.containsKey(field.def.name)) subTypes[field.def.name] = type
            }
        }
        b.append("\n")

        // Sub Types/Schemas
        for ((key, value) in subTypes) {
            if (value is Schema) {
                // Complex Schema Type
                b.append(indentStr)
                b.append(key)
                b.append(" => ")
                schemaToBnfHtml(value, b, indentSize + 2)
            } else {
                // Standard Field Type
                b.append(indentStr)
                b.append(key)
                b.append(" => ")
                b.append(value)
                b.append("\n")
            }
        }
    }

    private fun populateSchemaFields(schema: Schema, fields: MutableSet<BoundField>) {
        for (field in schema.fields()) {
            fields.add(field)
            if (field.def.type.isArray) {
                val innerType = field.def.type.arrayElementType().get()
                if (innerType is Schema) populateSchemaFields(innerType, fields)
            } else if (field.def.type is Schema)
                populateSchemaFields(field.def.type, fields)
        }
    }

    private fun schemaToFieldTableHtml(schema: Schema, b: StringBuilder) {
        val fields: MutableSet<BoundField> = LinkedHashSet()
        populateSchemaFields(schema, fields)
        b.append("<table class=\"data-table\"><tbody>\n")
        b.append("<tr>")
        b.append("<th>Field</th>\n")
        b.append("<th>Description</th>\n")
        b.append("</tr>")
        for (field in fields) {
            b.append("<tr>\n")
            b.append("<td>")
            b.append(field.def.name)
            b.append("</td>")
            b.append("<td>")
            b.append(field.def.docString)
            b.append("</td>")
            b.append("</tr>\n")
        }
        b.append("</tbody></table>\n")
    }

    fun toHtml(): String {
        val b = StringBuilder()
        b.append("<h5>Headers:</h5>\n")
        for (i in RequestHeaderData.SCHEMAS.indices) {
            b.append("<pre>")
            b.append("Request Header v").append(i).append(" => ")
            schemaToBnfHtml(RequestHeaderData.SCHEMAS[i], b, 2)
            b.append("</pre>\n")
            schemaToFieldTableHtml(RequestHeaderData.SCHEMAS[i], b)
        }
        for (i in ResponseHeaderData.SCHEMAS.indices) {
            b.append("<pre>")
            b.append("Response Header v").append(i).append(" => ")
            schemaToBnfHtml(ResponseHeaderData.SCHEMAS[i], b, 2)
            b.append("</pre>\n")
            schemaToFieldTableHtml(ResponseHeaderData.SCHEMAS[i], b)
        }
        for (key in ApiKeys.clientApis()) {
            // Key
            b.append("<h5>")
            b.append("<a name=\"The_Messages_" + key.name + "\">")
            b.append(key.name)
            b.append(" API (Key: ")
            b.append(key.id.toInt())
            b.append("):</a></h5>\n\n")
            // Requests
            b.append("<b>Requests:</b><br>\n")
            val requests = key.messageType.requestSchemas()
            for (i in requests.indices) {
                val schema = requests[i]
                // Schema
                if (schema != null) {
                    b.append("<div>")
                    // Version header
                    b.append("<pre>")
                    b.append(key.name)
                    b.append(" Request (Version: ")
                    b.append(i)
                    b.append(") => ")
                    schemaToBnfHtml(requests[i], b, 2)
                    b.append("</pre>")
                    schemaToFieldTableHtml(requests[i], b)
                }
                b.append("</div>\n")
            }

            // Responses
            b.append("<b>Responses:</b><br>\n")
            val responses = key.messageType.responseSchemas()
            for (i in responses.indices) {
                val schema = responses[i]
                // Schema
                if (schema != null) {
                    b.append("<div>")
                    // Version header
                    b.append("<pre>")
                    b.append(key.name)
                    b.append(" Response (Version: ")
                    b.append(i)
                    b.append(") => ")
                    schemaToBnfHtml(responses[i], b, 2)
                    b.append("</pre>")
                    schemaToFieldTableHtml(responses[i], b)
                }
                b.append("</div>\n")
            }
        }
        return b.toString()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        println(toHtml())
    }
}
