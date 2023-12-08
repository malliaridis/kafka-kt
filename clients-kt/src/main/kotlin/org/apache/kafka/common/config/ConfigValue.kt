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

package org.apache.kafka.common.config

data class ConfigValue(
    val name: String,
    var value: Any? = null,
    var recommendedValues: List<Any> = emptyList(),
    private var errorMessages: List<String?> = emptyList(),
    var visible: Boolean = true,
) {

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("name")
    )
    fun name(): String = name

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("value")
    )
    fun value(): Any? = value

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("recommendedValues")
    )
    fun recommendedValues(): List<Any> = recommendedValues

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("errorMessages")
    )
    fun errorMessages(): List<String?> = errorMessages

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("visible")
    )
    fun visible(): Boolean = visible

    @Deprecated("Use property instead")
    fun value(value: Any?) {
        this.value = value
    }

    @Deprecated("Use property instead")
    fun recommendedValues(recommendedValues: List<Any>) {
        this.recommendedValues = recommendedValues
    }

    fun addErrorMessage(errorMessage: String?) {
        errorMessages += errorMessage
    }

    @Deprecated("Use property instead")
    fun visible(visible: Boolean) {
        this.visible = visible
    }

    override fun toString(): String = "[$name,$value,$recommendedValues,$errorMessages,$visible]"
}
