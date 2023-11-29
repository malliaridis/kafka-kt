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

package org.apache.kafka.common.config.internals

import java.util.*
import java.util.function.Consumer
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.scram.internals.ScramMechanism.Companion.mechanismNames

/**
 * Define the dynamic quota configs. Note that these are not normal configurations that exist in properties files. They
 * only exist dynamically in the controller (or ZK, depending on which mode the cluster is running).
 */
object QuotaConfigs {

    const val PRODUCER_BYTE_RATE_OVERRIDE_CONFIG = "producer_byte_rate"

    const val CONSUMER_BYTE_RATE_OVERRIDE_CONFIG = "consumer_byte_rate"

    const val REQUEST_PERCENTAGE_OVERRIDE_CONFIG = "request_percentage"

    const val CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG = "controller_mutation_rate"

    const val IP_CONNECTION_RATE_OVERRIDE_CONFIG = "connection_creation_rate"

    const val PRODUCER_BYTE_RATE_DOC =
        "A rate representing the upper bound (bytes/sec) for producer traffic."

    const val CONSUMER_BYTE_RATE_DOC =
        "A rate representing the upper bound (bytes/sec) for consumer traffic."

    const val REQUEST_PERCENTAGE_DOC =
        "A percentage representing the upper bound of time spent for processing requests."

    const val CONTROLLER_MUTATION_RATE_DOC = "The rate at which mutations are accepted for the " +
            "create topics request, the create partitions request and the delete topics request. " +
            "The rate is accumulated by the number of partitions created or deleted."

    const val IP_CONNECTION_RATE_DOC =
        "An int representing the upper bound of connections accepted " +
                "for the specified IP."

    const val IP_CONNECTION_RATE_DEFAULT = Int.MAX_VALUE

    private val userClientConfigNames: Set<String> = hashSetOf(
        PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
        REQUEST_PERCENTAGE_OVERRIDE_CONFIG, CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG
    )

    private fun buildUserClientQuotaConfigDef(configDef: ConfigDef) {
        configDef.define(
            name = PRODUCER_BYTE_RATE_OVERRIDE_CONFIG,
            type = ConfigDef.Type.LONG,
            defaultValue = Long.MAX_VALUE,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = PRODUCER_BYTE_RATE_DOC
        )
        configDef.define(
            name = CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
            type = ConfigDef.Type.LONG,
            defaultValue = Long.MAX_VALUE,
            importance = ConfigDef.Importance.MEDIUM,
            documentation = CONSUMER_BYTE_RATE_DOC
        )
        configDef.define(
            name = REQUEST_PERCENTAGE_OVERRIDE_CONFIG,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = Integer.valueOf(Int.MAX_VALUE).toDouble(),
            importance = ConfigDef.Importance.MEDIUM,
            documentation = REQUEST_PERCENTAGE_DOC
        )
        configDef.define(
            name = CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG,
            type = ConfigDef.Type.DOUBLE,
            defaultValue = Integer.valueOf(Int.MAX_VALUE).toDouble(),
            importance = ConfigDef.Importance.MEDIUM,
            documentation = CONTROLLER_MUTATION_RATE_DOC
        )
    }

    fun isClientOrUserConfig(name: String): Boolean {
        return userClientConfigNames.contains(name)
    }

    fun userConfigs(): ConfigDef {
        val configDef = ConfigDef()
        mechanismNames.forEach(
            Consumer { mechanismName: String ->
                configDef.define(
                    name = mechanismName,
                    type = ConfigDef.Type.STRING,
                    defaultValue = null,
                    importance = ConfigDef.Importance.MEDIUM,
                    documentation = "User credentials for SCRAM mechanism $mechanismName"
                )
            })
        buildUserClientQuotaConfigDef(configDef)
        return configDef
    }

    fun clientConfigs(): ConfigDef {
        val configDef = ConfigDef()
        buildUserClientQuotaConfigDef(configDef)
        return configDef
    }

    fun ipConfigs(): ConfigDef {
        val configDef = ConfigDef()
        configDef.define(
            name = IP_CONNECTION_RATE_OVERRIDE_CONFIG,
            type = ConfigDef.Type.INT,
            defaultValue = Int.MAX_VALUE,
            validator = ConfigDef.Range.atLeast(0),
            importance = ConfigDef.Importance.MEDIUM,
            documentation = IP_CONNECTION_RATE_DOC
        )
        return configDef
    }
}
