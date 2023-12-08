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

package org.apache.kafka.common.security

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs

class TestSecurityConfig(originals: Map<String, Any?>) : AbstractConfig(
    definition = CONFIG,
    originals = originals,
    doLog = false,
) {

    companion object {

        private val CONFIG: ConfigDef = ConfigDef()
            .define(
                name = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
                type = ConfigDef.Type.STRING,
                defaultValue = null,
                importance = Importance.MEDIUM,
                documentation = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC,
            )
            .define(
                name = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                type = ConfigDef.Type.LIST,
                defaultValue = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS,
                importance = Importance.MEDIUM,
                documentation = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC,
            )
            .define(
                name = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS,
                type = ConfigDef.Type.CLASS,
                defaultValue = null,
                importance = Importance.MEDIUM,
                documentation = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC,
            )
            .define(
                name = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
                type = ConfigDef.Type.CLASS,
                defaultValue = null,
                importance = Importance.MEDIUM,
                documentation = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC,
            )
            .define(
                name = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS,
                type = ConfigDef.Type.LONG,
                defaultValue = 0L,
                importance = Importance.MEDIUM,
                documentation = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC,
            )
            .define(
                name = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG,
                type = ConfigDef.Type.INT,
                defaultValue = BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE,
                importance = Importance.LOW,
                documentation = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC,
            )
            .withClientSslSupport()
            .withClientSaslSupport()
    }
}
