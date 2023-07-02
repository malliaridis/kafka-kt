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

import javax.security.auth.login.Configuration
import org.apache.kafka.common.KafkaException
import org.slf4j.LoggerFactory

object JaasUtils {

    private val LOG = LoggerFactory.getLogger(JaasUtils::class.java)

    const val JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config"

    const val DISALLOWED_LOGIN_MODULES_CONFIG = "org.apache.kafka.disallowed.login.modules"

    const val DISALLOWED_LOGIN_MODULES_DEFAULT = "com.sun.security.auth.module.JndiLoginModule"

    const val SERVICE_NAME = "serviceName"

    const val ZK_SASL_CLIENT = "zookeeper.sasl.client"

    const val ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig"

    private const val DEFAULT_ZK_LOGIN_CONTEXT_NAME = "Client"

    private const val DEFAULT_ZK_SASL_CLIENT = "true"

    fun zkSecuritySysConfigString(): String {
        val loginConfig = System.getProperty(JAVA_LOGIN_CONFIG_PARAM)
        val clientEnabled = System.getProperty(ZK_SASL_CLIENT, "default:$DEFAULT_ZK_SASL_CLIENT")
        val contextName = System.getProperty(
            ZK_LOGIN_CONTEXT_NAME_KEY,
            "default:$DEFAULT_ZK_LOGIN_CONTEXT_NAME"
        )

        return "[" +
                JAVA_LOGIN_CONFIG_PARAM + "=" + loginConfig +
                ", " +
                ZK_SASL_CLIENT + "=" + clientEnabled +
                ", " +
                ZK_LOGIN_CONTEXT_NAME_KEY + "=" + contextName +
                "]"
    }

    val isZkSaslEnabled: Boolean
        get() {
            // Technically a client must also check if TLS mutual authentication has been
            // configured, but we will leave that up to the client code to determine since direct
            // connectivity to ZooKeeper has been deprecated in many clients, and we don't wish to
            // re-introduce a ZooKeeper jar dependency here.
            val zkSaslEnabled = java.lang.Boolean.parseBoolean(
                System.getProperty(
                    ZK_SASL_CLIENT,
                    DEFAULT_ZK_SASL_CLIENT
                )
            )
            val zkLoginContextName =
                System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, DEFAULT_ZK_LOGIN_CONTEXT_NAME)
            LOG.debug(
                "Checking login config for Zookeeper JAAS context {}",
                zkSecuritySysConfigString()
            )

            val foundLoginConfigEntry: Boolean = try {
                val loginConf = Configuration.getConfiguration()
                loginConf.getAppConfigurationEntry(zkLoginContextName) != null
            } catch (e: Exception) {
                throw KafkaException(
                    "Exception while loading Zookeeper JAAS login context ${zkSecuritySysConfigString()}",
                    e
                )
            }

            if (foundLoginConfigEntry && !zkSaslEnabled) {
                LOG.error(
                    "JAAS configuration is present, but system property {} " +
                            "is set to false, which disables SASL in the ZooKeeper client",
                    ZK_SASL_CLIENT
                )
                throw KafkaException(
                    "Exception while determining if ZooKeeper is secure ${zkSecuritySysConfigString()}"
                )
            }
            return foundLoginConfigEntry
        }
}
