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

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import org.apache.kafka.common.KafkaException

/**
 * JAAS configuration parser that constructs a JAAS configuration object with a single login context
 * from the Kafka configuration option [org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG].
 *
 * JAAS configuration file format is described
 * [here](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).
 * The format of the property value is:
 * ```java
 * <loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;
 * ```
 */
internal class JaasConfig(
    loginContextName: String?,
    jaasConfigParams: String,
) : Configuration() {

    private val loginContextName: String?

    private val configEntries: MutableList<AppConfigurationEntry>

    init {
        val tokenizer = StreamTokenizer(StringReader(jaasConfigParams))
        tokenizer.slashSlashComments(true)
        tokenizer.slashStarComments(true)
        tokenizer.wordChars('-'.code, '-'.code)
        tokenizer.wordChars('_'.code, '_'.code)
        tokenizer.wordChars('$'.code, '$'.code)

        try {
            configEntries = ArrayList()
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                configEntries.add(parseAppConfigurationEntry(tokenizer))
            }
            require(configEntries.isNotEmpty()) { "Login module not specified in JAAS config" }
            this.loginContextName = loginContextName
        } catch (e: IOException) {
            throw KafkaException("Unexpected exception while parsing JAAS config")
        }
    }

    override fun getAppConfigurationEntry(name: String): Array<AppConfigurationEntry>? {
        return if (loginContextName == name) configEntries.toTypedArray<AppConfigurationEntry>()
        else null
    }

    private fun loginModuleControlFlag(flag: String?): AppConfigurationEntry.LoginModuleControlFlag {
        requireNotNull(flag) { "Login module control flag is not available in the JAAS config" }

        val controlFlag: AppConfigurationEntry.LoginModuleControlFlag = when (flag.uppercase()) {
            "REQUIRED" -> AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
            "REQUISITE" -> AppConfigurationEntry.LoginModuleControlFlag.REQUISITE
            "SUFFICIENT" -> AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT
            "OPTIONAL" -> AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL
            else -> throw IllegalArgumentException("Invalid login module control flag '$flag' in JAAS config")
        }
        return controlFlag
    }

    @Throws(IOException::class)
    private fun parseAppConfigurationEntry(tokenizer: StreamTokenizer): AppConfigurationEntry {
        val loginModule = tokenizer.sval

        require(tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
            "Login module control flag not specified in JAAS config"
        }

        val controlFlag = loginModuleControlFlag(tokenizer.sval)
        val options: MutableMap<String, String?> = HashMap()

        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF && tokenizer.ttype != ';'.code) {
            val key = tokenizer.sval

            require(
                !(
                    tokenizer.nextToken() != '='.code
                    || tokenizer.nextToken() == StreamTokenizer.TT_EOF
                    || tokenizer.sval == null
                )
            ) { "Value not specified for key '$key' in JAAS config" }
            val value = tokenizer.sval
            options[key] = value
        }

        require(tokenizer.ttype == ';'.code) {
            "JAAS config entry not terminated by semi-colon"
        }

        return AppConfigurationEntry(loginModule, controlFlag, options)
    }
}
