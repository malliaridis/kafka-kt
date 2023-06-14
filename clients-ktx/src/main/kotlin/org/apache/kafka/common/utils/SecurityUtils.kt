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

package org.apache.kafka.common.utils

import java.security.Security
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProviderCreator
import org.slf4j.LoggerFactory

object SecurityUtils {

    private val LOGGER = LoggerFactory.getLogger(SecurityConfig::class.java)

    private val NAME_TO_RESOURCE_TYPES: MutableMap<String, ResourceType> =
        HashMap(ResourceType.values().size)

    private val NAME_TO_OPERATIONS: MutableMap<String, AclOperation> =
        HashMap(AclOperation.values().size)

    private val NAME_TO_PERMISSION_TYPES: MutableMap<String, AclPermissionType> =
        HashMap(AclOperation.values().size)

    init {
        ResourceType.values().forEach { resourceType ->
            val resourceTypeName = toPascalCase(resourceType.name)
            NAME_TO_RESOURCE_TYPES[resourceTypeName] = resourceType
            NAME_TO_RESOURCE_TYPES[resourceTypeName.uppercase()] = resourceType
        }

        AclOperation.values().forEach { operation ->
            val operationName = toPascalCase(operation.name)
            NAME_TO_OPERATIONS[operationName] = operation
            NAME_TO_OPERATIONS[operationName.uppercase()] = operation
        }

        AclPermissionType.values().forEach { permissionType ->
            val permissionName = toPascalCase(permissionType.name)
            NAME_TO_PERMISSION_TYPES[permissionName] = permissionType
            NAME_TO_PERMISSION_TYPES[permissionName.uppercase()] = permissionType
        }
    }

    fun parseKafkaPrincipal(str: String): KafkaPrincipal {
        require(str.isNotEmpty()) {
            "expected a string in format principalType:principalName but got $str"
        }

        val split = str.split(":".toRegex(), limit = 2)

        require(split.size == 2) {
            "expected a string in format principalType:principalName but got $str"
        }

        return KafkaPrincipal(split[0], split[1])
    }

    fun addConfiguredSecurityProviders(configs: Map<String, *>) {
        val securityProviderClassesStr =
            configs[SecurityConfig.SECURITY_PROVIDERS_CONFIG] as String?

        if (securityProviderClassesStr == null || securityProviderClassesStr == "") return

        try {
            val securityProviderClasses =
                securityProviderClassesStr.replace("\\s+".toRegex(), "")
                    .split(",".toRegex())
                    .dropLastWhile { it.isEmpty() }
                    .toTypedArray()

            securityProviderClasses.forEachIndexed { index, clazz ->
                val securityProviderCreator =
                    Class.forName(clazz).getConstructor().newInstance() as SecurityProviderCreator

                securityProviderCreator.configure(configs)
                Security.insertProviderAt(securityProviderCreator.provider, index + 1)
            }
        } catch (e: ClassCastException) {
            LOGGER.error(
                "Creators provided through " + SecurityConfig.SECURITY_PROVIDERS_CONFIG +
                        " are expected to be sub-classes of SecurityProviderCreator"
            )
        } catch (cnfe: ClassNotFoundException) {
            LOGGER.error("Unrecognized security provider creator class", cnfe)
        } catch (e: ReflectiveOperationException) {
            LOGGER.error("Unexpected implementation of security provider creator class", e)
        }
    }

    fun resourceType(name: String): ResourceType =
        valueFromMap(NAME_TO_RESOURCE_TYPES, name, ResourceType.UNKNOWN)

    fun operation(name: String): AclOperation =
        valueFromMap(NAME_TO_OPERATIONS, name, AclOperation.UNKNOWN)

    fun permissionType(name: String): AclPermissionType =
        valueFromMap(NAME_TO_PERMISSION_TYPES, name, AclPermissionType.UNKNOWN)

    // We use Pascal-case to store these values, so lookup using provided key first to avoid
    // case conversion for the common case. For backward compatibility, also perform
    // case-insensitive look up (without underscores) by converting the key to upper-case.
    private fun <T> valueFromMap(map: Map<String, T>, key: String, unknown: T): T =
        map[key] ?: map[key.uppercase()] ?: unknown

    fun resourceTypeName(resourceType: ResourceType): String = toPascalCase(resourceType.name)

    fun operationName(operation: AclOperation): String = toPascalCase(operation.name)

    fun permissionTypeName(permissionType: AclPermissionType): String =
        toPascalCase(permissionType.name)

    private fun toPascalCase(name: String): String {
        val builder = StringBuilder()
        var capitalizeNext = true
        for (c: Char in name.toCharArray()) {
            if (c == '_') capitalizeNext = true else if (capitalizeNext) {
                builder.append(c.uppercaseChar())
                capitalizeNext = false
            } else builder.append(c.lowercaseChar())
        }
        return builder.toString()
    }

    fun authorizeByResourceTypeCheckArgs(
        op: AclOperation,
        type: ResourceType
    ) {
        require(type !== ResourceType.ANY) {
            "Must specify a non-filter resource type for authorizeByResourceType"
        }

        require(type !== ResourceType.UNKNOWN) { "Unknown resource type" }

        require(op !== AclOperation.ANY) {
            "Must specify a non-filter operation type for authorizeByResourceType"
        }

        require(op !== AclOperation.UNKNOWN) { "Unknown operation type" }
    }

    fun denyAll(pattern: ResourcePattern): Boolean =
        pattern.patternType === PatternType.LITERAL
                && (pattern.name == ResourcePattern.WILDCARD_RESOURCE)
}
