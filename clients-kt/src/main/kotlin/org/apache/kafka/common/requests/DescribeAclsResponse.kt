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

package org.apache.kafka.common.requests

import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.DescribeAclsResponseData
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

class DescribeAclsResponse : AbstractResponse {

    private val data: DescribeAclsResponseData

    constructor(
        data: DescribeAclsResponseData,
        version: Short,
    ) : super(ApiKeys.DESCRIBE_ACLS) {
        this.data = data
        validate(version)
    }

    // Skips version validation, visible for testing
    internal constructor(data: DescribeAclsResponseData) : super(ApiKeys.DESCRIBE_ACLS) {
        this.data = data
        validate(null)
    }

    override fun data(): DescribeAclsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("error"),
    )
    fun error(): ApiError = ApiError(Errors.forCode(data.errorCode()), data.errorMessage())

    val error: ApiError
        get() = ApiError(Errors.forCode(data.errorCode()), data.errorMessage())

    override fun errorCounts(): Map<Errors, Int> = errorCounts(Errors.forCode(data.errorCode()))

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("acls"),
    )
    fun acls(): List<DescribeAclsResource> = data.resources()

    val acls: List<DescribeAclsResource>
        get() = data.resources()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    private fun validate(version: Short?) {
        if (
            version?.toInt() == 0
            && acls.any { acl -> acl.patternType() != PatternType.LITERAL.code }
        ) throw UnsupportedVersionException("Version 0 only supports literal resource pattern types")

        require(
            acls.none { resource ->
                resource.patternType() == PatternType.UNKNOWN.code
                        || resource.resourceType() == ResourceType.UNKNOWN.code
                        || resource.acls().none { acl ->
                    acl.operation() == AclOperation.UNKNOWN.code
                            || acl.permissionType() == AclPermissionType.UNKNOWN.code
                }
            }
        ) { "Contain UNKNOWN elements" }
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeAclsResponse =
            DescribeAclsResponse(
                DescribeAclsResponseData(ByteBufferAccessor(buffer), version),
                version,
            )

        private fun aclBindings(resource: DescribeAclsResource): List<AclBinding> {
            return resource.acls().map { acl: AclDescription ->
                val pattern =
                    ResourcePattern(
                        ResourceType.fromCode(resource.resourceType()),
                        resource.resourceName(),
                        PatternType.fromCode(resource.patternType())
                    )
                val entry = AccessControlEntry(
                    acl.principal(),
                    acl.host(),
                    AclOperation.fromCode(acl.operation()),
                    AclPermissionType.fromCode(acl.permissionType())
                )

                AclBinding(pattern, entry)
            }
        }

        fun aclBindings(resources: List<DescribeAclsResource>): List<AclBinding> =
            resources.flatMap(::aclBindings)

        fun aclsResources(acls: Collection<AclBinding>): List<DescribeAclsResource> {
            val patternToEntries = acls.groupBy(
                keySelector = { it.pattern },
                valueTransform = { it.entry },
            )

            return patternToEntries.map { (key, value) ->
                val aclDescriptions = value.map { (principal, host, operation, permissionType) ->
                    AclDescription()
                        .setHost(host)
                        .setOperation(operation.code)
                        .setPermissionType(permissionType.code)
                        .setPrincipal(principal)
                }

                DescribeAclsResource()
                    .setResourceName(key.name)
                    .setPatternType(key.patternType.code)
                    .setResourceType(key.resourceType.code)
                    .setAcls(aclDescriptions)
            }
        }
    }
}
