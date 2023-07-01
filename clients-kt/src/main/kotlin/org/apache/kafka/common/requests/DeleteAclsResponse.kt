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
import org.apache.kafka.common.message.DeleteAclsResponseData
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.AclDeleteResult
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

class DeleteAclsResponse(
    private val data: DeleteAclsResponseData,
    version: Short,
) : AbstractResponse(ApiKeys.DELETE_ACLS) {

    init {
        validate(version)
    }

    override fun data(): DeleteAclsResponseData = data

    override fun throttleTimeMs(): Int = data.throttleTimeMs()

    override fun maybeSetThrottleTimeMs(throttleTimeMs: Int) {
        data.setThrottleTimeMs(throttleTimeMs)
    }

    fun filterResults(): List<DeleteAclsFilterResult> = data.filterResults()

    override fun errorCounts(): Map<Errors, Int> =
        errorCounts(filterResults().map { Errors.forCode(it.errorCode()) })

    override fun toString(): String = data.toString()

    override fun shouldClientThrottle(version: Short): Boolean = version >= 1

    private fun validate(version: Short) {
        if (version.toInt() == 0) {
            val unsupported = filterResults().flatMap(DeleteAclsFilterResult::matchingAcls)
                .any { matchingAcl -> matchingAcl.patternType() != PatternType.LITERAL.code }
            if (unsupported) throw UnsupportedVersionException(
                "Version 0 only supports literal resource pattern types"
            )
        }

        val unknown = filterResults()
            .flatMap(DeleteAclsFilterResult::matchingAcls)
            .any { matchingAcl ->
                matchingAcl.patternType() == PatternType.UNKNOWN.code
                        || matchingAcl.resourceType() == ResourceType.UNKNOWN.code
                        || matchingAcl.permissionType() == AclPermissionType.UNKNOWN.code
                        || matchingAcl.operation() == AclOperation.UNKNOWN.code
            }

        require(!unknown) { "DeleteAclsMatchingAcls contain UNKNOWN elements" }
    }

    companion object {

        val log: Logger = LoggerFactory.getLogger(DeleteAclsResponse::class.java)

        fun parse(buffer: ByteBuffer?, version: Short): DeleteAclsResponse {
            return DeleteAclsResponse(
                DeleteAclsResponseData(ByteBufferAccessor(buffer!!), version),
                version
            )
        }

        fun filterResult(result: AclDeleteResult): DeleteAclsFilterResult {
            val (error1, message) = result.exception?.let { e -> ApiError.fromThrowable(e) }
                ?: ApiError.NONE

            val matchingAcls = result.deleteResults.map { matchingAcl(it) }

            return DeleteAclsFilterResult()
                .setErrorCode(error1.code)
                .setErrorMessage(message)
                .setMatchingAcls(matchingAcls)
        }

        private fun matchingAcl(result: AclBindingDeleteResult): DeleteAclsMatchingAcl {
            val error: ApiError =
                result.exception?.let { e -> ApiError.fromThrowable(e) } ?: ApiError.NONE

            return matchingAcl(result.aclBinding, error)
        }

        // Visible for testing
        fun matchingAcl(acl: AclBinding, apiError: ApiError): DeleteAclsMatchingAcl {
            return DeleteAclsMatchingAcl()
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
                .setResourceName(acl.pattern.name)
                .setResourceType(acl.pattern.resourceType.code)
                .setPatternType(acl.pattern.patternType.code)
                .setHost(acl.entry.host)
                .setOperation(acl.entry.operation.code)
                .setPermissionType(acl.entry.permissionType.code)
                .setPrincipal(acl.entry.principal)
        }

        fun aclBinding(matchingAcl: DeleteAclsMatchingAcl): AclBinding {
            val resourcePattern = ResourcePattern(
                resourceType = ResourceType.fromCode(matchingAcl.resourceType()),
                name = matchingAcl.resourceName(),
                patternType = PatternType.fromCode(matchingAcl.patternType()),
            )
            val accessControlEntry = AccessControlEntry(
                principal = matchingAcl.principal(),
                host = matchingAcl.host(),
                operation = AclOperation.fromCode(matchingAcl.operation()),
                permissionType = AclPermissionType.fromCode(matchingAcl.permissionType()),
            )
            return AclBinding(resourcePattern, accessControlEntry)
        }
    }
}
