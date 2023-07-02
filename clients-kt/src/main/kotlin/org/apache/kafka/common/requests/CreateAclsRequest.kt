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
import org.apache.kafka.common.message.CreateAclsRequestData
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation
import org.apache.kafka.common.message.CreateAclsResponseData
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import java.nio.ByteBuffer
import java.util.*

class CreateAclsRequest internal constructor(
    data: CreateAclsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.CREATE_ACLS, version) {

    private val data: CreateAclsRequestData

    init {
        validate(data)
        this.data = data
    }

    fun aclCreations(): List<AclCreation> = data.creations()

    override fun data(): CreateAclsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val result = aclResult(e)

        val results = Collections.nCopies(data.creations().size, result)
        return CreateAclsResponse(
            CreateAclsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResults(results)
        )
    }

    private fun validate(data: CreateAclsRequestData) {
        if (version.toInt() == 0) {
            val unsupported = data.creations()
                .any { creation -> creation.resourcePatternType() != PatternType.LITERAL.code }
            if (unsupported) throw UnsupportedVersionException(
                "Version 0 only supports literal resource pattern types"
            )
        }
        val unknown = data.creations().any { creation ->
            creation.resourcePatternType() == PatternType.UNKNOWN.code
                    || creation.resourceType() == ResourceType.UNKNOWN.code
                    || creation.permissionType() == AclPermissionType.UNKNOWN.code
                    || creation.operation() == AclOperation.UNKNOWN.code
        }

        require(!unknown) { "CreatableAcls contain unknown elements: " + data.creations() }
    }

    class Builder(
        private val data: CreateAclsRequestData,
    ) : AbstractRequest.Builder<CreateAclsRequest>(ApiKeys.CREATE_ACLS) {

        override fun build(version: Short): CreateAclsRequest = CreateAclsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): CreateAclsRequest =
            CreateAclsRequest(CreateAclsRequestData(ByteBufferAccessor(buffer), version), version)

        fun aclBinding(acl: AclCreation): AclBinding {
            val pattern = ResourcePattern(
                ResourceType.fromCode(acl.resourceType()),
                acl.resourceName(),
                PatternType.fromCode(acl.resourcePatternType())
            )
            val entry = AccessControlEntry(
                acl.principal(),
                acl.host(),
                AclOperation.fromCode(acl.operation()),
                AclPermissionType.fromCode(acl.permissionType())
            )

            return AclBinding(pattern, entry)
        }

        fun aclCreation(binding: AclBinding): AclCreation = AclCreation()
            .setHost(binding.entry.host)
            .setOperation(binding.entry.operation.code)
            .setPermissionType(binding.entry.permissionType.code)
            .setPrincipal(binding.entry.principal)
            .setResourceName(binding.pattern.name)
            .setResourceType(binding.pattern.resourceType.code)
            .setResourcePatternType(binding.pattern.patternType.code)

        private fun aclResult(throwable: Throwable): AclCreationResult {
            val (error, message) = ApiError.fromThrowable(throwable)

            return AclCreationResult()
                .setErrorCode(error.code)
                .setErrorMessage(message)
        }
    }
}
