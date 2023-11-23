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

import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.DescribeAclsRequestData
import org.apache.kafka.common.message.DescribeAclsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import java.nio.ByteBuffer

class DescribeAclsRequest private constructor(
    private val data: DescribeAclsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DESCRIBE_ACLS, version) {

    init {
        normalizeAndValidate(version)
    }

    private fun normalizeAndValidate(version: Short) {
        if (version.toInt() == 0) {
            val patternType = PatternType.fromCode(data.patternTypeFilter)
            // On older brokers, no pattern types existed except LITERAL (effectively). So even
            // though ANY is not directly supported on those brokers, we can get the same effect as
            // ANY by setting the pattern type to LITERAL. Note that the wildcard `*` is considered
            // `LITERAL` for compatibility reasons.
            if (patternType === PatternType.ANY) data.setPatternTypeFilter(PatternType.LITERAL.code)
            else if (patternType !== PatternType.LITERAL) throw UnsupportedVersionException(
                "Version 0 only supports literal resource pattern types"
            )
        }

        require(
            data.patternTypeFilter != PatternType.UNKNOWN.code
                    && data.resourceTypeFilter != ResourceType.UNKNOWN.code
                    && data.permissionType != AclPermissionType.UNKNOWN.code
                    && data.operation != AclOperation.UNKNOWN.code
        ) { "DescribeAclsRequest contains UNKNOWN elements: $data" }
    }

    override fun data(): DescribeAclsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val (error1, message) = ApiError.fromThrowable(e)
        val response = DescribeAclsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error1.code)
            .setErrorMessage(message)

        return DescribeAclsResponse(response, version)
    }

    fun filter(): AclBindingFilter {
        val rpf = ResourcePatternFilter(
            ResourceType.fromCode(data.resourceTypeFilter),
            data.resourceNameFilter,
            PatternType.fromCode(data.patternTypeFilter)
        )
        val acef = AccessControlEntryFilter(
            data.principalFilter,
            data.hostFilter,
            AclOperation.fromCode(data.operation),
            AclPermissionType.fromCode(data.permissionType)
        )
        return AclBindingFilter(rpf, acef)
    }

    class Builder(
        filter: AclBindingFilter
    ) : AbstractRequest.Builder<DescribeAclsRequest>(ApiKeys.DESCRIBE_ACLS) {

        private val data: DescribeAclsRequestData

        init {
            val (resourceType, name, patternType) = filter.patternFilter
            val (principal, host, operation, permissionType) = filter.entryFilter
            data = DescribeAclsRequestData()
                .setHostFilter(host)
                .setOperation(operation.code)
                .setPermissionType(permissionType.code)
                .setPrincipalFilter(principal)
                .setResourceNameFilter(name)
                .setPatternTypeFilter(patternType.code)
                .setResourceTypeFilter(resourceType.code)
        }

        override fun build(version: Short): DescribeAclsRequest = DescribeAclsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DescribeAclsRequest =
            DescribeAclsRequest(
                DescribeAclsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )
    }
}
