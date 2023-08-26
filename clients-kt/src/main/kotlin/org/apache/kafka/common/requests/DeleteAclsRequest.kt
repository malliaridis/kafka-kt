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
import org.apache.kafka.common.message.DeleteAclsRequestData
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter
import org.apache.kafka.common.message.DeleteAclsResponseData
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import java.nio.ByteBuffer
import java.util.*

class DeleteAclsRequest private constructor(
    private val data: DeleteAclsRequestData,
    version: Short,
) : AbstractRequest(ApiKeys.DELETE_ACLS, version) {

    init {
        normalizeAndValidate()
    }

    private fun normalizeAndValidate() {
        if (version.toInt() == 0) {
            for (filter: DeleteAclsFilter in data.filters) {
                val patternType = PatternType.fromCode(filter.patternTypeFilter)

                // On older brokers, no pattern types existed except LITERAL (effectively). So even
                // though ANY is not directly supported on those brokers, we can get the same effect
                // as ANY by setting the pattern type to LITERAL. Note that the wildcard `*` is
                // considered `LITERAL` for compatibility reasons.
                if (patternType === PatternType.ANY)
                    filter.setPatternTypeFilter(PatternType.LITERAL.code)
                else if (patternType !== PatternType.LITERAL) throw UnsupportedVersionException(
                    "Version 0 does not support pattern type $patternType (only LITERAL and ANY " +
                            "are supported)"
                )
            }
        }
        val unknown = data.filters.any { filter: DeleteAclsFilter ->
            filter.patternTypeFilter == PatternType.UNKNOWN.code
                    || filter.resourceTypeFilter == ResourceType.UNKNOWN.code
                    || filter.operation == AclOperation.UNKNOWN.code
                    || filter.permissionType == AclPermissionType.UNKNOWN.code
        }
        require(!unknown) { "Filters contain UNKNOWN elements, filters: " + data.filters }
    }

    fun filters(): List<AclBindingFilter> =
        data.filters.map { filter -> aclBindingFilter(filter) }

    override fun data(): DeleteAclsRequestData = data

    override fun getErrorResponse(throttleTimeMs: Int, e: Throwable): AbstractResponse {
        val apiError = ApiError.fromThrowable(e)
        val filterResults = Collections.nCopies(
            data.filters.size,
            DeleteAclsFilterResult()
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
        )

        return DeleteAclsResponse(
            DeleteAclsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setFilterResults(filterResults), version
        )
    }

    class Builder(
        private val data: DeleteAclsRequestData,
    ) : AbstractRequest.Builder<DeleteAclsRequest>(ApiKeys.DELETE_ACLS) {

        override fun build(version: Short): DeleteAclsRequest = DeleteAclsRequest(data, version)

        override fun toString(): String = data.toString()
    }

    companion object {

        fun parse(buffer: ByteBuffer, version: Short): DeleteAclsRequest =
            DeleteAclsRequest(
                DeleteAclsRequestData(ByteBufferAccessor(buffer), version),
                version,
            )

        fun deleteAclsFilter(filter: AclBindingFilter): DeleteAclsFilter =
            DeleteAclsFilter()
                .setResourceNameFilter(filter.patternFilter.name)
                .setResourceTypeFilter(filter.patternFilter.resourceType.code)
                .setPatternTypeFilter(filter.patternFilter.patternType.code)
                .setHostFilter(filter.entryFilter.host)
                .setOperation(filter.entryFilter.operation.code)
                .setPermissionType(filter.entryFilter.permissionType.code)
                .setPrincipalFilter(filter.entryFilter.principal)

        private fun aclBindingFilter(filter: DeleteAclsFilter): AclBindingFilter {
            val patternFilter = ResourcePatternFilter(
                resourceType = ResourceType.fromCode(filter.resourceTypeFilter),
                name = filter.resourceNameFilter,
                patternType = PatternType.fromCode(filter.patternTypeFilter),
            )
            val entryFilter = AccessControlEntryFilter(
                principal = filter.principalFilter,
                host = filter.hostFilter,
                operation = AclOperation.fromCode(filter.operation),
                permissionType = AclPermissionType.fromCode(filter.permissionType),
            )

            return AclBindingFilter(patternFilter, entryFilter)
        }
    }
}
