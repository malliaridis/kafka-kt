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

package org.apache.kafka.server.authorizer

import java.util.*
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.errors.ApiException


@Evolving
data class AclDeleteResult(
    val deleteResults: Collection<AclBindingDeleteResult> = emptySet(),
    val exception: ApiException? = null
) {
    
    /**
     * Returns any exception while attempting to match ACL filter to delete ACLs.
     * If exception is empty, filtering has succeeded. See [aclBindingDeleteResults]
     * for deletion results for each filter.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("exception")
    )
    fun exception(): ApiException? = exception

    /**
     * Returns delete result for each matching ACL binding.
     */
    @Deprecated(
        message = "Use property instead.",
        replaceWith = ReplaceWith("deleteResults")
    )
    fun aclBindingDeleteResults(): Collection<AclBindingDeleteResult> = deleteResults

    /**
     * Delete result for each ACL binding that matched a delete filter.
     */
    data class AclBindingDeleteResult(
        val aclBinding: AclBinding,
        val exception: ApiException? = null
    ) {
        
        /**
         * Returns ACL binding that matched the delete filter. If [exception] is
         * empty, the ACL binding was successfully deleted.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("aclBinding")
        )
        fun aclBinding(): AclBinding = aclBinding

        /**
         * Returns any exception that resulted in failure to delete ACL binding.
         * If exception is empty, the ACL binding was successfully deleted.
         */
        @Deprecated(
            message = "Use property instead.",
            replaceWith = ReplaceWith("exception")
        )
        fun exception(): ApiException? = exception
    }
}