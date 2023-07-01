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

package org.apache.kafka.clients.admin

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.errors.ApiException

/**
 * The result of the [Admin.deleteAcls] call.
 *
 * The API of this class is evolving, see [Admin] for details.
 */
@Evolving
class DeleteAclsResult internal constructor(
    private val futures: Map<AclBindingFilter, KafkaFuture<FilterResults>>,
) {

    /**
     * A class containing either the deleted ACL binding or an exception if the delete failed.
     * @property binding The deleted ACL binding or `null` if there was an error.
     * @property exception An exception if the ACL delete was not successful or `null` if it was.
     */
    data class FilterResult internal constructor(
        val binding: AclBinding?,
        val exception: ApiException?,
    ) {

        /**
         * Return the deleted ACL binding or `null` if there was an error.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("binding"),
        )
        fun binding(): AclBinding? = binding

        /**
         * Return an exception if the ACL delete was not successful or `null` if it was.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("exception"),
        )
        fun exception(): ApiException? = exception
    }

    /**
     * A class containing the results of the delete ACLs operation.
     *
     * @property values A list of delete ACLs results for a given filter.
     */
    data class FilterResults internal constructor(val values: List<FilterResult>) {

        /**
         * Return a list of delete ACLs results for a given filter.
         */
        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("values"),
        )
        fun values(): List<FilterResult> = values
    }

    /**
     * Return a map from acl filters to futures which can be used to check the status of the
     * deletions by each filter.
     */
    fun values(): Map<AclBindingFilter, KafkaFuture<FilterResults>> = futures

    /**
     * Return a future which succeeds only if all the ACLs deletions succeed, and which contains all
     * the deleted ACLs. Note that it if the filters don't match any ACLs, this is not considered an
     * error.
     */
    fun all(): KafkaFuture<Collection<AclBinding>> =
        KafkaFuture.allOf(futures.values).thenApply { getAclBindings(futures) }

    private fun getAclBindings(
        futures: Map<AclBindingFilter, KafkaFuture<FilterResults>>,
    ): List<AclBinding> {
        val acls: MutableList<AclBinding> = ArrayList()
        for (value in futures.values) {
            val results: FilterResults = try {
                value.get()
            } catch (e: Throwable) {
                // This should be unreachable, since the future returned by KafkaFuture#allOf should
                // have failed if any Future failed.
                throw KafkaException("DeleteAclsResult#all: internal error", e)
            }
            for (result in results.values) {
                if (result.exception != null) throw result.exception
                acls.add(result.binding!!)
            }
        }
        return acls
    }
}
