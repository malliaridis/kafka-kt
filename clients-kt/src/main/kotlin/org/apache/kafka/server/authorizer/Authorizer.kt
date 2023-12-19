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

import java.io.Closeable
import java.util.EnumMap
import java.util.concurrent.CompletionStage
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.SecurityUtils

/**
 * Pluggable authorizer interface for Kafka brokers.
 *
 * Startup sequence in brokers:
 *
 *  1. Broker creates authorizer instance if configured in `authorizer.class.name`.
 *  2. Broker configures and starts authorizer instance. Authorizer implementation starts loading
 *     its metadata.
 *  3. Broker starts SocketServer to accept connections and process requests.
 *  4. For each listener, SocketServer waits for authorization metadata to be available in the
 *     authorizer before accepting connections. The future returned by [start] for each listener
 *     must return only when authorizer is ready to authorize requests on the listener.
 *  5. Broker accepts connections. For each connection, broker performs authentication and then
 *     accepts Kafka requests. For each request, broker invokes [authorize] to authorize actions
 *     performed by the request.
 *
 * Authorizer implementation class may optionally implement @[org.apache.kafka.common.Reconfigurable]
 * to enable dynamic reconfiguration without restarting the broker.
 *
 * **Threading model:**
 *
 * - All authorizer operations including authorization and ACL updates must be thread-safe.
 * - ACL update methods are asynchronous. Implementations with low update latency may return a
 *   completed future using [java.util.concurrent.CompletableFuture.completedFuture].
 *   This ensures that the request will be handled synchronously by the caller without using a
 *   purgatory to wait for the result. If ACL updates require remote communication which may block,
 *   return a future that is completed asynchronously when the remote operation completes. This
 *   enables the caller to process other requests on the request threads without blocking.
 * - Any threads or thread pools used for processing remote operations asynchronously can be started
 *   during [start]. These threads must be shutdown during [Authorizer.close].
 */
@Evolving
interface Authorizer : Configurable, Closeable {

    /**
     * Starts loading authorization metadata and returns futures that can be used to wait until
     * metadata for authorizing requests on each listener is available. Each listener will be
     * started only after its metadata is available and authorizer is ready to start authorizing
     * requests on that listener.
     *
     * @param serverInfo Metadata for the broker including broker id and listener endpoints
     * @return CompletionStage for each endpoint that completes when authorizer is ready to
     * start authorizing requests on that listener.
     */
    fun start(serverInfo: AuthorizerServerInfo): Map<Endpoint, CompletionStage<Unit>>

    /**
     * Authorizes the specified action. Additional metadata for the action is specified
     * in `requestContext`.
     *
     *
     * This is a synchronous API designed for use with locally cached ACLs. Since this method is
     * invoked on the request thread while processing each request, implementations of this method
     * should avoid time-consuming remote communication that may block request threads.
     *
     * @param requestContext Request context including request type, security protocol and listener
     * name
     * @param actions Actions being authorized including resource and operation for each action
     * @return List of authorization results for each action in the same order as the provided
     * actions
     */
    fun authorize(
        requestContext: AuthorizableRequestContext,
        actions: List<Action>
    ): List<AuthorizationResult>

    /**
     * Creates new ACL bindings.
     *
     *
     * This is an asynchronous API that enables the caller to avoid blocking during the update.
     * Implementations of this API can return completed futures using
     * [java.util.concurrent.CompletableFuture.completedFuture] to process the update synchronously
     * on the request thread.
     *
     * @param requestContext Request context if the ACL is being created by a broker to handle
     * a client request to create ACLs. This may be null if ACLs are created directly in ZooKeeper
     * using AclCommand.
     * @param aclBindings ACL bindings to create
     *
     * @return Create result for each ACL binding in the same order as in the input list. Each
     * result is returned as a CompletionStage that completes when the result is available.
     */
    fun createAcls(
        requestContext: AuthorizableRequestContext,
        aclBindings: List<AclBinding>
    ): List<CompletionStage<AclCreateResult>>

    /**
     * Deletes all ACL bindings that match the provided filters.
     *
     *
     * This is an asynchronous API that enables the caller to avoid blocking during the update.
     * Implementations of this API can return completed futures using
     * [java.util.concurrent.CompletableFuture.completedFuture] to process the update synchronously
     * on the request thread.
     *
     *
     * Refer to the authorizer implementation docs for details on concurrent update guarantees.
     *
     * @param requestContext Request context if the ACL is being deleted by a broker to handle
     * a client request to delete ACLs. This may be null if ACLs are deleted directly in ZooKeeper
     * using AclCommand.
     * @param aclBindingFilters Filters to match ACL bindings that are to be deleted
     *
     * @return Delete result for each filter in the same order as in the input list.
     * Each result indicates which ACL bindings were actually deleted as well as any
     * bindings that matched but could not be deleted. Each result is returned as a
     * CompletionStage that completes when the result is available.
     */
    fun deleteAcls(
        requestContext: AuthorizableRequestContext,
        aclBindingFilters: List<AclBindingFilter>
    ): List<CompletionStage<AclDeleteResult>>

    /**
     * Returns ACL bindings which match the provided filter.
     *
     *
     * This is a synchronous API designed for use with locally cached ACLs. This method is invoked
     * on the request thread while processing DescribeAcls requests and should avoid time-consuming
     * remote communication that may block request threads.
     *
     * @return Iterator for ACL bindings, which may be populated lazily.
     */
    fun acls(filter: AclBindingFilter?): Iterable<AclBinding>

    /**
     * Get the current number of ACLs, for the purpose of metrics. Authorizers that don't implement
     * this function will simply return `-1`.
     */
    fun aclCount(): Int = -1

    /**
     * Check if the caller is authorized to perform the given ACL operation on at least one resource
     * of the given type.
     *
     * Custom authorizer implementations should consider overriding this default implementation
     * because:
     * 1. The default implementation iterates all AclBindings multiple times, without any caching
     *    by principal, host, operation, permission types, and resource types. More efficient
     *    implementations may be added in custom authorizers that directly access cached entries.
     * 2. The default implementation cannot integrate with any audit logging included in the
     *    authorizer implementation.
     * 3. The default implementation does not support any custom authorizer configs or other access
     *    rules apart from ACLs.
     *
     * @param requestContext Request context including request resourceType, security protocol and
     * listener name
     * @param op The ACL operation to check
     * @param resourceType The resource type to check
     * @return Return [AuthorizationResult.ALLOWED] if the caller is authorized to perform the given
     * ACL operation on at least one resource of the given type. Return [AuthorizationResult.DENIED]
     * otherwise.
     */
    fun authorizeByResourceType(
        requestContext: AuthorizableRequestContext,
        op: AclOperation,
        resourceType: ResourceType
    ): AuthorizationResult {
        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType)

        // Check a hard-coded name to ensure that super users are granted
        // access regardless of DENY ACLs.
        if (authorize(
                requestContext, listOf(
                    Action(
                        operation = op,
                        resourcePattern = ResourcePattern(
                            resourceType = resourceType,
                            name = "hardcode",
                            patternType = PatternType.LITERAL,
                        ),
                        resourceReferenceCount = 0,
                        logIfAllowed = true,
                        logIfDenied = false
                    )
                )
            )[0] == AuthorizationResult.ALLOWED
        ) return AuthorizationResult.ALLOWED

        // Filter out all the resource pattern corresponding to the RequestContext,
        // AclOperation, and ResourceType
        val resourceTypeFilter = ResourcePatternFilter(resourceType, null, PatternType.ANY)
        val aclFilter = AclBindingFilter(resourceTypeFilter, AccessControlEntryFilter.ANY)

        val denyPatterns: Map<PatternType, MutableSet<String>> = mapOf(
            PatternType.LITERAL to mutableSetOf(),
            PatternType.PREFIXED to mutableSetOf(),
        )

        val allowPatterns: Map<PatternType, MutableSet<String>> = mapOf(
            PatternType.LITERAL to mutableSetOf(),
            PatternType.PREFIXED to mutableSetOf(),
        )

        var hasWildCardAllow = false
        val principal = KafkaPrincipal(
            requestContext.principal().principalType,
            requestContext.principal().name
        )
        val hostAddr = requestContext.clientAddress().hostAddress

        acls(aclFilter).forEach { binding ->

            if (binding.entry.host != hostAddr && binding.entry.host != "*") return@forEach

            if (SecurityUtils.parseKafkaPrincipal(binding.entry.principal) != principal
                && binding.entry.principal != "User:*"
            ) return@forEach

            if (binding.entry.operation != op
                && binding.entry.operation != AclOperation.ALL
            ) return@forEach

            if (binding.entry.permissionType == AclPermissionType.DENY) {
                when (binding.pattern.patternType) {
                    PatternType.LITERAL -> {
                        // If wildcard deny exists, return deny directly
                        if (binding.pattern.name == ResourcePattern.WILDCARD_RESOURCE)
                            return AuthorizationResult.DENIED

                        denyPatterns[PatternType.LITERAL]!!.add(binding.pattern.name)
                    }

                    PatternType.PREFIXED ->
                        denyPatterns[PatternType.PREFIXED]!!.add(binding.pattern.name)

                    else -> {}
                }
                return@forEach
            }

            if (binding.entry.permissionType != AclPermissionType.ALLOW) return@forEach

            when (binding.pattern.patternType) {
                PatternType.LITERAL -> {
                    if (binding.pattern.name == ResourcePattern.WILDCARD_RESOURCE) {
                        hasWildCardAllow = true
                        return@forEach
                    }
                    allowPatterns[PatternType.LITERAL]!!.add(binding.pattern.name)
                }

                PatternType.PREFIXED ->
                    allowPatterns[PatternType.PREFIXED]!!.add(binding.pattern.name)

                else -> {}
            }
        }

        if (hasWildCardAllow) return AuthorizationResult.ALLOWED

        // For any literal allowed, if there's no dominant literal and prefix denied, return allow.
        // For any prefix allowed, if there's no dominant prefix denied, return allow.
        allowPatterns.forEach { (key, value) ->
            value.forEach innerLoop@ { allowStr ->

                if (key == PatternType.LITERAL
                    && denyPatterns[PatternType.LITERAL]!!.contains(allowStr)
                ) return@innerLoop

                val sb = StringBuilder()
                var hasDominatedDeny = false

                for (ch in allowStr.toCharArray()) {
                    sb.append(ch)
                    if (denyPatterns[PatternType.PREFIXED]!!.contains(sb.toString())) {
                        hasDominatedDeny = true
                        break
                    }
                }

                if (!hasDominatedDeny) return AuthorizationResult.ALLOWED
            }
        }
        return AuthorizationResult.DENIED
    }
}
