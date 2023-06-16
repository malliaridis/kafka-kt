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

package org.apache.kafka.common.protocol

import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function
import java.util.stream.Collectors
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.types.Schema
import org.apache.kafka.common.protocol.types.Type
import org.apache.kafka.common.record.RecordBatch

/**
 * Identifiers for all the Kafka APIs
 *
 * @property clusterAction indicates if this is a ClusterAction request used only by brokers
 * @property minRequiredInterBrokerMagic Indicates the minimum required inter broker magic required
 * to support the API
 * @property forwardable Indicates whether the API is enabled for forwarding
 */
enum class ApiKeys(
    val messageType: ApiMessageType,
    val clusterAction: Boolean = false,
    val minRequiredInterBrokerMagic: Byte = RecordBatch.MAGIC_VALUE_V0,
    val forwardable: Boolean = false
) {

    PRODUCE(messageType = ApiMessageType.PRODUCE),

    FETCH(messageType = ApiMessageType.FETCH),

    LIST_OFFSETS(messageType = ApiMessageType.LIST_OFFSETS),

    METADATA(messageType = ApiMessageType.METADATA),

    LEADER_AND_ISR(
        messageType = ApiMessageType.LEADER_AND_ISR,
        clusterAction = true
    ),

    STOP_REPLICA(
        messageType = ApiMessageType.STOP_REPLICA,
        clusterAction = true
    ),

    UPDATE_METADATA(
        messageType = ApiMessageType.UPDATE_METADATA,
        clusterAction = true
    ),

    CONTROLLED_SHUTDOWN(messageType = ApiMessageType.CONTROLLED_SHUTDOWN, true),

    OFFSET_COMMIT(messageType = ApiMessageType.OFFSET_COMMIT),

    OFFSET_FETCH(messageType = ApiMessageType.OFFSET_FETCH),

    FIND_COORDINATOR(messageType = ApiMessageType.FIND_COORDINATOR),

    JOIN_GROUP(messageType = ApiMessageType.JOIN_GROUP),

    HEARTBEAT(messageType = ApiMessageType.HEARTBEAT),

    LEAVE_GROUP(messageType = ApiMessageType.LEAVE_GROUP),

    SYNC_GROUP(messageType = ApiMessageType.SYNC_GROUP),

    DESCRIBE_GROUPS(messageType = ApiMessageType.DESCRIBE_GROUPS),

    LIST_GROUPS(messageType = ApiMessageType.LIST_GROUPS),

    SASL_HANDSHAKE(messageType = ApiMessageType.SASL_HANDSHAKE),

    API_VERSIONS(messageType = ApiMessageType.API_VERSIONS),

    CREATE_TOPICS(
        messageType = ApiMessageType.CREATE_TOPICS,
        clusterAction = false,
        forwardable = true
    ),

    DELETE_TOPICS(
        messageType = ApiMessageType.DELETE_TOPICS,
        clusterAction = false,
        forwardable = true
    ),

    DELETE_RECORDS(messageType = ApiMessageType.DELETE_RECORDS),

    INIT_PRODUCER_ID(messageType = ApiMessageType.INIT_PRODUCER_ID),

    OFFSET_FOR_LEADER_EPOCH(messageType = ApiMessageType.OFFSET_FOR_LEADER_EPOCH),

    ADD_PARTITIONS_TO_TXN(
        messageType = ApiMessageType.ADD_PARTITIONS_TO_TXN,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V2,
        forwardable = false
    ),

    ADD_OFFSETS_TO_TXN(
        messageType = ApiMessageType.ADD_OFFSETS_TO_TXN,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V2,
        forwardable = false
    ),

    END_TXN(
        messageType = ApiMessageType.END_TXN,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V2,
        forwardable = false
    ),

    WRITE_TXN_MARKERS(
        messageType = ApiMessageType.WRITE_TXN_MARKERS,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V2,
        forwardable = false
    ),

    TXN_OFFSET_COMMIT(
        messageType = ApiMessageType.TXN_OFFSET_COMMIT,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V2,
        forwardable = false
    ),

    DESCRIBE_ACLS(messageType = ApiMessageType.DESCRIBE_ACLS),

    CREATE_ACLS(
        messageType = ApiMessageType.CREATE_ACLS,
        clusterAction = false,
        forwardable = true
    ),

    DELETE_ACLS(
        messageType = ApiMessageType.DELETE_ACLS,
        clusterAction = false,
        forwardable = true
    ),

    DESCRIBE_CONFIGS(messageType = ApiMessageType.DESCRIBE_CONFIGS),

    ALTER_CONFIGS(
        messageType = ApiMessageType.ALTER_CONFIGS,
        clusterAction = false,
        forwardable = true
    ),

    ALTER_REPLICA_LOG_DIRS(messageType = ApiMessageType.ALTER_REPLICA_LOG_DIRS),

    DESCRIBE_LOG_DIRS(messageType = ApiMessageType.DESCRIBE_LOG_DIRS),

    SASL_AUTHENTICATE(messageType = ApiMessageType.SASL_AUTHENTICATE),

    CREATE_PARTITIONS(
        messageType = ApiMessageType.CREATE_PARTITIONS,
        clusterAction = false,
        forwardable = true
    ),

    CREATE_DELEGATION_TOKEN(
        messageType = ApiMessageType.CREATE_DELEGATION_TOKEN,
        clusterAction = false,
        forwardable = true
    ),

    RENEW_DELEGATION_TOKEN(
        messageType = ApiMessageType.RENEW_DELEGATION_TOKEN,
        clusterAction = false,
        forwardable = true
    ),

    EXPIRE_DELEGATION_TOKEN(
        messageType = ApiMessageType.EXPIRE_DELEGATION_TOKEN,
        clusterAction = false,
        forwardable = true
    ),

    DESCRIBE_DELEGATION_TOKEN(messageType = ApiMessageType.DESCRIBE_DELEGATION_TOKEN),

    DELETE_GROUPS(messageType = ApiMessageType.DELETE_GROUPS),

    ELECT_LEADERS(
        messageType = ApiMessageType.ELECT_LEADERS,
        clusterAction = false,
        forwardable = true
    ),

    INCREMENTAL_ALTER_CONFIGS(
        messageType = ApiMessageType.INCREMENTAL_ALTER_CONFIGS,
        clusterAction = false,
        forwardable = true
    ),

    ALTER_PARTITION_REASSIGNMENTS(
        messageType = ApiMessageType.ALTER_PARTITION_REASSIGNMENTS,
        clusterAction = false,
        forwardable = true
    ),

    LIST_PARTITION_REASSIGNMENTS(
        messageType = ApiMessageType.LIST_PARTITION_REASSIGNMENTS,
        clusterAction = false,
        forwardable = true
    ),

    OFFSET_DELETE(messageType = ApiMessageType.OFFSET_DELETE),

    DESCRIBE_CLIENT_QUOTAS(messageType = ApiMessageType.DESCRIBE_CLIENT_QUOTAS),

    ALTER_CLIENT_QUOTAS(
        messageType = ApiMessageType.ALTER_CLIENT_QUOTAS,
        clusterAction = false,
        forwardable = true
    ),

    DESCRIBE_USER_SCRAM_CREDENTIALS(messageType = ApiMessageType.DESCRIBE_USER_SCRAM_CREDENTIALS),

    ALTER_USER_SCRAM_CREDENTIALS(
        messageType = ApiMessageType.ALTER_USER_SCRAM_CREDENTIALS,
        clusterAction = false,
        forwardable = true
    ),

    VOTE(
        messageType = ApiMessageType.VOTE,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    BEGIN_QUORUM_EPOCH(
        messageType = ApiMessageType.BEGIN_QUORUM_EPOCH,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    END_QUORUM_EPOCH(
        messageType = ApiMessageType.END_QUORUM_EPOCH,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    DESCRIBE_QUORUM(
        messageType = ApiMessageType.DESCRIBE_QUORUM,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = true
    ),

    ALTER_PARTITION(messageType = ApiMessageType.ALTER_PARTITION, clusterAction = true),

    UPDATE_FEATURES(
        messageType = ApiMessageType.UPDATE_FEATURES,
        clusterAction = true,
        forwardable = true
    ),

    ENVELOPE(
        messageType = ApiMessageType.ENVELOPE,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    FETCH_SNAPSHOT(
        messageType = ApiMessageType.FETCH_SNAPSHOT,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    DESCRIBE_CLUSTER(messageType = ApiMessageType.DESCRIBE_CLUSTER),

    DESCRIBE_PRODUCERS(messageType = ApiMessageType.DESCRIBE_PRODUCERS),

    BROKER_REGISTRATION(
        ApiMessageType.BROKER_REGISTRATION,
        true,
        RecordBatch.MAGIC_VALUE_V0,
        false
    ),

    BROKER_HEARTBEAT(
        messageType = ApiMessageType.BROKER_HEARTBEAT,
        clusterAction = true,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = false
    ),

    UNREGISTER_BROKER(
        messageType = ApiMessageType.UNREGISTER_BROKER,
        clusterAction = false,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = true
    ),

    DESCRIBE_TRANSACTIONS(messageType = ApiMessageType.DESCRIBE_TRANSACTIONS),

    LIST_TRANSACTIONS(messageType = ApiMessageType.LIST_TRANSACTIONS),

    ALLOCATE_PRODUCER_IDS(
        messageType = ApiMessageType.ALLOCATE_PRODUCER_IDS,
        clusterAction = true,
        forwardable = true
    );

    /**
     * The permanent and immutable id of an API - this can't change ever
     */
    val id: Short = messageType.apiKey()

    val requiresDelayedAllocation: Boolean by lazy {
        forwardable || shouldRetainsBufferReference(messageType.requestSchemas())
    }

    constructor(
        messageType: ApiMessageType,
        clusterAction: Boolean,
        forwardable: Boolean,
    ) : this(
        messageType = messageType,
        clusterAction = clusterAction,
        minRequiredInterBrokerMagic = RecordBatch.MAGIC_VALUE_V0,
        forwardable = forwardable
    )

    fun latestVersion(): Short = messageType.highestSupportedVersion()

    fun oldestVersion(): Short = messageType.lowestSupportedVersion()

    fun allVersions(): List<Short> {
        val versions: MutableList<Short> = ArrayList(latestVersion() - oldestVersion() + 1)
        for (version in oldestVersion()..latestVersion()) {
            versions.add(version.toShort())
        }
        return versions
    }

    fun isVersionSupported(apiVersion: Short): Boolean =
        apiVersion >= oldestVersion() && apiVersion <= latestVersion()

    fun requestHeaderVersion(apiVersion: Short): Short =
        messageType.requestHeaderVersion(apiVersion)


    fun responseHeaderVersion(apiVersion: Short): Short =
        messageType.responseHeaderVersion(apiVersion)

    fun inScope(listener: ListenerType?): Boolean =
        messageType.listeners().contains(listener)

    companion object {

        private val APIS_BY_LISTENER: Map<ListenerType, EnumSet<ApiKeys>> =
            ListenerType.values().associateWith(::filterApisForListener)

        // The generator ensures every `ApiMessageType` has a unique id
        private val ID_TO_TYPE = values().associateBy(ApiKeys::id)

        private fun shouldRetainsBufferReference(requestSchemas: Array<Schema>): Boolean =
            requestSchemas.any(::retainsBufferReference)

        fun forId(id: Int): ApiKeys = ID_TO_TYPE[id.toShort()]
            ?: throw IllegalArgumentException("Unexpected api key: $id")

        fun hasId(id: Int): Boolean = ID_TO_TYPE.containsKey(id.toShort())

        private fun toHtml(): String {
            val b = StringBuilder()
            b.append("<table class=\"data-table\"><tbody>\n")
            b.append("<tr>")
            b.append("<th>Name</th>\n")
            b.append("<th>Key</th>\n")
            b.append("</tr>")
            for (key in clientApis()) {
                b.append("<tr>\n")
                b.append("<td>")
                b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>")
                b.append("</td>")
                b.append("<td>")
                b.append(key.id.toInt())
                b.append("</td>")
                b.append("</tr>\n")
            }
            b.append("</tbody></table>\n")
            return b.toString()
        }

        @JvmStatic
        fun main(args: Array<String>) {
            println(toHtml())
        }

        private fun retainsBufferReference(schema: Schema): Boolean {
            val hasBuffer = AtomicBoolean(false)

            val detector: Schema.Visitor = object : Schema.Visitor() {

                override fun visit(field: Type) {
                    if (
                        field === Type.BYTES
                        || field === Type.NULLABLE_BYTES
                        || field === Type.RECORDS
                        || field === Type.COMPACT_BYTES
                        || field === Type.COMPACT_NULLABLE_BYTES
                    ) hasBuffer.set(true)
                }
            }

            schema.walk(detector)
            return hasBuffer.get()
        }

        fun zkBrokerApis(): EnumSet<ApiKeys> = apisForListener(ListenerType.ZK_BROKER)

        fun controllerApis(): EnumSet<ApiKeys> = apisForListener(ListenerType.CONTROLLER)

        fun clientApis(): EnumSet<ApiKeys> {
            return EnumSet.copyOf(
                values().filter { apiKey: ApiKeys ->
                    apiKey.inScope(ListenerType.ZK_BROKER) || apiKey.inScope(ListenerType.BROKER)
                }
            )
        }

        fun apisForListener(listener: ListenerType): EnumSet<ApiKeys> = APIS_BY_LISTENER[listener]!!

        private fun filterApisForListener(listener: ListenerType): EnumSet<ApiKeys> {
            return EnumSet.copyOf(values().filter { apiKey -> apiKey.inScope(listener) })
        }
    }
}
