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

package org.apache.kafka.server.common

import org.apache.kafka.common.record.RecordVersion

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate
 * between themselves. This is only for inter-broker communications - when communicating with clients,
 * the client decides on the API version.
 *
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another if it is lower in the enum list (to avoid depending on lexicographic order)
 *
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 0.10.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 0.10.0, we will add a config value "0.10.0-IV0" and a
 * corresponding enum constant IBP_0_10_0-IV0. We will also add a config value "0.10.0" that will be mapped to the
 * latest internal version object, which is IBP_0_10_0-IV0. When we change the protocol a second time while developing
 * 0.10.0, we will add a new config value "0.10.0-IV1" and a corresponding enum constant IBP_0_10_0-IV1. We will change
 * the config value "0.10.0" to map to the latest internal version IBP_0_10_0-IV1. The config value of
 * "0.10.0-IV0" is still mapped to IBP_0_10_0-IV0. This way, if people are deploying from trunk, they can use
 * "0.10.0-IV0" and "0.10.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "0.10.0" when upgrading to the 0.10.0 release.
 */
enum class MetadataVersion(
    val featureLevel: Short,
    private val release: String,
    subVersion: String,
    val didMetadataChange: Boolean = true,
) {
    IBP_0_8_0(featureLevel = -1, release = "0.8.0", subVersion = ""),
    IBP_0_8_1(featureLevel = -1, release = "0.8.1", subVersion = ""),
    IBP_0_8_2(featureLevel = -1, release = "0.8.2", subVersion = ""),
    IBP_0_9_0(featureLevel = -1, release = "0.9.0", subVersion = ""),

    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    IBP_0_10_0_IV0(featureLevel = -1, release = "0.10.0", subVersion = "IV0"),

    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    IBP_0_10_0_IV1(featureLevel = -1, release = "0.10.0", subVersion = "IV1"),

    // introduced for JoinGroup protocol change in KIP-62
    IBP_0_10_1_IV0(featureLevel = -1, release = "0.10.1", subVersion = "IV0"),

    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    IBP_0_10_1_IV1(featureLevel = -1, release = "0.10.1", subVersion = "IV1"),

    // introduced ListOffsetRequest v1 in KIP-79
    IBP_0_10_1_IV2(featureLevel = -1, release = "0.10.1", subVersion = "IV2"),

    // introduced UpdateMetadataRequest v3 in KIP-103
    IBP_0_10_2_IV0(featureLevel = -1, release = "0.10.2", subVersion = "IV0"),

    // KIP-98 (idempotent and transactional producer support)
    IBP_0_11_0_IV0(featureLevel = -1, release = "0.11.0", subVersion = "IV0"),

    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    IBP_0_11_0_IV1(featureLevel = -1, release = "0.11.0", subVersion = "IV1"),

    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    IBP_0_11_0_IV2(featureLevel = -1, release = "0.11.0", subVersion = "IV2"),

    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    IBP_1_0_IV0(featureLevel = -1, release = "1.0", subVersion = "IV0"),

    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    IBP_1_1_IV0(featureLevel = -1, release = "1.1", subVersion = "IV0"),

    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader and follower
    // after fast leader fail over)
    IBP_2_0_IV0(featureLevel = -1, release = "2.0", subVersion = "IV0"),

    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    IBP_2_0_IV1(-1, "2.0", "IV1"),

    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    IBP_2_1_IV0(featureLevel = -1, release = "2.1", subVersion = "IV0"),

    // New Fetch, OffsetsForLeaderEpoch, and ListOffsets schemas (KIP-320)
    IBP_2_1_IV1(featureLevel = -1, release = "2.1", subVersion = "IV1"),

    // Support ZStandard Compression Codec (KIP-110)
    IBP_2_1_IV2(featureLevel = -1, release = "2.1", subVersion = "IV2"),

    // Introduced broker generation (KIP-380), and
    // LeaderAndIsrRequest V2, UpdateMetadataRequest V5, StopReplicaRequest V1
    IBP_2_2_IV0(featureLevel = -1, release = "2.2", subVersion = "IV0"),

    // New error code for ListOffsets when a new leader is lagging behind former HW (KIP-207)
    IBP_2_2_IV1(featureLevel = -1, release = "2.2", subVersion = "IV1"),

    // Introduced static membership.
    IBP_2_3_IV0(featureLevel = -1, release = "2.3", subVersion = "IV0"),

    // Add rack_id to FetchRequest, preferred_read_replica to FetchResponse, and replica_id to OffsetsForLeaderRequest
    IBP_2_3_IV1(featureLevel = -1, release = "2.3", subVersion = "IV1"),

    // Add adding_replicas and removing_replicas fields to LeaderAndIsrRequest
    IBP_2_4_IV0(featureLevel = -1, release = "2.4", subVersion = "IV0"),

    // Flexible version support in inter-broker APIs
    IBP_2_4_IV1(featureLevel = -1, release = "2.4", subVersion = "IV1"),

    // No new APIs, equivalent to 2.4-IV1
    IBP_2_5_IV0(featureLevel = -1, release = "2.5", subVersion = "IV0"),

    // Introduced StopReplicaRequest V3 containing the leader epoch for each partition (KIP-570)
    IBP_2_6_IV0(featureLevel = -1, release = "2.6", subVersion = "IV0"),

    // Introduced feature versioning support (KIP-584)
    IBP_2_7_IV0(featureLevel = -1, release = "2.7", subVersion = "IV0"),

    // Bup Fetch protocol for Raft protocol (KIP-595)
    IBP_2_7_IV1(featureLevel = -1, release = "2.7", subVersion = "IV1"),

    // Introduced AlterPartition (KIP-497)
    IBP_2_7_IV2(featureLevel = -1, release = "2.7", subVersion = "IV2"),

    // Flexible versioning on ListOffsets, WriteTxnMarkers and OffsetsForLeaderEpoch. Also adds topic IDs (KIP-516)
    IBP_2_8_IV0(featureLevel = -1, release = "2.8", subVersion = "IV0"),

    // Introduced topic IDs to LeaderAndIsr and UpdateMetadata requests/responses (KIP-516)
    IBP_2_8_IV1(featureLevel = -1, release = "2.8", subVersion = "IV1"),

    // Introduce AllocateProducerIds (KIP-730)
    IBP_3_0_IV0(featureLevel = -1, release = "3.0", subVersion = "IV0"),

    // Introduce ListOffsets V7 which supports listing offsets by max timestamp (KIP-734)
    // Assume message format version is 3.0 (KIP-724)
    IBP_3_0_IV1(featureLevel = 1, release = "3.0", subVersion = "IV1", didMetadataChange = true),

    // Adds topic IDs to Fetch requests/responses (KIP-516)
    IBP_3_1_IV0(featureLevel = 2, release = "3.1", subVersion = "IV0", didMetadataChange = false),

    // Support for leader recovery for unclean leader election (KIP-704)
    IBP_3_2_IV0(featureLevel = 3, release = "3.2", subVersion = "IV0", didMetadataChange = true),

    // Support for metadata.version feature flag and Removes min_version_level from the finalized version range
    // that is written to ZooKeeper (KIP-778)
    IBP_3_3_IV0(featureLevel = 4, release = "3.3", subVersion = "IV0", didMetadataChange = false),

    // Support NoopRecord for the cluster metadata log (KIP-835)
    IBP_3_3_IV1(featureLevel = 5, release = "3.3", subVersion = "IV1", didMetadataChange = true),

    // In KRaft mode, use BrokerRegistrationChangeRecord instead of UnfenceBrokerRecord and FenceBrokerRecord.
    IBP_3_3_IV2(featureLevel = 6, release = "3.3", subVersion = "IV2", didMetadataChange = true),

    // Adds InControlledShutdown state to RegisterBrokerRecord and BrokerRegistrationChangeRecord (KIP-841).
    IBP_3_3_IV3(featureLevel = 7, release = "3.3", subVersion = "IV3", didMetadataChange = true),

    // Adds ZK to KRaft migration support (KIP-866). This includes ZkMigrationRecord, a new version of
    // RegisterBrokerRecord, and updates to a handful of RPCs.
    IBP_3_4_IV0(featureLevel = 8, release = "3.4", subVersion = "IV0", didMetadataChange = true),

    // Support for tiered storage (KIP-405)
    IBP_3_5_IV0(featureLevel = 9, release = "3.5", subVersion = "IV0", didMetadataChange = false),

    // Adds replica epoch to Fetch request (KIP-903).
    IBP_3_5_IV1(featureLevel = 10, release = "3.5", subVersion = "IV1", didMetadataChange = false),

    // KRaft support for SCRAM
    IBP_3_5_IV2(featureLevel = 11, release = "3.5", subVersion = "IV2", didMetadataChange = true),

    // Remove leader epoch bump when KRaft controller shrinks the ISR (KAFKA-15021)
    IBP_3_6_IV0(featureLevel = 12, release = "3.6", subVersion = "IV0", didMetadataChange = false),

    // Add metadata transactions
    IBP_3_6_IV1(featureLevel = 13, release = "3.6", subVersion = "IV1", didMetadataChange = true),

    // Add KRaft support for Delegation Tokens
    IBP_3_6_IV2(featureLevel = 14, release = "3.6", subVersion = "IV2", didMetadataChange = true);

    private val ibpVersion: String = if (subVersion.isEmpty()) release else "$release-$subVersion"

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("featureLevel")
    )
    fun featureLevel(): Short = featureLevel

    val isSaslInterBrokerHandshakeRequestEnabled: Boolean
        get() = isAtLeast(IBP_0_10_0_IV1)

    val isOffsetForLeaderEpochSupported: Boolean
        get() = isAtLeast(IBP_0_11_0_IV2)

    val isFeatureVersioningSupported: Boolean
        get() = isAtLeast(IBP_2_7_IV0)

    val isTruncationOnFetchSupported: Boolean
        get() = isAtLeast(IBP_2_7_IV1)

    val isAlterPartitionSupported: Boolean
        get() = isAtLeast(IBP_2_7_IV2)

    val isTopicIdsSupported: Boolean
        get() = isAtLeast(IBP_2_8_IV0)

    val isAllocateProducerIdsSupported: Boolean
        get() = isAtLeast(IBP_3_0_IV0)

    val isLeaderRecoverySupported: Boolean
        get() = isAtLeast(IBP_3_2_IV0)

    val isNoOpRecordSupported: Boolean
        get() = isAtLeast(IBP_3_3_IV1)

    val isApiForwardingEnabled: Boolean
        get() = isAtLeast(IBP_3_4_IV0)

    val isScramSupported: Boolean
        get() = isAtLeast(IBP_3_5_IV2)

    val isLeaderEpochBumpRequiredOnIsrShrink: Boolean
        get() = !isAtLeast(IBP_3_6_IV0)

    val isMetadataTransactionSupported: Boolean
        get() = isAtLeast(IBP_3_6_IV1)

    val isDelegationTokenSupported: Boolean
        get() = isAtLeast(IBP_3_6_IV2)

    val isKRaftSupported: Boolean
        get() = featureLevel > 0

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("highestSupportedRecordVersion"),
    )
    fun highestSupportedRecordVersion(): RecordVersion {
        return if (isLessThan(IBP_0_10_0_IV0)) RecordVersion.V0
        else if (isLessThan(IBP_0_11_0_IV0)) RecordVersion.V1
        else RecordVersion.V2
    }

    val highestSupportedRecordVersion: RecordVersion
        get() {
            return if (isLessThan(IBP_0_10_0_IV0)) RecordVersion.V0
            else if (isLessThan(IBP_0_11_0_IV0)) RecordVersion.V1
            else RecordVersion.V2
        }

    val isBrokerRegistrationChangeRecordSupported: Boolean
        get() = isAtLeast(IBP_3_3_IV2)

    val isInControlledShutdownStateSupported: Boolean
        get() = isAtLeast(IBP_3_3_IV3)

    val isMigrationSupported: Boolean
        get() = isAtLeast(IBP_3_4_IV0)

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("registerBrokerRecordVersion"),
    )
    fun registerBrokerRecordVersion(): Short = registerBrokerRecordVersion

    val registerBrokerRecordVersion: Short
        get() = when {
        isMigrationSupported -> 2.toShort() // new isMigrationZkBroker field
        isInControlledShutdownStateSupported -> 1.toShort()
        else -> 0.toShort()
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("fetchRequestVersion"),
    )
    fun fetchRequestVersion(): Short = fetchRequestVersion

    val fetchRequestVersion: Short
        get() = when {
            isAtLeast(IBP_3_5_IV1) -> 15
            isAtLeast(IBP_3_5_IV0) -> 14
            isAtLeast(IBP_3_1_IV0) -> 13
            isAtLeast(IBP_2_7_IV1) -> 12
            isAtLeast(IBP_2_3_IV1) -> 11
            isAtLeast(IBP_2_1_IV2) -> 10
            isAtLeast(IBP_2_0_IV1) -> 8
            isAtLeast(IBP_1_1_IV0) -> 7
            isAtLeast(IBP_0_11_0_IV1) -> 5
            isAtLeast(IBP_0_11_0_IV0) -> 4
            isAtLeast(IBP_0_10_1_IV1) -> 3
            isAtLeast(IBP_0_10_0_IV0) -> 2
            isAtLeast(IBP_0_9_0) -> 1
            else -> 0
        }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("offsetForLeaderEpochRequestVersion"),
    )
    fun offsetForLeaderEpochRequestVersion(): Short = offsetForLeaderEpochRequestVersion

    val offsetForLeaderEpochRequestVersion: Short
        get() = when {
            isAtLeast(IBP_2_8_IV0) -> 4
            isAtLeast(IBP_2_3_IV1) -> 3
            isAtLeast(IBP_2_1_IV1) -> 2
            isAtLeast(IBP_2_0_IV0) -> 1
            else -> 0
        }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("listOffsetRequestVersion"),
    )
    fun listOffsetRequestVersion(): Short = listOffsetRequestVersion

    val listOffsetRequestVersion: Short
        get() = when {
            isAtLeast(IBP_3_5_IV0) -> 8
            isAtLeast(IBP_3_0_IV1) -> 7
            isAtLeast(IBP_2_8_IV0) -> 6
            isAtLeast(IBP_2_2_IV1) -> 5
            isAtLeast(IBP_2_1_IV1) -> 4
            isAtLeast(IBP_2_0_IV1) -> 3
            isAtLeast(IBP_0_11_0_IV0) -> 2
            isAtLeast(IBP_0_10_1_IV2) -> 1
            else -> 0
        }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("groupMetadataValueVersion"),
    )
    fun groupMetadataValueVersion(): Short = groupMetadataValueVersion

    val groupMetadataValueVersion: Short
        get() = when {
            isLessThan(IBP_0_10_1_IV0) -> 0
            isLessThan(IBP_2_1_IV0) -> 1
            isLessThan(IBP_2_3_IV0) -> 2
            else -> {
                // Serialize with the highest supported non-flexible version
                // until a tagged field is introduced or the version is bumped.
                3
            }
        }

    fun offsetCommitValueVersion(expireTimestampMs: Boolean): Short = when {
        isLessThan(IBP_2_1_IV0) || expireTimestampMs -> 1
        isLessThan(IBP_2_1_IV1) -> 2
        else -> {
            // Serialize with the highest supported non-flexible version
            // until a tagged field is introduced or the version is bumped.
            3
        }
    }

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("shortVersion"),
    )
    fun shortVersion(): String = release

    val shortVersion: String
        get() = release

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("version"),
    )
    fun version(): String = ibpVersion

    val version: String
        get() = ibpVersion

    @Deprecated(
        message = "Use property instead",
        replaceWith = ReplaceWith("didMetadataChange"),
    )
    fun didMetadataChange(): Boolean = didMetadataChange

    fun previous(): MetadataVersion? {
        val idx = ordinal
        return if (idx > 0) VERSIONS[idx - 1]
        else null
    }

    fun isAtLeast(otherVersion: MetadataVersion?): Boolean {
        return this >= otherVersion!!
    }

    fun isLessThan(otherVersion: MetadataVersion?): Boolean {
        return this < otherVersion!!
    }

    override fun toString(): String = ibpVersion

    companion object {

        // NOTES when adding a new version:
        //   Update the default version in @ClusterTest annotation to point to the latest version
        //   Change expected message in org.apache.kafka.tools.FeatureCommandTest in multiple places
        //   (search for "Change expected message")
        const val FEATURE_NAME = "metadata.version"

        /**
         * The first version we currently support in KRaft.
         */
        val MINIMUM_KRAFT_VERSION = IBP_3_0_IV1

        /**
         * The first version we currently support in the bootstrap metadata. We chose 3.3IV0 since it
         * is the first version that supports storing the `metadata.version` in the log.
         */
        val MINIMUM_BOOTSTRAP_VERSION = IBP_3_3_IV0

        val VERSIONS: Array<MetadataVersion>
            get() = entries.toTypedArray()

        private val IBP_VERSIONS: Map<String, MetadataVersion>

        init {
            IBP_VERSIONS = HashMap()
            val maxInterVersion = mutableMapOf<String, MetadataVersion>()
            for (metadataVersion in VERSIONS) {
                maxInterVersion[metadataVersion.release] = metadataVersion
                IBP_VERSIONS[metadataVersion.ibpVersion] = metadataVersion
            }
            IBP_VERSIONS.putAll(maxInterVersion)
        }

        /**
         * Return an `MetadataVersion` instance for `versionString`, which can be in a variety of formats
         * (e.g. "0.8.0", "0.8.0.x", "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString`
         * cannot be mapped to an `MetadataVersion`.
         *
         * Note that 'misconfigured' values such as "1.0.1" will be parsed to `IBP_1_0_IV0` as we ignore anything
         * after the first two digits for versions that don't start with "0."
         */
        fun fromVersionString(versionString: String): MetadataVersion {
            val versionSegments = versionString.split(".")
            val numSegments = if (versionString.startsWith("0.")) 3 else 2

            val key = if (numSegments >= versionSegments.size) versionString
            else versionSegments.subList(0, numSegments).joinToString(".")

            return requireNotNull(IBP_VERSIONS[key]) { "Version $versionString is not a valid version" }
        }

        fun fromFeatureLevel(version: Short): MetadataVersion {
            return requireNotNull(entries.firstOrNull { it.featureLevel == version }) {
                "No MetadataVersion with metadata version $version"
            }
        }

        /**
         * Return the minimum `MetadataVersion` that supports `RecordVersion`.
         */
        fun minSupportedFor(recordVersion: RecordVersion): MetadataVersion {
            return when (recordVersion) {
                RecordVersion.V0 -> IBP_0_8_0
                RecordVersion.V1 -> IBP_0_10_0_IV0
                RecordVersion.V2 -> IBP_0_11_0_IV0
                else -> throw IllegalArgumentException("Invalid message format version $recordVersion")
            }
        }

        fun latest(): MetadataVersion = VERSIONS[VERSIONS.size - 1]

        fun checkIfMetadataChanged(
            sourceVersion: MetadataVersion,
            targetVersion: MetadataVersion,
        ): Boolean {
            if (sourceVersion == targetVersion) return false

            val highVersion: MetadataVersion
            val lowVersion: MetadataVersion
            if (sourceVersion < targetVersion) {
                highVersion = targetVersion
                lowVersion = sourceVersion
            } else {
                highVersion = sourceVersion
                lowVersion = targetVersion
            }
            return checkIfMetadataChangedOrdered(highVersion, lowVersion)
        }

        private fun checkIfMetadataChangedOrdered(
            highVersion: MetadataVersion,
            lowVersion: MetadataVersion,
        ): Boolean {
            var version = highVersion
            while (!version.didMetadataChange && version != lowVersion) {
                version = version.previous() ?: break
            }
            return version != lowVersion
        }
    }
}
