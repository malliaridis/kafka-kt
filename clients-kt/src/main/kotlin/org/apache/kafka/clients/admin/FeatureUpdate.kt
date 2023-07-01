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

/**
 * Encapsulates details about an update to a finalized feature.
 *
 * @property maxVersionLevel The new maximum version level for the finalized feature. A value of
 * zero is special and indicates that the update is intended to delete the finalized feature,
 * and should be accompanied by setting the upgradeType to safe or unsafe.
 * @property upgradeType Indicate what kind of upgrade should be performed in this operation.
 */
data class FeatureUpdate(
    val maxVersionLevel: Short,
    val upgradeType: UpgradeType,
) {

    /**
     * @param maxVersionLevel the new maximum version level for the finalized feature. A value of
     * zero is special and indicates that the update is intended to delete the finalized feature,
     * and should be accompanied by setting the allowDowngrade flag to true.
     * @param allowDowngrade `true`, if this feature update was meant to downgrade the existing
     * maximum version level of the finalized feature. Only "safe" downgrades are enabled with this
     * boolean. See [FeatureUpdate.FeatureUpdate]. `false` otherwise.
     */
    @Deprecated("")
    constructor(maxVersionLevel: Short, allowDowngrade: Boolean) : this(
        maxVersionLevel = maxVersionLevel,
        upgradeType = if (allowDowngrade) UpgradeType.SAFE_DOWNGRADE else UpgradeType.UPGRADE,
    )

    init {
        require(maxVersionLevel.toInt() != 0 || upgradeType != UpgradeType.UPGRADE) {
            String.format(
                "The downgradeType flag should be set to SAFE or UNSAFE when the provided " +
                        "maxVersionLevel:%d is < 1.",
                maxVersionLevel,
            )
        }
        require(maxVersionLevel >= 0) { "Cannot specify a negative version level." }
    }

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("maxVersionLevel"),
    )
    fun maxVersionLevel(): Short = maxVersionLevel

    @Deprecated("")
    fun allowDowngrade(): Boolean = upgradeType != UpgradeType.UPGRADE

    @Deprecated(
        message = "User property instead",
        replaceWith = ReplaceWith("upgradeType"),
    )
    fun upgradeType(): UpgradeType = upgradeType

    override fun toString(): String = String.format(
        "FeatureUpdate{maxVersionLevel:%d, downgradeType:%s}",
        maxVersionLevel,
        upgradeType,
    )

    enum class UpgradeType(val code: Byte) {

        UNKNOWN(0),

        /**
         * Upgrading the feature level.
         */
        UPGRADE(1),
        /**
         * Only downgrades which do not result in metadata loss are permitted.
         */
        SAFE_DOWNGRADE(2),

        /**
         * Any downgrade, including those which may result in metadata loss, are permitted.
         */
        UNSAFE_DOWNGRADE(3);

        constructor(code: Int) : this(code = code.toByte())

        @Deprecated(
            message = "User property instead",
            replaceWith = ReplaceWith("code"),
        )
        fun code(): Byte = code

        companion object {

            fun fromCode(code: Int): UpgradeType = when (code) {
                1 -> UPGRADE
                2 -> SAFE_DOWNGRADE
                3 -> UNSAFE_DOWNGRADE
                else -> UNKNOWN
            }
        }
    }
}
