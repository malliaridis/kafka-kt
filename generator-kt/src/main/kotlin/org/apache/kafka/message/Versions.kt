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

package org.apache.kafka.message

/**
 * A version range.
 *
 * A range consists of two 16-bit numbers: the lowest version which is accepted, and the highest.
 * Ranges are inclusive, meaning that both the lowest and the highest version are valid versions.
 * The only exception to this is the NONE range, which contains no versions at all.
 *
 * Version ranges can be represented as strings.
 *
 * A single supported version V is represented as "V".
 *
 * A bounded range from A to B is represented as "A-B".
 *
 * All versions greater than A is represented as "A+".
 *
 * The NONE range is represented as an the string "none".
 */
class Versions {

    val lowest: Short

    val highest: Short

    val isEmpty: Boolean
        get() = lowest > highest

    private constructor() {
        lowest = 0
        highest = -1
    }

    constructor(lowest: Short, highest: Short) {
        this.lowest = lowest
        this.highest = highest

        if (lowest < 0 || highest < 0)
            throw RuntimeException("Invalid version range $lowest to $highest")
    }

    override fun toString(): String = if (isEmpty) NONE_STRING
    else if (lowest == highest) lowest.toString()
    else if (highest == Short.MAX_VALUE) "$lowest+"
    else "$lowest-$highest"

    /**
     * Return the intersection of two version ranges.
     *
     * @param other The other version range.
     * @return A new version range.
     */
    fun intersect(other: Versions): Versions {
        val newLowest = if (lowest > other.lowest) lowest else other.lowest
        val newHighest = if (highest < other.highest) highest else other.highest
        return if (newLowest > newHighest) {
            NONE
        } else Versions(newLowest, newHighest)
    }

    /**
     * Return a new version range that trims some versions from this range, if possible. We can't
     * trim any versions if the resulting range would be disjoint.
     *
     * Some examples:
     * ```
     * 1-4.trim(1-2) = 3-4
     * 3+.trim(4+) = 3
     * 4+.trim(3+) = none
     * 1-5.trim(2-4) = null
     * ```
     *
     * @param other The other version range.
     * @return A new version range.
     */
    fun subtract(other: Versions): Versions? {
        return if (other.lowest <= lowest) {
            // Case 1: other is a superset of this. Trim everything.
            if (other.highest >= highest) NONE
            // Case 2: other is a disjoint version range that is lower than this. Trim nothing.
            else if (other.highest < lowest) this
            // Case 3: trim some values from the beginning of this range.
            //
            // Note: it is safe to assume that other.highest() + 1 will not overflow.
            // The reason is because if other.highest() were Short.MAX_VALUE,
            // other.highest() < highest could not be true.
            else Versions((other.highest + 1).toShort(), highest)
        } else if (other.highest >= highest) {
            val newHighest = other.lowest - 1
            // Case 4: other was NONE. Trim nothing.
            if (newHighest < 0) this
            // Case 5: trim some values from the end of this range.
            else if (newHighest < highest) Versions(lowest, newHighest.toShort())
            // Case 6: other is a disjoint range that is higher than this. Trim nothing.
            else this
        } else null // Case 7: the difference between this and other would be two ranges, not one.
    }

    operator fun contains(version: Short): Boolean = version in lowest..highest

    operator fun contains(other: Versions): Boolean {
        return if (other.isEmpty) true
        else !(lowest > other.lowest || highest < other.highest)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Versions

        if (lowest != other.lowest) return false
        return highest == other.highest
    }

    override fun hashCode(): Int {
        var result = lowest.toInt()
        result = 31 * result + highest
        return result
    }

    companion object {

        fun parse(input: String?, defaultVersions: Versions?): Versions? {
            if (input == null) return defaultVersions

            val trimmedInput = input.trim { it <= ' ' }
            if (trimmedInput.isEmpty()) return defaultVersions
            if (trimmedInput == NONE_STRING) return NONE

            return if (trimmedInput.endsWith("+")) Versions(
                lowest = trimmedInput.substring(0, trimmedInput.length - 1).toShort(),
                highest = Short.MAX_VALUE,
            )
            else {
                val dashIndex = trimmedInput.indexOf("-")
                if (dashIndex < 0) {
                    val version = trimmedInput.toShort()
                    return Versions(lowest = version, highest = version)
                }
                Versions(
                    lowest = trimmedInput.substring(0, dashIndex).toShort(),
                    highest = trimmedInput.substring(dashIndex + 1).toShort(),
                )
            }
        }

        val ALL = Versions(0.toShort(), Short.MAX_VALUE)

        val NONE = Versions()

        const val NONE_STRING = "none"
    }
}
