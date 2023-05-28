package org.apache.kafka.clients.admin

import java.util.*

/**
 * This class is used to describe the state of the quorum received in DescribeQuorumResponse.
 * @property leaderId
 * @property leaderEpoch
 * @property highWatermark
 * @property voters
 * @property observers
 */
class QuorumInfo internal constructor(
    val leaderId: Int,
    val leaderEpoch: Long,
    val highWatermark: Long,
    val voters: List<ReplicaState>,
    val observers: List<ReplicaState>
) {

    override fun toString(): String {
        return "QuorumInfo(" +
                "leaderId=" + leaderId +
                ", leaderEpoch=" + leaderEpoch +
                ", highWatermark=" + highWatermark +
                ", voters=" + voters +
                ", observers=" + observers +
                ')'
    }

    /**
     * @property replicaId The ID for this replica
     * @property logEndOffset The logEndOffset known by the leader for this replica.
     * @property lastFetchTimestamp The last millisecond timestamp that the leader received a fetch from this replica,
     * `null` if not known.
     * @property lastCaughtUpTimestamp The last millisecond timestamp at which this replica was known to be caught up
     * with the leader, `null` if not known.
     */
    data class ReplicaState internal constructor(
        val replicaId: Int = 0,
        val logEndOffset: Long = 0,
        val lastFetchTimestamp: Long? = null,
        val lastCaughtUpTimestamp: Long? = null,
    ) {
        override fun toString(): String {
            return "ReplicaState(" +
                    "replicaId=" + replicaId +
                    ", logEndOffset=" + logEndOffset +
                    ", lastFetchTimestamp=" + lastFetchTimestamp +
                    ", lastCaughtUpTimestamp=" + lastCaughtUpTimestamp +
                    ')'
        }
    }
}
