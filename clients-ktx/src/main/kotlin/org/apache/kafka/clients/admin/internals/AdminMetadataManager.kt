package org.apache.kafka.clients.admin.internals

import org.apache.kafka.clients.MetadataUpdater
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.utils.LogContext
import org.slf4j.Logger

/**
 * Manages the metadata for KafkaAdminClient.
 *
 * This class is not thread-safe.  It is only accessed from the AdminClient
 * service thread (which also uses the NetworkClient).
 *
 * @property refreshBackoffMs The minimum amount of time that we should wait between subsequent
 * retries, when fetching metadata.
 * @property metadataExpireMs The minimum amount of time that we should wait before triggering an
 * automatic metadata refresh.
 */
class AdminMetadataManager private constructor(
    private val log: Logger,
    private val refreshBackoffMs: Long,
    private val metadataExpireMs: Long
) {

    /**
     * Used to update the NetworkClient metadata.
     */
    private val updater = AdminMetadataUpdater()

    /**
     * The current metadata state.
     */
    private var state = State.QUIESCENT

    /**
     * The time in wall-clock milliseconds when we last updated the metadata.
     */
    private var lastMetadataUpdateMs: Long = 0

    /**
     * The time in wall-clock milliseconds when we last attempted to fetch new
     * metadata.
     */
    private var lastMetadataFetchAttemptMs: Long = 0

    /**
     * The current cluster information.
     */
    private var cluster = Cluster.empty()

    /**
     * If we got an authorization exception when we last attempted to fetch
     * metadata, this is it; null, otherwise.
     */
    private var authException: AuthenticationException? = null

    inner class AdminMetadataUpdater : MetadataUpdater {
        override fun fetchNodes(): List<Node?> {
            return cluster.nodes()
        }

        override fun isUpdateDue(now: Long): Boolean {
            return false
        }

        override fun maybeUpdate(now: Long): Long {
            return Long.MAX_VALUE
        }

        override fun handleServerDisconnect(
            now: Long,
            nodeId: String?,
            maybeAuthException: AuthenticationException?
        ) {
            maybeAuthException?.let { updateFailed(it) }
            requestUpdate()
        }

        override fun handleFailedRequest(now: Long, maybeFatalException: KafkaException?) {
            // Do nothing
        }

        override fun handleSuccessfulResponse(
            requestHeader: RequestHeader?,
            now: Long,
            response: MetadataResponse
        ) {
            // Do nothing
        }

        override fun close() = Unit
    }

    /**
     * The current AdminMetadataManager state.
     */
    internal enum class State {
        QUIESCENT,
        UPDATE_REQUESTED,
        UPDATE_PENDING
    }

    constructor(
        logContext: LogContext,
        refreshBackoffMs: Long,
        metadataExpireMs: Long,
    ) : this(
        log = logContext.logger(AdminMetadataManager::class.java),
        refreshBackoffMs = refreshBackoffMs,
        metadataExpireMs = metadataExpireMs,
    )

    fun updater(): AdminMetadataUpdater {
        return updater
    }

    val isReady: Boolean
        get() {
            authException?.let {exception ->
                log.debug("Metadata is not usable: failed to get metadata.", exception)
                throw exception
            }
            if (cluster.nodes().isEmpty()) {
                log.trace("Metadata is not ready: bootstrap nodes have not been initialized yet.")
                return false
            }
            if (cluster.isBootstrapConfigured) {
                log.trace("Metadata is not ready: we have not fetched metadata from the bootstrap nodes yet.")
                return false
            }
            log.trace("Metadata is ready to use.")
            return true
        }

    fun controller(): Node {
        return cluster.controller()
    }

    fun nodeById(nodeId: Int): Node {
        return cluster.nodeById(nodeId)
    }

    fun requestUpdate() {
        if (state == State.QUIESCENT) {
            state = State.UPDATE_REQUESTED
            log.debug("Requesting metadata update.")
        }
    }

    fun clearController() {
        if (cluster.controller() != null) {
            log.trace("Clearing cached controller node {}.", cluster.controller())
            cluster = Cluster(
                cluster.clusterResource().clusterId(),
                cluster.nodes(), emptySet(), emptySet(), emptySet(),
                null
            )
        }
    }

    /**
     * Determine if the AdminClient should fetch new metadata.
     */
    fun metadataFetchDelayMs(now: Long): Long {
        return when (state) {
            State.QUIESCENT ->                 // Calculate the time remaining until the next periodic update.
                // We want to avoid making many metadata requests in a short amount of time,
                // so there is a metadata refresh backoff period.
                Math.max(delayBeforeNextAttemptMs(now), delayBeforeNextExpireMs(now))

            State.UPDATE_REQUESTED ->                 // Respect the backoff, even if an update has been requested
                delayBeforeNextAttemptMs(now)

            else ->                 // An update is already pending, so we don't need to initiate another one.
                Long.MAX_VALUE
        }
    }

    private fun delayBeforeNextExpireMs(now: Long): Long {
        val timeSinceUpdate = now - lastMetadataUpdateMs
        return Math.max(0, metadataExpireMs - timeSinceUpdate)
    }

    private fun delayBeforeNextAttemptMs(now: Long): Long {
        val timeSinceAttempt = now - lastMetadataFetchAttemptMs
        return Math.max(0, refreshBackoffMs - timeSinceAttempt)
    }

    /**
     * Transition into the UPDATE_PENDING state.  Updates lastMetadataFetchAttemptMs.
     */
    fun transitionToUpdatePending(now: Long) {
        state = State.UPDATE_PENDING
        lastMetadataFetchAttemptMs = now
    }

    fun updateFailed(exception: Throwable?) {
        // We depend on pending calls to request another metadata update
        state = State.QUIESCENT
        if (exception is AuthenticationException) {
            log.warn("Metadata update failed due to authentication error", exception)
            authException = exception
        } else {
            log.info("Metadata update failed", exception)
        }
    }

    /**
     * Receive new metadata, and transition into the QUIESCENT state.
     * Updates lastMetadataUpdateMs, cluster, and authException.
     */
    fun update(cluster: Cluster, now: Long) {
        if (cluster.isBootstrapConfigured) {
            log.debug("Setting bootstrap cluster metadata {}.", cluster)
        } else {
            log.debug("Updating cluster metadata to {}", cluster)
            lastMetadataUpdateMs = now
        }
        state = State.QUIESCENT
        authException = null
        if (!cluster.nodes().isEmpty()) {
            this.cluster = cluster
        }
    }
}
