package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.ReplicationPlugin
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.shard.IndexShard
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList

class ShardReplicationChangesTracker(clusterService: ClusterService, indexShard: IndexShard) {
    private val log = Loggers.getLogger(javaClass, indexShard.shardId())!!

    private val SLEEP_TIME_BETWEEN_POLL_MS = 50L
    private val mutex = Mutex()
    private val readersPerShard = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_PARALLEL_READ_PER_SHARD)
    private var backupReaders = 2
    private val rateLimiter = Semaphore(readersPerShard)
    private val missingBatches = Collections.synchronizedList(ArrayList<Pair<Long, Long>>())
    private val observedSeqNoAtLeader = AtomicLong(indexShard.localCheckpoint)
    private val seqNoAlreadyRequested = AtomicLong(indexShard.localCheckpoint)
    private val currentLease = AtomicLong(indexShard.localCheckpoint)

    @Volatile private var batchSize = clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_CHANGE_BATCH_SIZE)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ReplicationPlugin.REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
        if (readersPerShard == 1) {
            backupReaders = 0
        } else if (readersPerShard == 2) {
            backupReaders = 1
        }
    }

    suspend fun requestBatchToFetch():Pair<Long, Long> {
        mutex.withLock {
            debugLog("Mutex lock")
            try {
                debugLog("Waiting for permit..available: ${rateLimiter.availablePermits}")
                while (rateLimiter.availablePermits <= backupReaders && missingBatches.isEmpty()) {
                    delay(SLEEP_TIME_BETWEEN_POLL_MS)
                }

                rateLimiter.acquire()
                debugLog("Waiting to get batch. requested: ${seqNoAlreadyRequested.get()}, leader: ${observedSeqNoAtLeader.get()}")
                while (seqNoAlreadyRequested.get() > observedSeqNoAtLeader.get() + 1 && missingBatches.isEmpty()) {
                    delay(SLEEP_TIME_BETWEEN_POLL_MS)
                }

                return if (missingBatches.isNotEmpty()) {
                    debugLog("Fetching missing batch ${missingBatches[0].first}-${missingBatches[0].second}")
                    missingBatches.removeAt(0)
                } else {
                    val fromSeq = seqNoAlreadyRequested.getAndAdd(batchSize.toLong()) + 1
                    Pair(fromSeq, fromSeq + batchSize - 1)
                }
            } finally {
                debugLog("Mutex unlock")
            }
        }
    }

    fun updateBatchFetched(success: Boolean, fromSeqNoRequested: Long, toSeqNoRequested: Long, toSeqNoReceived: Long, seqNoAtLeader: Long) {
        try {
            if (success) {
                // we shouldn't ever be getting more operations than expected.
                assert(toSeqNoRequested >= toSeqNoReceived) { "${Thread.currentThread().getName()} Got more operations in the batch than requested" }

                debugLog("Updating. ${fromSeqNoRequested}-${toSeqNoReceived}/${toSeqNoRequested}, seqNoAtLeader:$seqNoAtLeader")
                if (toSeqNoRequested > toSeqNoReceived) {
                    if (seqNoAtLeader > toSeqNoReceived) {
                        debugLog("Didn't get the complete batch. Adding the missing operations ${toSeqNoReceived + 1}-${toSeqNoRequested}. Leader at $seqNoAtLeader")
                        missingBatches.add(Pair(toSeqNoReceived + 1, toSeqNoRequested))
                    } else {
                        debugLog(":leader didn't have enough translogs")
                        seqNoAlreadyRequested.getAndUpdate { toSeqNoReceived }
                    }
                }

                // TODO: Instead of checking for missingBatches.size, we can also check if there are any pending operation before updating the new lease.
                currentLease.getAndUpdate { value -> if (toSeqNoReceived + 1 > value && missingBatches.size == 0) toSeqNoReceived + 1 else value }

                observedSeqNoAtLeader.getAndUpdate { value -> if (seqNoAtLeader > value) seqNoAtLeader else value }
                debugLog("current lease: ${currentLease.get()}, observedSeqNoAtLeader: ${observedSeqNoAtLeader.get()}")
            } else {
                // If this is the last batch getting updated.
                val updatedSeqNoRequested = seqNoAlreadyRequested.updateAndGet { value -> if (toSeqNoRequested == value) fromSeqNoRequested - 1 else value }

                // If this was not the last batch, we might have already fetched other batch of operations after this. Adding this to missing.
                if (updatedSeqNoRequested != fromSeqNoRequested - 1) {
                    debugLog("Adding batch to missing $fromSeqNoRequested-$toSeqNoRequested")
                    missingBatches.add(Pair(fromSeqNoRequested, toSeqNoRequested))
                }
            }
        } finally {
            rateLimiter.release()
            debugLog("Releasing permit..available now: ${rateLimiter.availablePermits}")
        }
    }

    fun getSeqNoForLeaseRenewal(): Long {
        return currentLease.get()
    }

    private fun debugLog(msg: String) {
        log.info("ankikala: ${Thread.currentThread().getName()}: $msg")
    }
}
