package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.ReplicationException
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesAction
import com.amazon.elasticsearch.replication.action.replay.ReplayChangesRequest
import com.amazon.elasticsearch.replication.util.suspendExecute
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.elasticsearch.client.Client
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.tasks.TaskId
import java.util.concurrent.ConcurrentHashMap

/**
 * A TranslogSequencer allows multiple producers of [Translog.Operation]s to write them in sequence number order to an
 * index.  It internally uses an [actor] to serialize writes to the index. Producer can call the [send] method
 * to add a batch of operations to the queue.  If the queue is full the producer will be suspended.  Operations can be
 * sent out of order i.e. the operation with sequence number 2 can be sent before the operation with sequence number 1.
 * In this case the Sequencer will internally buffer the operations that cannot be delivered until the missing in-order
 * operations arrive.
 *
 * This uses the ObsoleteCoroutinesApi actor API.  As described in the [actor] docs there is no current replacement for
 * this API and a new one is being worked on to which we can migrate when needed.
 */
@ObsoleteCoroutinesApi
class TranslogSequencer(scope: CoroutineScope, private val followerShardId: ShardId,
                        private val remoteCluster: String, private val remoteIndexName: String,
                        private val parentTaskId: TaskId, private val client: Client,
                        private val rateLimiter: Semaphore, initialSeqNo: Long) {

    private val unAppliedChanges = ConcurrentHashMap<Long, GetChangesResponse>()
    private val log = Loggers.getLogger(javaClass, followerShardId)!!
    private val completed = CompletableDeferred<Unit>()

    private val mutex = Mutex()

    // Channel is unlimited capacity as changes can arrive out of order but must be applied in-order.  If the channel
    // had limited capacity it could deadlock.  Instead we use a separate rate limiter Semaphore whose permits are
    // always acquired in order of sequence number to avoid deadlock.
    private val sequencer = scope.actor<Unit>(capacity = Channel.UNLIMITED) {
        // Exceptions thrown here will mark the channel as failed and the next attempt to send to the channel will
        // raise the same exception.  See [SendChannel.close] method for details.
        var highWatermark = initialSeqNo
        for (m in channel) {
            debugLog("Checking for ${highWatermark + 1}.")
            debugLog("Queue is ${unAppliedChanges.keys().toList()}")
            while (checkIfChangesPresentFrom(highWatermark + 1)) {
                try {
                    val next = getChangesFrom(highWatermark + 1)
                    val replayRequest = ReplayChangesRequest(followerShardId, next.changes, next.maxSeqNoOfUpdatesOrDeletes,
                            remoteCluster, remoteIndexName)
                    replayRequest.parentTask = parentTaskId
                    val startTime = System.nanoTime()
                    val replayResponse = client.suspendExecute(ReplayChangesAction.INSTANCE, replayRequest)
                    val endTime = System.nanoTime()
                    debugLog("ReplayChangesRequest. Total time taken(ms): " + (endTime - startTime)/1000000)
                    if (replayResponse.shardInfo.failed > 0) {
                        replayResponse.shardInfo.failures.forEachIndexed { i, failure ->
                            log.error("Failed replaying changes. Failure:$i:$failure")
                        }
                        throw ReplicationException("failed to replay changes", replayResponse.shardInfo.failures)
                    }
                    highWatermark = next.changes.lastOrNull()?.seqNo() ?: highWatermark
                } finally {
                    debugLog(".")
                    rateLimiter.release()
                }
            }
        }
        completed.complete(Unit)
    }

    private suspend fun getChangesFrom(seqNo: Long): GetChangesResponse {
        return unAppliedChanges.remove(seqNo)!!
        /*
        mutex.withLock {
            return unAppliedChanges.remove(seqNo)!!
        }
        */
    }

    private suspend fun checkIfChangesPresentFrom(seqNo: Long): Boolean {
        return unAppliedChanges.containsKey(seqNo)
        /*
        mutex.withLock {
            return unAppliedChanges.containsKey(seqNo)
        }
        */
    }
    private suspend fun pushChanges(changes : GetChangesResponse) {
        unAppliedChanges[changes.fromSeqNo] = changes
        /*
        mutex.withLock {
            unAppliedChanges[changes.fromSeqNo] = changes
        }
        */
    }

    private fun debugLog(msg: String) {
        log.info("ankikala: ${Thread.currentThread().getName()}: $msg")
    }


    suspend fun close() {
        sequencer.close()
        completed.await()
    }

    suspend fun send(changes : GetChangesResponse) {
        pushChanges(changes)
        sequencer.send(Unit)
    }
}
