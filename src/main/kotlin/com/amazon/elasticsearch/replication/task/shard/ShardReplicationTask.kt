package com.amazon.elasticsearch.replication.task.shard

import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_CHANGE_BATCH_SIZE
import com.amazon.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATION_PARALLEL_READ_PER_SHARD
import com.amazon.elasticsearch.replication.action.changes.GetChangesAction
import com.amazon.elasticsearch.replication.action.changes.GetChangesRequest
import com.amazon.elasticsearch.replication.action.changes.GetChangesResponse
import com.amazon.elasticsearch.replication.metadata.getReplicationStateParamsForIndex
import com.amazon.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import com.amazon.elasticsearch.replication.task.CrossClusterReplicationTask
import com.amazon.elasticsearch.replication.task.ReplicationState
import com.amazon.elasticsearch.replication.util.indicesService
import com.amazon.elasticsearch.replication.util.suspendExecuteWithRetries
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.seqno.RetentionLeaseActions
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksNodeService
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class ShardReplicationTask(id: Long, type: String, action: String, description: String, parentTask: TaskId,
                           params: ShardReplicationParams, executor: String, clusterService: ClusterService,
                           threadPool: ThreadPool, client: Client)
    : CrossClusterReplicationTask(id, type, action, description, parentTask, emptyMap(),
        executor, clusterService, threadPool, client) {

    override val remoteCluster: String = params.remoteCluster
    override val followerIndexName: String = params.followerShardId.indexName
    private val remoteShardId = params.remoteShardId
    private val followerShardId = params.followerShardId
    private val remoteClient = client.getRemoteClusterClient(remoteCluster)
    private val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
    private val clusterStateListenerForTaskInterruption = ClusterStateListenerForTaskInterruption()
    private val SLEEP_TIME_BETWEEN_POLL_MS = 10L
    @Volatile private var batchSize = clusterService.clusterSettings.get(REPLICATION_CHANGE_BATCH_SIZE)
    private var readersPerShard = clusterService.clusterSettings.get(REPLICATION_PARALLEL_READ_PER_SHARD)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE) { batchSize = it }
    }

    override val log = Loggers.getLogger(javaClass, followerShardId)!!

    companion object {
        fun taskIdForShard(shardId: ShardId) = "replication:${shardId}"
    }

    @ObsoleteCoroutinesApi
    override suspend fun execute(initialState: PersistentTaskState?) {
        replicate()
    }

    override suspend fun cleanup() {
        retentionLeaseHelper.removeRetentionLease(remoteShardId, followerShardId)
        /* This is to minimise overhead of calling an additional listener as
         * it continues to be called even after the task is completed.
         */
        clusterService.removeListener(clusterStateListenerForTaskInterruption)
    }

    private fun addListenerToInterruptTask() {
        clusterService.addListener(clusterStateListenerForTaskInterruption)
    }

    inner class ClusterStateListenerForTaskInterruption : ClusterStateListener {
        override fun clusterChanged(event: ClusterChangedEvent) {
            log.debug("Cluster metadata listener invoked on shard task...")
            if (event.metadataChanged()) {
                val replicationStateParams = getReplicationStateParamsForIndex(clusterService, followerShardId.indexName)
                if (replicationStateParams == null) {
                    if (PersistentTasksNodeService.Status(State.STARTED) == status)
                        scope.cancel("Shard replication task received an interrupt.")
                }
            }
        }
    }

    override fun indicesOrShards() = listOf(followerShardId)


    @ObsoleteCoroutinesApi
    private suspend fun replicate() {
        updateTaskState(FollowingState)
        // TODO: Acquire retention lease prior to initiating remote recovery

        val followerIndexService = indicesService.indexServiceSafe(followerShardId.index)
        val indexShard = followerIndexService.getShard(followerShardId.id)
        retentionLeaseHelper.addRetentionLease(remoteShardId, indexShard.lastSyncedGlobalCheckpoint, followerShardId)
        // After restore, persisted localcheckpoint is matched with maxSeqNo.
        // Fetch the operations after localCheckpoint from the leader
        //var seqNo = indexShard.localCheckpoint + 1
        val rateLimiter = Semaphore(readersPerShard)

        addListenerToInterruptTask()
        val sequencer = TranslogSequencer(scope, followerShardId, remoteCluster, remoteShardId.indexName,
                TaskId(clusterService.nodeName, id), client, rateLimiter, indexShard.localCheckpoint)
        val changeTracker = ShardReplicationChangesTracker(clusterService, indexShard)
        var lastLeaseRenewalTime = System.currentTimeMillis()
        coroutineScope {
            // TODO: Redesign this to avoid sharing the rateLimiter between this block and the sequencer.
            // This was done as a stopgap to work around a concurrency bug that needed to be fixed fast.
            while (scope.isActive) {
                rateLimiter.acquire()
                launch {
                    debugLog("Spawning")
                    val batchToFetch = changeTracker.requestBatchToFetch()
                    val fromSeqNo = batchToFetch.first
                    val toSeqNo = batchToFetch.second
                    try {
                        debugLog("Getting changes $fromSeqNo-$toSeqNo")
                        val changesResponse = getChanges(fromSeqNo, toSeqNo)
                        val firstSeqNo = changesResponse.changes.firstOrNull()?.seqNo() ?: -1
                        val lastSeqNo = changesResponse.changes.lastOrNull()?.seqNo() ?: -1
                        var nextSeqNo = firstSeqNo
                        var uniqueEntries = 0
                        changesResponse.changes.forEach {op->
                            if(op.seqNo() == nextSeqNo && op.opType().name == Translog.Operation.Type.INDEX.name) {
                                uniqueEntries++
                                nextSeqNo += 1L
                            }
                        }
                        log.info("Got ${changesResponse.changes.size} changes starting from seqNo: $fromSeqNo")
                        log.info("firstSeqNo: $firstSeqNo, lastSeqNo: $lastSeqNo, uniqueEntries: $uniqueEntries")
                        sequencer.send(changesResponse)
                        changeTracker.updateBatchFetched(true, fromSeqNo, toSeqNo, changesResponse.changes.lastOrNull()?.seqNo() ?: fromSeqNo - 1,
                                changesResponse.lastSyncedGlobalCheckpoint)
                    } catch (e: ElasticsearchTimeoutException) {
                        debugLog("Timed out waiting for new changes.")
                        changeTracker.updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1,-1)
                    } finally {
                        rateLimiter.release()
                    }
                }
                //Wait just enough for the coroutine to start
                //delay(SLEEP_TIME_BETWEEN_POLL_MS)

                // Renew lease after a pre-determined time period.
                if (System.currentTimeMillis() - lastLeaseRenewalTime > 5000) {
                    val seqNoForRenewal = changeTracker.getSeqNoForLeaseRenewal()
                    log.info("ankikala: renewing lease to $seqNoForRenewal")
                    lastLeaseRenewalTime = System.currentTimeMillis()
                    retentionLeaseHelper.renewRetentionLease(remoteShardId, seqNoForRenewal, followerShardId)
                }
            }
        }
        sequencer.close()
    }

    private fun debugLog(msg: String) {
        log.info("ankikala: ${Thread.currentThread().getName()}: $msg")
    }

    private suspend fun getChanges(fromSeqNo: Long, toSeqNo: Long): GetChangesResponse {
        val request = GetChangesRequest(remoteShardId, fromSeqNo, toSeqNo)
        return remoteClient.suspendExecuteWithRetries(action = GetChangesAction.INSTANCE, req = request, log = log)
    }

    override fun toString(): String {
        return "ShardReplicationTask(from=${remoteCluster}$remoteShardId to=$followerShardId)"
    }

    override fun replicationTaskResponse(): CrossClusterReplicationTaskResponse {
        // Cancellation and valid executions are marked as completed
        return CrossClusterReplicationTaskResponse(ReplicationState.COMPLETED.name)
    }
}
