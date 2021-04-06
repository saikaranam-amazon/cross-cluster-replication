package com.amazon.elasticsearch.replication.seqno

import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.core.internal.io.IOUtils
import org.elasticsearch.index.engine.Engine
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.translog.Translog
import org.elasticsearch.indices.IndicesService
import java.io.Closeable
import java.util.*
import kotlin.collections.HashMap

@Singleton
class RemoteClusterTranslogService @Inject constructor(private val indicesService: IndicesService):
        AbstractLifecycleComponent(){


    private val closableResources: MutableList<Closeable> = mutableListOf()
    private val trackingShardId: HashMap<ShardId, Boolean> = HashMap()

    companion object {
        private val log = LogManager.getLogger(RemoteClusterTranslogService::class.java)
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
        // Obj in the list being null or closed has no effect
        IOUtils.close(closableResources)
    }

    @Synchronized
    public fun getHistoryOfOperations(indexShard: IndexShard, startSeqNo: Long, toSeqNo: Long): List<Translog.Operation> {
        if(!trackingShardId.getOrElse(indexShard.shardId(), { false })) {
            log.info("Locking translog")
            val historyLock = indexShard.acquireHistoryRetentionLock(Engine.HistorySource.TRANSLOG)
            closableResources.add(historyLock)
            trackingShardId[indexShard.shardId()] = true
        }
        if(!indexShard.hasCompleteHistoryOperations("odfe_replication", Engine.HistorySource.TRANSLOG, startSeqNo)) {
            log.info("Doesn't have history of operations starting from $startSeqNo")
        }
        log.info("Getting translog snapshot from $startSeqNo")
        val snapshot = indexShard.getHistoryOperations("odfe_replication", Engine.HistorySource.TRANSLOG, startSeqNo)
        val ops = ArrayList<Translog.Operation>(1000000)
        //var ops = arrayListOf<Translog.Operation>()
        var i = 0
        var op  = snapshot.next()
        while(op != null) {
            if(op.seqNo() >= startSeqNo && op.seqNo() <= startSeqNo + 999999) {
                ops.add(op)
            }
            i++
            op = snapshot.next()
        }
        snapshot.close()
        //var sortedOps = ops.sortedBy { it.seqNo() }
        val sortedOps = ArrayList<Translog.Operation>(1000000)
        sortedOps.addAll(ops)
        sortedOps.addAll(ops)
        for(ele in ops) {
            sortedOps[(ele.seqNo() - startSeqNo).toInt()] = ele
        }

        log.info("Starting seqno after sorting ${sortedOps[0].seqNo()} and ending seqno ${sortedOps[ops.size-1].seqNo()}")
        return sortedOps.subList(0, ops.size.coerceAtMost(999999))
    }

}