package com.amazon.elasticsearch.replication.action.status


import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportActionProxy
import org.elasticsearch.transport.TransportService

class TransportIndexReplicationStatusAction  @Inject constructor(threadPool: ThreadPool, clusterService: ClusterService,
                                                                 transportService: TransportService, actionFilters: ActionFilters,
                                                                 indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                 private val indicesService: IndicesService) :
        TransportSingleShardAction<IndexReplicationStatusRequest, StatusResponse>(IndexReplicationStatusAction.NAME,
                threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ::IndexReplicationStatusRequest, ThreadPool.Names.SEARCH) {

    init {
        TransportActionProxy.registerProxyAction(transportService, IndexReplicationStatusAction.NAME, ::StatusResponse)
    }

    companion object {
        private val log = LogManager.getLogger(TransportIndexReplicationStatusAction::class.java)
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(request: IndexReplicationStatusRequest?, shardId: ShardId?, listener: ActionListener<StatusResponse>?) {
        log.error("remove this log 32 urier "+ (request?.indexName ?: null))
        val shards = clusterService.state().routingTable.indicesRouting().get((request?.indexName ?: null)).shards()
        val replayDetailsList: MutableList<ReplayDetails> = mutableListOf<ReplayDetails>()
        log.error("remove this log 333434 urier " + shards)
        shards.forEach {
            val shardid = it.value.shardId
            val indexShard =  indicesService.indexServiceSafe(shardid.index).getShard( shardid.id)
            log.error("reaching here 1616 global check point "+ shardid + "  "+ indexShard.lastSyncedGlobalCheckpoint  + " -- "+  " -- "+ indexShard.lastSyncedGlobalCheckpoint + " "+ indexShard.recoveryState().index)
            replayDetailsList.add( ReplayDetails(true,false))
        }


//        if (request != null) {
//            log.error("remove this log 33 new "+ shardId)
//        }
//        log.error("remove this log 35 new "+ shardId)
//        if (shardId != null) {
//            log.error("remove this log 34 neeww "+ indicesService.indexServiceSafe(shardId?.index).getShard(shardId.id))
//        }
//        log.error("remove this log 35")
        if (listener != null) {
            listener.onResponse(StatusResponse(false,replayDetailsList))
        }
    }

    override fun shardOperation(request: IndexReplicationStatusRequest?, shardId: ShardId?): StatusResponse {
        log.error("remove this log 32 new 2 "+ (request?.indexName ?: null))
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    override fun resolveIndex(request: IndexReplicationStatusRequest?): Boolean {
        return true
    }

    override fun getResponseReader(): Writeable.Reader<StatusResponse> {
        return Writeable.Reader { inp: StreamInput -> StatusResponse(inp)}
        }

    override fun shards(state: ClusterState?, request: InternalRequest?): ShardsIterator? {
        return null
    }


}