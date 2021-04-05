package com.amazon.elasticsearch.replication.action.changes

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.single.shard.SingleShardRequest
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId

class GetChangesRequest : SingleShardRequest<GetChangesRequest> {

    val shardId : ShardId
    val fromSeqNo: Long
    val toSeqNo: Long

    constructor(shardId: ShardId, fromSeqNo: Long, toSeqNo: Long) : super(shardId.indexName) {
        this.shardId = shardId
        this.fromSeqNo = fromSeqNo
        this.toSeqNo = toSeqNo
    }

    constructor(input : StreamInput) : super(input) {
        this.shardId = ShardId(input)
        this.fromSeqNo = input.readLong()
        this.toSeqNo = input.readVLong()
    }

    override fun validate(): ActionRequestValidationException? {
        return super.validateNonNullIndex()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        shardId.writeTo(out)
        out.writeLong(fromSeqNo)
        out.writeVLong(toSeqNo)
    }
}
