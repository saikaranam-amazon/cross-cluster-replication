package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class IndexReplicationMasterStatusRequest :  MasterNodeRequest<IndexReplicationMasterStatusRequest>, ToXContentObject {

    lateinit var indexReplicationStatusRequest: IndexReplicationStatusRequest

    constructor(indexReplicationStatusRequest: IndexReplicationStatusRequest?): super() {
        if (indexReplicationStatusRequest != null) {
            this.indexReplicationStatusRequest = indexReplicationStatusRequest
        }
    }


    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder? {
        val responseBuilder =  builder?.startObject()

        return responseBuilder?.let { indexReplicationStatusRequest.toXContent(it, params).endObject() }
    }


}