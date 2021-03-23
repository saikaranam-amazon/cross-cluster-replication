package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class StatusResponse(val acknowledged: Boolean,val replayDetailsList: List<ReplayDetails>): ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readBoolean(), inp.readGenericValue() as List<ReplayDetails>)


    private val ACKNOWLEDGED = ParseField("acknowledged")
    private val REPLAYDETAILS = ParseField("replay_task_details")

    fun isAcknowledged(): Boolean {
        return acknowledged
    }

    fun isYacknowledged(): List<ReplayDetails> {
        return replayDetailsList
    }

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(acknowledged)
        out.writeGenericValue(replayDetailsList)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(ACKNOWLEDGED.preferredName, isAcknowledged())
        builder.field(REPLAYDETAILS.preferredName, isYacknowledged())
        builder.endObject()
        return builder
    }
}


class ReplayDetails(acknowledged: Boolean,yacknowledged : Boolean): ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readBoolean(),inp.readBoolean())

    private val ACKNOWLEDGED = ParseField("acknowledged")
    private val YACKNOWLEDGED = ParseField("yacknowledged")
    var acknowledged : Boolean = true
    var yacknowledged : Boolean = true

    fun isAcknowledged(): Boolean {
        return acknowledged
    }

    fun isYacknowledged(): Boolean {
        return yacknowledged
    }

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(acknowledged)
        out.writeBoolean(yacknowledged)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(ACKNOWLEDGED.preferredName, isAcknowledged())
        builder.field(YACKNOWLEDGED.preferredName, isYacknowledged())
        builder.endObject()
        return builder
    }
}



class RestoreDetails(acknowledged: Boolean,yacknowledged : Boolean): ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readBoolean(),inp.readBoolean())

    private val ACKNOWLEDGED = ParseField("acknowledged")
    private val YACKNOWLEDGED = ParseField("yacknowledged")
    var acknowledged : Boolean = true
    var yacknowledged : Boolean = true

    fun isAcknowledged(): Boolean {
        return acknowledged
    }

    fun isYacknowledged(): Boolean {
        return yacknowledged
    }

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(acknowledged)
        out.writeBoolean(yacknowledged)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(ACKNOWLEDGED.preferredName, isAcknowledged())
        builder.field(YACKNOWLEDGED.preferredName, isYacknowledged())
        builder.endObject()
        return builder
    }
}

