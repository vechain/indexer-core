package org.vechain.indexer.event.model

interface EventParameters {
    fun getReturnValues(): Map<String, Any>
    fun getEventType(): String
}
