package org.vechain.indexer.event.model

class GenericEventParameters(
    private val returnValues: Map<String, Any> = emptyMap(),
    private val eventType: String = "GenericEvent",
) : EventParameters {
    override fun getReturnValues(): Map<String, Any> = returnValues

    override fun getEventType(): String = eventType
}
