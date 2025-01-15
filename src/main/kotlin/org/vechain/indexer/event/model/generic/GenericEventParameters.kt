package org.vechain.indexer.event.model.generic

class GenericEventParameters(
    private val returnValues: Map<String, Any> = emptyMap(),
    private val eventType: String = "GenericEvent",
) : EventParameters {
    override val name: String
        get() = eventType // Set this to the event type or another appropriate value

    override val params: Map<String, Any>
        get() = returnValues // Provide the map of parameters

    override fun getReturnValues(): Map<String, Any> = returnValues

    override fun getEventType(): String = eventType
}
