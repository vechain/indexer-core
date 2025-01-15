package org.vechain.indexer.event.model.generic

data class GenericEventParameters(
    private val returnValues: Map<String, Any> = emptyMap(),
    private val eventType: String = "GenericEvent",
) {
    val name: String
        get() = eventType // Set this to the event type or another appropriate value

    val params: Map<String, Any>
        get() = returnValues // Provide the map of parameters

    fun getReturnValues(): Map<String, Any> = returnValues

    fun getEventType(): String = eventType
}
