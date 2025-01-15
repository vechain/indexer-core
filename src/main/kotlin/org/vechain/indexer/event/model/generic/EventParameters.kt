package org.vechain.indexer.event.model.generic

interface EventParameters {
    /**
     * Gets the name of the event.
     *
     * @return The name of the event.
     */
    val name: String

    /**
     * Gets the parameters of the event as a key-value map.
     *
     * @return A map of event parameters.
     */
    val params: Map<String, Any>

    fun getReturnValues(): Map<String, Any>

    fun getEventType(): String
}
