package org.vechain.indexer.event.model.generic

/**
 * Represents the raw data of a blockchain event.
 *
 * @param data The raw data payload of the event.
 * @param topics A list of topics associated with the event, typically including the event signature
 *   and indexed parameters.
 */
data class RawEvent(
    val data: String, // The raw event data (hex-encoded string).
    val topics: List<String>, // The topics associated with the event (hex-encoded strings).
)
