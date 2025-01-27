package org.vechain.indexer.event.model.generic

/**
 * Represents filter criteria for processing events.
 */
data class FilterCriteria(
    val abiNames: List<String> = emptyList(),
    val eventNames: List<String> = emptyList(),
    val businessEventNames: List<String> = emptyList(),
    val contractAddresses: List<String> = emptyList(),
    val vetTransfers: Boolean = false,
) {
    fun addBusinessEventNames(businessGenericEventNames: List<String>): FilterCriteria =
        this.copy(
            eventNames =
                this.eventNames + businessGenericEventNames,
        )
}
