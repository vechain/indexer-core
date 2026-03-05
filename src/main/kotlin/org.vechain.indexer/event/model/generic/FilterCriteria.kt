package org.vechain.indexer.event.model.generic

/** Represents filter criteria for processing events. */
data class FilterCriteria(
    val eventNames: List<String> = emptyList(),
    val businessEventNames: List<String> = emptyList(),
    val contractAddresses: List<String> = emptyList(),
    val vetTransfers: Boolean = false,
    val removeDuplicates: Boolean = true,
) {
    /**
     * Merges business event names with the existing event filter. Ensures no duplicates by using a
     * Set.
     */
    fun addBusinessEventNames(businessGenericEventNames: List<String>): FilterCriteria {
        if (businessGenericEventNames.isEmpty()) return this

        return this.copy(
            eventNames = this.eventNames + businessGenericEventNames.toSet(),
        )
    }
}
