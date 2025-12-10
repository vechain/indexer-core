package org.vechain.indexer.exception

/** Thrown when the API returns HTTP 429 (Too Many Requests). */
class RateLimitException(message: String) : Exception(message)
