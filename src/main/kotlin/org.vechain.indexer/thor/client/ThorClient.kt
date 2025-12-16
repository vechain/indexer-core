package org.vechain.indexer.thor.client

import org.vechain.indexer.thor.model.*

/**
 * Client interface to access the Thorest RESTful API of the VeChain Thor Network.
 *
 * Use with default implementation {@link org.vechain.indexer.thor.client.DefaultThorClient.class
 * DefaultThorClient} or make you own implementation to pass onto the indexer.
 *
 * @see <a href="https://docs.vechain.org/thor/thorest-api">Thorest API</a>
 */
interface ThorClient {
    suspend fun getBlock(revision: BlockRevision, expanded: Boolean? = null): Block

    suspend fun getBlock(blockNumber: Long, expanded: Boolean? = null): Block =
        getBlock(BlockRevision.Number(blockNumber), expanded)

    suspend fun waitForBlock(revision: BlockRevision, expanded: Boolean? = null): Block

    suspend fun waitForBlock(blockNumber: Long, expanded: Boolean? = null): Block =
        waitForBlock(BlockRevision.Number(blockNumber), expanded)

    suspend fun getBestBlock(expanded: Boolean? = null): Block

    suspend fun getFinalizedBlock(expanded: Boolean? = null): Block

    suspend fun getJustifiedBlock(expanded: Boolean? = null): Block

    suspend fun getEventLogs(req: EventLogsRequest): List<EventLog>

    suspend fun getVetTransfers(req: TransferLogsRequest): List<TransferLog>

    suspend fun inspectClauses(clauses: List<Clause>, blockID: String): List<InspectionResult>
}
