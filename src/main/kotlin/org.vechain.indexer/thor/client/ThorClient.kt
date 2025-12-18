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
    suspend fun getBlock(revision: BlockRevision): Block

    @Deprecated(
        message = "Use getBlock(BlockRevision.Number(blockNumber)) instead.",
        replaceWith = ReplaceWith("getBlock(BlockRevision.Number(blockNumber))"),
    )
    suspend fun getBlock(blockNumber: Long): Block = getBlock(BlockRevision.Number(blockNumber))

    suspend fun getBlockUnexpanded(revision: BlockRevision): BlockUnexpanded

    suspend fun waitForBlock(revision: BlockRevision): Block

    suspend fun waitForBlockUnexpanded(revision: BlockRevision): BlockUnexpanded

    @Deprecated(
        message = "Use waitForBlock(BlockRevision.Number(blockNumber)) instead.",
        replaceWith = ReplaceWith("waitForBlock(BlockRevision.Number(blockNumber))"),
    )
    suspend fun waitForBlock(blockNumber: Long): Block =
        waitForBlock(BlockRevision.Number(blockNumber))

    @Deprecated(
        message = "Use getBlock(BlockRevision.Keyword.BEST) instead.",
        replaceWith = ReplaceWith("getBlock(BlockRevision.Keyword.BEST)"),
    )
    suspend fun getBestBlock(): Block = getBlock(BlockRevision.Keyword.BEST)

    @Deprecated(
        message = "Use getBlock(BlockRevision.Keyword.FINALIZED) instead.",
        replaceWith = ReplaceWith("getBlock(BlockRevision.Keyword.FINALIZED)"),
    )
    suspend fun getFinalizedBlock(): Block = getBlock(BlockRevision.Keyword.FINALIZED)

    @Deprecated(
        message = "Use getBlock(BlockRevision.Keyword.JUSTIFIED) instead.",
        replaceWith = ReplaceWith("getBlock(BlockRevision.Keyword.JUSTIFIED)"),
    )
    suspend fun getJustifiedBlock(): Block = getBlock(BlockRevision.Keyword.JUSTIFIED)

    suspend fun getEventLogs(req: EventLogsRequest): List<EventLog>

    suspend fun getVetTransfers(req: TransferLogsRequest): List<TransferLog>

    suspend fun inspectClauses(
        clauses: List<Clause>,
        revision: BlockRevision? = null,
    ): List<InspectionResult>

    suspend fun getAccountState(
        address: String,
        revision: BlockRevision? = null
    ): ExecuteAccountResponse

    suspend fun getAccountCode(
        address: String,
        revision: BlockRevision? = null
    ): AccountCodeResponse
}

data class ExecuteAccountResponse(
    val balance: String,
    val energy: String,
    val hasCode: Boolean = false
)

data class AccountCodeResponse(val code: String)
