package org.vechain.indexer

import LogsIndexer
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.CriteriaSet
import org.vechain.indexer.thor.model.EventLog

class ContractIndexer: LogsIndexer(
    thorClient = DefaultThorClient("https://mainnet.vechain.org"),
    criteriaSet = listOf(CriteriaSet(topic0 = "0xb35bf4274d4295009f1ec66ed3f579db287889444366c03d3a695539372e8951"))
) {

    val inMemoryLogs = mutableListOf<EventLog>()

    override fun getLastSyncedBlock(): BlockIdentifier? {
        return null
    }

    override fun rollback(blockNumber: Long) {

    }

    override fun processLogs(logs: Map<Number, List<EventLog>>) {
        logs.values.flatten().forEach {
            inMemoryLogs.add(it)
        }

        logger.info("In memory logs: ${inMemoryLogs.size}")
    }

}

class ContractIndexerTest {
    private val contractIndexer = ContractIndexer()

    protected val logger: Logger = LoggerFactory.getLogger(ContractIndexerTest::class.java)

    @Test
    fun `should process logs`() {
        contractIndexer.startInCoroutine()

        Thread.sleep(1000_000000)

        logger.info("In memory logs: ${contractIndexer.inMemoryLogs.size}")

    }
}
