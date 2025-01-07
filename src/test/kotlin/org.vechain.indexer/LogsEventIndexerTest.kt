package org.vechain.indexer

import io.mockk.junit5.MockKExtension
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.thor.client.DefaultThorClient
import org.vechain.indexer.thor.model.BlockIdentifier
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLog
import java.util.*

@ExtendWith(MockKExtension::class)
open class LogsEventIndexerTest {

    @Test
    fun `should be able to index master events very quickly`() = runBlocking {
        val indexer = MasterEventIndexer()

        launch {
            indexer.start()
        }

        for (i in 0..10) {
            delay(1000)
            if (indexer.syncMap.isEmpty()) {
                println("$i seconds - no contracts found")
            } else {
                println("$i seconds - found ${indexer.syncMap.size} contracts (Latest Block = ${indexer.syncMap.values.maxBy { it.meta.blockNumber }.meta.blockNumber})")
            }
        }
    }
}

class MasterEventIndexer : LogsIndexer(
    thorClient = DefaultThorClient("https://mainnet.vechain.org"),
    startBlock = 0,
    syncLoggerInterval = 1000,
    criteriaSet = listOf(
        EventCriteria(
            topic0 = "0xb35bf4274d4295009f1ec66ed3f579db287889444366c03d3a695539372e8951"
        )
    )
) {

    val syncMap: MutableMap<String, EventLog> = Collections.synchronizedMap(mutableMapOf())

    override fun processLogs(events: List<EventLog>) {
        events.forEach { syncMap[it.address] = it }
    }

    override fun getLastSyncedBlock(): BlockIdentifier? { return null }

    override fun rollback(blockNumber: Long) {}
}
