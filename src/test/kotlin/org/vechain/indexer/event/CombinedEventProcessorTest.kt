package org.vechain.indexer.event

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.fixtures.BlockFixtures
import org.vechain.indexer.fixtures.IndexedEventFixture
import org.vechain.indexer.fixtures.TransferLogFixtures
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty

@ExtendWith(MockKExtension::class)
class EventProcessorTest {

    @MockK lateinit var abiEventProcessor: AbiEventProcessor

    @MockK lateinit var businessEventProcessor: BusinessEventProcessor

    private lateinit var eventProcessor: CombinedEventProcessor

    @BeforeEach
    fun setUp() {
        eventProcessor =
            createTestProcessor(abi = abiEventProcessor, business = businessEventProcessor)
    }

    @Test
    fun `processEvents(block) returns business events`() {
        val block = mockk<Block>()
        val abiEvents =
            listOf(IndexedEventFixture.create(id = "abi-event", txId = "tx1", clauseIndex = 0))
        val businesEvents =
            listOf(IndexedEventFixture.create(id = "business-event", txId = "tx2", clauseIndex = 0))

        every { abiEventProcessor.processEvents(block) } returns abiEvents
        every { businessEventProcessor.processEvents(block) } returns businesEvents

        val result = eventProcessor.processEvents(block)

        expectThat(result).isEqualTo(abiEvents + businesEvents)
    }

    @Test
    fun `processEvents(block) returns abi events if business processor is null`() {
        val block = mockk<Block>()
        val abiEvents = listOf(mockk<IndexedEvent>())

        every { abiEventProcessor.processEvents(block) } returns abiEvents

        val processor = createTestProcessor(abi = abiEventProcessor, business = null)
        val result = processor.processEvents(block)

        expectThat(result).isEqualTo(abiEvents)
    }

    @Test
    fun `processEvents returns business events event when abi processor is null`() {
        val processor = createTestProcessor(abi = null, business = businessEventProcessor)

        val businessEvents = listOf(IndexedEventFixture.create(id = "business-event"))

        every { businessEventProcessor.processEvents(any<Block>()) } returns businessEvents

        val result = processor.processEvents(mockk<Block>())

        expectThat(result).isEqualTo(businessEvents)
    }

    @Test
    fun `processEvents(logs) returns abi events if business processor is null`() {
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())
        val abiEvents = listOf(mockk<IndexedEvent>())

        every { abiEventProcessor.processEvents(eventLogs, transferLogs) } returns abiEvents

        val processor = createTestProcessor(abi = abiEventProcessor, business = null)
        val result = processor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(abiEvents)
    }

    @Test
    fun `processEvents(logs) returns business events when abi processor is null`() {
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())
        val businessEvents = listOf(mockk<IndexedEvent>())

        every { businessEventProcessor.processEvents(eventLogs, transferLogs) } returns
            businessEvents

        val processor = createTestProcessor(abi = null, business = businessEventProcessor)
        val result = processor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(businessEvents)
    }

    @Test
    fun `processEvents(logs) returns abi events when business processor is null`() {
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())
        val contractEvents = listOf(IndexedEventFixture.create(id = "contract-event"))
        val vetTransfers = listOf(IndexedEventFixture.create(id = "vet-transfer"))
        val allEvents = contractEvents + vetTransfers

        every { abiEventProcessor.processEvents(eventLogs, transferLogs) } returns
            contractEvents + vetTransfers

        val processor = createTestProcessor(abi = abiEventProcessor, business = null)
        val result = processor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(allEvents)
    }

    @Test
    fun `processEvents will remove duplicate events when a corresponding business event exists`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event",
                txId = "tx1",
                clauseIndex = 0,
            )
        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event",
                txId = "tx1",
                clauseIndex = 0,
            )
        val block = mockk<Block>()

        every { abiEventProcessor.processEvents(block) } returns listOf(abiEvent)
        every { businessEventProcessor.processEvents(block) } returns listOf(businessEvent)

        val processor =
            createTestProcessor(abi = abiEventProcessor, business = businessEventProcessor)
        val result = processor.processEvents(block)

        expectThat(result).isEqualTo(listOf(businessEvent))
    }

    @Test
    fun `processEvents(logs) will remove duplicate events when a corresponding business event exists`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event",
                txId = "tx1",
                clauseIndex = 0,
            )
        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event",
                txId = "tx1",
                clauseIndex = 0,
            )
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())

        every { abiEventProcessor.processEvents(eventLogs, transferLogs) } returns listOf(abiEvent)
        every { businessEventProcessor.processEvents(eventLogs, transferLogs) } returns
            listOf(businessEvent)

        val processor =
            createTestProcessor(abi = abiEventProcessor, business = businessEventProcessor)
        val result = processor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(listOf(businessEvent))
    }

    @Test
    fun `processEvents will return both abi and business events when no duplicates exist`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event",
                txId = "tx1",
                clauseIndex = 0,
            )
        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event",
                txId = "tx2",
                clauseIndex = 0,
            )
        val block = mockk<Block>()

        every { abiEventProcessor.processEvents(block) } returns listOf(abiEvent)
        every { businessEventProcessor.processEvents(block) } returns listOf(businessEvent)

        val processor =
            createTestProcessor(abi = abiEventProcessor, business = businessEventProcessor)
        val result = processor.processEvents(block)

        expectThat(result).isEqualTo(listOf(abiEvent, businessEvent))
    }

    @Test
    fun `processEvents returns mixed events in chronological order when indexes are available`() {
        val laterAbiEvent =
            IndexedEventFixture.create(
                id = "abi-event",
                txId = "tx2",
                txIndex = 2,
                clauseIndex = 0,
            )
        val earlierBusinessEvent =
            IndexedEventFixture.create(
                id = "business-event",
                txId = "tx1",
                txIndex = 1,
                clauseIndex = 0,
            )
        val block = mockk<Block>()

        every { abiEventProcessor.processEvents(block) } returns listOf(laterAbiEvent)
        every { businessEventProcessor.processEvents(block) } returns listOf(earlierBusinessEvent)

        val result = eventProcessor.processEvents(block)

        expectThat(result).isEqualTo(listOf(earlierBusinessEvent, laterAbiEvent))
    }

    @Test
    fun `create supports transfer-only processing for logs when includeVetTransfers is true`() {
        val processor =
            CombinedEventProcessor.create(
                abiBasePath = null,
                abiEventNames = emptyList(),
                abiContracts = emptyList(),
                includeVetTransfers = true,
                businessEventPath = null,
                businessEventAbiBasePath = null,
                businessEventNames = emptyList(),
                businessEventContracts = emptyList(),
                substitutionParams = emptyMap(),
            )

        val result = processor.processEvents(emptyList(), TransferLogFixtures.LOGS_VET_TRANSFER)

        expectThat(result).isNotEmpty()
        expectThat(result.map { it.eventType }.distinct()).containsExactly("VET_TRANSFER")
    }

    @Test
    fun `create supports transfer-only processing for blocks when includeVetTransfers is true`() {
        val processor =
            CombinedEventProcessor.create(
                abiBasePath = null,
                abiEventNames = emptyList(),
                abiContracts = emptyList(),
                includeVetTransfers = true,
                businessEventPath = null,
                businessEventAbiBasePath = null,
                businessEventNames = emptyList(),
                businessEventContracts = emptyList(),
                substitutionParams = emptyMap(),
            )

        val result = processor.processEvents(BlockFixtures.BLOCK_STARGATE_STAKE)

        expectThat(result).isNotEmpty()
        expectThat(result.map { it.eventType }.distinct()).containsExactly("VET_TRANSFER")
    }

    @Test
    fun `deriveEventCriteria unions and dedupes criteria from both processors`() {
        val abiCriteria =
            listOf(
                EventCriteria(address = "0xaaa", topic0 = "0xsig1"),
                EventCriteria(address = "0xbbb", topic0 = "0xsig1"),
            )
        val businessCriteria =
            listOf(
                EventCriteria(address = "0xaaa", topic0 = "0xsig1"), // duplicate
                EventCriteria(address = "0xccc", topic0 = "0xsig2"),
            )

        every { abiEventProcessor.buildEventCriteria() } returns abiCriteria
        every { businessEventProcessor.buildEventCriteria() } returns businessCriteria

        val result = eventProcessor.deriveEventCriteria()

        expectThat(result)
            .hasSize(3)
            .containsExactlyInAnyOrder(
                EventCriteria(address = "0xaaa", topic0 = "0xsig1"),
                EventCriteria(address = "0xbbb", topic0 = "0xsig1"),
                EventCriteria(address = "0xccc", topic0 = "0xsig2"),
            )
    }

    @Test
    fun `deriveEventCriteria returns empty list when no processors are configured`() {
        val processor = createTestProcessor(abi = null, business = null)

        expectThat(processor.deriveEventCriteria()).isEmpty()
    }

    @Test
    fun `deriveEventCriteria falls back to address-only when cartesian exceeds Thor cap`() {
        // 6 contracts × 4 events = 24 criteria — exceeds Thor's cap of 10. Both addresses
        // (6) and topic0s (4) fit individually; we prefer addresses because they pin to
        // specific contracts, while topic0s like Transfer match every contract that emits
        // them — empirically dominating Thor scan cost.
        val contracts = (1..6).map { "0x${it}" }
        val topic0s = (1..4).map { "0xsig${it}" }
        val cartesian =
            contracts.flatMap { addr ->
                topic0s.map { sig -> EventCriteria(address = addr, topic0 = sig) }
            }

        every { abiEventProcessor.buildEventCriteria() } returns cartesian
        every { businessEventProcessor.buildEventCriteria() } returns emptyList()

        val result = eventProcessor.deriveEventCriteria()

        expectThat(result)
            .hasSize(6)
            .containsExactlyInAnyOrder(contracts.map { EventCriteria(address = it) })
    }

    @Test
    fun `deriveEventCriteria falls back to address-only when topic0s exceed Thor cap`() {
        // 2 contracts × 11 events = 22 criteria — both cartesian and topic0-only exceed 10,
        // but addresses (2) fit.
        val contracts = (1..2).map { "0x${it}" }
        val topic0s = (1..11).map { "0xsig${it}" }
        val cartesian =
            contracts.flatMap { addr ->
                topic0s.map { sig -> EventCriteria(address = addr, topic0 = sig) }
            }

        every { abiEventProcessor.buildEventCriteria() } returns cartesian
        every { businessEventProcessor.buildEventCriteria() } returns emptyList()

        val result = eventProcessor.deriveEventCriteria()

        expectThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(contracts.map { EventCriteria(address = it) })
    }

    @Test
    fun `deriveEventCriteria falls back to topic0-only when only topic0s fit Thor cap`() {
        // 11 contracts × 4 events = 44 criteria — cartesian and address-only (11) both
        // exceed 10. Topic0-only (4) fits, so it wins.
        val contracts = (1..11).map { "0x${it}" }
        val topic0s = (1..4).map { "0xsig${it}" }
        val cartesian =
            contracts.flatMap { addr ->
                topic0s.map { sig -> EventCriteria(address = addr, topic0 = sig) }
            }

        every { abiEventProcessor.buildEventCriteria() } returns cartesian
        every { businessEventProcessor.buildEventCriteria() } returns emptyList()

        val result = eventProcessor.deriveEventCriteria()

        expectThat(result)
            .hasSize(4)
            .containsExactlyInAnyOrder(topic0s.map { EventCriteria(topic0 = it) })
    }

    @Test
    fun `deriveEventCriteria falls back to no filter when both axes exceed Thor cap`() {
        // 11 contracts × 11 events — neither axis fits.
        val contracts = (1..11).map { "0x${it}" }
        val topic0s = (1..11).map { "0xsig${it}" }
        val cartesian =
            contracts.flatMap { addr ->
                topic0s.map { sig -> EventCriteria(address = addr, topic0 = sig) }
            }

        every { abiEventProcessor.buildEventCriteria() } returns cartesian
        every { businessEventProcessor.buildEventCriteria() } returns emptyList()

        expectThat(eventProcessor.deriveEventCriteria()).isEmpty()
    }

    @Test
    fun `create returns empty list when transfer-only requested but includeVetTransfers is false`() {
        val processor =
            CombinedEventProcessor.create(
                abiBasePath = null,
                abiEventNames = emptyList(),
                abiContracts = emptyList(),
                includeVetTransfers = false,
                businessEventPath = null,
                businessEventAbiBasePath = null,
                businessEventNames = emptyList(),
                businessEventContracts = emptyList(),
                substitutionParams = emptyMap(),
            )

        val result = processor.processEvents(emptyList(), TransferLogFixtures.LOGS_VET_TRANSFER)

        expectThat(result).isEmpty()
    }

    private fun createTestProcessor(
        abi: AbiEventProcessor? = null,
        business: BusinessEventProcessor? = null
    ): CombinedEventProcessor {
        return object :
            CombinedEventProcessor(abiEventProcessor = abi, businessEventProcessor = business) {}
    }
}

class DeDuplicateTest {
    private val eventProcessor = EventProcessorWrapper()

    @Test
    fun `deduplicateEvents returns empty list when both lists are empty`() {
        val result = eventProcessor.publicDeduplicateEvents(emptyList(), emptyList())
        expectThat(result).isEmpty()
    }

    @Test
    fun `deduplicateEvents returns only abi events when business events are empty`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event-1",
                txId = "tx1",
                clauseIndex = 0,
            )

        val result = eventProcessor.publicDeduplicateEvents(listOf(abiEvent), emptyList())
        expectThat(result).containsExactly(abiEvent)
    }

    @Test
    fun `deduplicateEvents returns only business events when abi events are empty`() {
        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event-1",
                txId = "tx2",
                clauseIndex = 1,
            )

        val result = eventProcessor.publicDeduplicateEvents(emptyList(), listOf(businessEvent))
        expectThat(result).containsExactly(businessEvent)
    }

    @Test
    fun `deduplicateEvents returns all events when no duplicates exist`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event-1",
                txId = "tx1",
                clauseIndex = 0,
            )

        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event-1",
                txId = "tx2",
                clauseIndex = 1,
            )

        val result = eventProcessor.publicDeduplicateEvents(listOf(abiEvent), listOf(businessEvent))
        expectThat(result).containsExactlyInAnyOrder(abiEvent, businessEvent)
    }

    @Test
    fun `deduplicateEvents removes abi event if duplicate business event exists`() {
        val abiEvent =
            IndexedEventFixture.create(
                id = "abi-event-dup",
                txId = "tx3",
                clauseIndex = 2,
            )

        val businessEvent =
            IndexedEventFixture.create(
                id = "business-event-dup",
                txId = "tx3",
                clauseIndex = 2,
            )

        val result = eventProcessor.publicDeduplicateEvents(listOf(abiEvent), listOf(businessEvent))
        expectThat(result).containsExactly(businessEvent)
    }

    @Test
    fun `deduplicateEvents removes only abi events that overlap with business events`() {
        val abiEvent1 =
            IndexedEventFixture.create(
                id = "abi-event-1",
                txId = "tx1",
                clauseIndex = 0,
            )
        val abiEvent2 =
            IndexedEventFixture.create(
                id = "abi-event-2",
                txId = "tx2",
                clauseIndex = 0,
            )
        val abiEvent3 =
            IndexedEventFixture.create(
                id = "abi-event-3",
                txId = "tx3",
                clauseIndex = 1,
            )

        val businessEvent1 =
            IndexedEventFixture.create(
                id = "business-event-1",
                txId = "tx1",
                clauseIndex = 0,
            )
        val businessEvent2 =
            IndexedEventFixture.create(
                id = "business-event-2",
                txId = "tx3",
                clauseIndex = 1,
            )
        val businessEvent3 =
            IndexedEventFixture.create(
                id = "business-event-3",
                txId = "tx4",
                clauseIndex = 2,
            )

        val result =
            eventProcessor.publicDeduplicateEvents(
                listOf(abiEvent1, abiEvent2, abiEvent3),
                listOf(businessEvent1, businessEvent2, businessEvent3)
            )

        expectThat(result)
            .containsExactlyInAnyOrder(
                abiEvent2, // no overlap
                businessEvent1, // replaces abiEvent1
                businessEvent2, // replaces abiEvent3
                businessEvent3 // no overlap
            )
    }

    inner class EventProcessorWrapper() :
        CombinedEventProcessor(
            abiEventProcessor = mockk<AbiEventProcessor>(),
            businessEventProcessor = mockk<BusinessEventProcessor>()
        ) {
        fun publicDeduplicateEvents(
            abiEvents: List<IndexedEvent>,
            businessEvents: List<IndexedEvent>
        ): List<IndexedEvent> {
            return super.deduplicateEvents(abiEvents, businessEvents)
        }
    }
}
