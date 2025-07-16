package org.vechain.indexer.event

import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.event.model.generic.IndexedEvent
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.TransferLog
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

@ExtendWith(MockKExtension::class)
class EventProcessorTest {

    @MockK
    lateinit var abiEventProcessor: AbiEventProcessor

    @MockK
    lateinit var businessEventProcessor: BusinessEventProcessor

    private lateinit var eventProcessor: EventProcessor

    @BeforeEach
    fun setUp() {
        eventProcessor = createTestProcessor(
            abi = abiEventProcessor,
            business = businessEventProcessor
        )
    }

    @Test
    fun `processEvents(block) returns business events`() {
        val block = mockk<Block>()
        val abiEvents = listOf(mockk<IndexedEvent>())
        val processedEvents = listOf(mockk<IndexedEvent>())

        every { abiEventProcessor.getEvents(block) } returns abiEvents
        every { businessEventProcessor.getEvents(abiEvents) } returns processedEvents

        val result = eventProcessor.processEvents(block)

        expectThat(result).isEqualTo(processedEvents)
        verify { abiEventProcessor.getEvents(block) }
        verify { businessEventProcessor.getEvents(abiEvents) }
    }

    @Test
    fun `processEvents(block) returns abi events if business processor is null`() {
        val block = mockk<Block>()
        val abiEvents = listOf(mockk<IndexedEvent>())

        every { abiEventProcessor.getEvents(block) } returns abiEvents

        val processor = createTestProcessor(abi = abiEventProcessor, business = null)
        val result = processor.processEvents(block)

        expectThat(result).isEqualTo(abiEvents)
    }

    @Test
    fun `processEvents returns empty when abi processor is null`() {
        val processor = createTestProcessor(abi = null, business = businessEventProcessor)
        val result = processor.processEvents(mockk<Block>())

        expectThat(result).isEmpty()
    }

    @Test
    fun `processEvents(logs) returns combined and processed events`() {
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())
        val contractEvents = listOf(mockk<IndexedEvent>())
        val vetTransfers = listOf(mockk<IndexedEvent>())
        val allEvents = contractEvents + vetTransfers
        val finalEvents = listOf(mockk<IndexedEvent>())

        every { abiEventProcessor.decodeLogEvents(eventLogs) } returns contractEvents
        every { abiEventProcessor.decodeLogTransfers(transferLogs) } returns vetTransfers
        every { businessEventProcessor.getEvents(allEvents) } returns finalEvents

        val result = eventProcessor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(finalEvents)
    }

    @Test
    fun `processEvents(logs) returns empty when abi processor is null`() {
        val processor = createTestProcessor(abi = null, business = businessEventProcessor)
        val result = processor.processEvents(listOf(mockk<EventLog>()), listOf(mockk<TransferLog>()))

        expectThat(result).isEmpty()
    }

    @Test
    fun `processEvents(logs) returns abi events when business processor is null`() {
        val eventLogs = listOf(mockk<EventLog>())
        val transferLogs = listOf(mockk<TransferLog>())
        val contractEvents = listOf(mockk<IndexedEvent>())
        val vetTransfers = listOf(mockk<IndexedEvent>())
        val allEvents = contractEvents + vetTransfers

        every { abiEventProcessor.decodeLogEvents(eventLogs) } returns contractEvents
        every { abiEventProcessor.decodeLogTransfers(transferLogs) } returns vetTransfers

        val processor = createTestProcessor(abi = abiEventProcessor, business = null)
        val result = processor.processEvents(eventLogs, transferLogs)

        expectThat(result).isEqualTo(allEvents)
    }

    private fun createTestProcessor(
        abi: AbiEventProcessor? = null,
        business: BusinessEventProcessor? = null
    ): EventProcessor {
        return object : EventProcessor(
            abiEventProcessor = abi,
            businessEventProcessor = business
        ) {}
    }
}