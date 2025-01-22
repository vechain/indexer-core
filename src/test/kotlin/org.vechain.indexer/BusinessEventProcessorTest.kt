package org.vechain.indexer

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.vechain.indexer.EventMockFactory.b3trSwapB3trEvent
import org.vechain.indexer.EventMockFactory.b3trSwapVot3Event
import org.vechain.indexer.EventMockFactory.createIndexedEvent
import org.vechain.indexer.EventMockFactory.vot3SwapEventDefinition
import org.vechain.indexer.event.BusinessEventManager
import org.vechain.indexer.event.BusinessEventProcessor
import org.vechain.indexer.event.model.enums.Operator
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotEqualTo
import strikt.assertions.isTrue
import java.math.BigInteger

internal class BusinessEventProcessorTest {
    private val businessEventManager: BusinessEventManager = mockk(relaxed = true)
    private val processor = BusinessEventProcessor(businessEventManager)

    @Nested
    inner class ProcessEventsTest {
        @Test
        fun `should process business events correctly`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)

            val events =
                listOf(b3trSwapB3trIndexedEvent to b3trSwapB3trEvent, b3trSwapVot3IndexedEvent to b3trSwapVot3Event)

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].second.getEventType()).isEqualTo("B3trVot3Swap")
            expectThat(result[0].second.getReturnValues()).isEqualTo(
                mapOf(
                    "user" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "amountB3TR" to BigInteger("50000000000000000000"),
                    "amountVOT3" to BigInteger("50000000000000000000"),
                ),
            )
        }

        @Test
        fun `should process business events with filters correctly`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)

            val events =
                listOf(b3trSwapB3trIndexedEvent to b3trSwapB3trEvent, b3trSwapVot3IndexedEvent to b3trSwapVot3Event)

            every { businessEventManager.getBusinessEventsByNames(listOf("Test")) } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, listOf("Test"))

            // Assert
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].second.getEventType()).isEqualTo("B3trVot3Swap")
            expectThat(result[0].second.getReturnValues()).isEqualTo(
                mapOf(
                    "user" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "amountB3TR" to BigInteger("50000000000000000000"),
                    "amountVOT3" to BigInteger("50000000000000000000"),
                ),
            )
        }

        @Test
        fun `should return all indexed events not part of business events aswell as business events`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)
            val transferIndexedEvent =
                createIndexedEvent("0x4e17357053da4b473e2daa2c65c2c949545724b8", 1, b3trSwapB3trEvent)

            val events =
                listOf(
                    b3trSwapB3trIndexedEvent to b3trSwapB3trEvent,
                    b3trSwapVot3IndexedEvent to b3trSwapVot3Event,
                    transferIndexedEvent to b3trSwapB3trEvent,
                )

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(2)
            expectThat(result[0].second.getEventType()).isEqualTo("Transfer")
            expectThat(result[0].second.getReturnValues()).isEqualTo(
                mapOf(
                    "from" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "to" to "0x76ca782b59c74d088c7d2cce2f211bc00836c602",
                    "value" to BigInteger("50000000000000000000"),
                ),
            )
            expectThat(result[1].second.getEventType()).isEqualTo("B3trVot3Swap")
            expectThat(result[1].second.getReturnValues()).isEqualTo(
                mapOf(
                    "user" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "amountB3TR" to BigInteger("50000000000000000000"),
                    "amountVOT3" to BigInteger("50000000000000000000"),
                ),
            )
        }

        @Test
        fun `should return all indexed events and business events if remove duplicates is set to false`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)
            val transferIndexedEvent =
                createIndexedEvent("0x4e17357053da4b473e2daa2c65c2c949545724b8", 1, b3trSwapB3trEvent)

            val events =
                listOf(
                    b3trSwapB3trIndexedEvent to b3trSwapB3trEvent,
                    b3trSwapVot3IndexedEvent to b3trSwapVot3Event,
                    transferIndexedEvent to b3trSwapB3trEvent,
                )

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, emptyList(), false)

            // Assert
            expectThat(result.size).isEqualTo(4)
            expectThat(result[0].second.getEventType()).isEqualTo("Transfer")
            expectThat(result[1].second.getEventType()).isEqualTo("Transfer")
            expectThat(result[2].second.getEventType()).isEqualTo("Transfer")
            expectThat(result[3].second.getEventType()).isEqualTo("B3trVot3Swap")
        }

        @Test
        fun `should return all indexed events if no business event was found`() {
            // This is not a business event unless another event happens
            val b3trTransferEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)

            val events =
                listOf(b3trTransferEvent to b3trSwapB3trEvent)

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].second.getEventType()).isNotEqualTo("B3trVot3Swap")
            expectThat(result[0].second.getEventType()).isEqualTo("Transfer")
        }

        @Test
        fun `should not mark event as a business event if same clause is set to true but events exist in different clauses`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            // Same TX, different clause
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 1, b3trSwapB3trEvent)

            val events =
                listOf(b3trSwapB3trIndexedEvent to b3trSwapB3trEvent, b3trSwapVot3IndexedEvent to b3trSwapVot3Event)

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.processEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(2)
            // Assert
            expectThat(result[0].second.getEventType()).isEqualTo("Transfer")
            expectThat(result[1].second.getEventType()).isEqualTo("Transfer")
        }
    }

    @Nested
    inner class GetOnlyBusinessEventsTest {
        @Test
        fun `should process business events correctly`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 0, b3trSwapB3trEvent)
            val transferIndexedEvent =
                createIndexedEvent("0x4e17357053da4b473e2daa2c65c2c949545724b8", 1, b3trSwapB3trEvent)

            val events =
                listOf(
                    b3trSwapB3trIndexedEvent to b3trSwapB3trEvent,
                    b3trSwapVot3IndexedEvent to b3trSwapVot3Event,
                    transferIndexedEvent to b3trSwapB3trEvent,
                )

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.getOnlyBusinessEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].second.getEventType()).isEqualTo("B3trVot3Swap")
            expectThat(result[0].second.getReturnValues()).isEqualTo(
                mapOf(
                    "user" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                    "amountB3TR" to BigInteger("50000000000000000000"),
                    "amountVOT3" to BigInteger("50000000000000000000"),
                ),
            )
        }

        @Test
        fun `should not return regular events`() {
            // Arrange
            val b3trSwapVot3IndexedEvent =
                createIndexedEvent("0x76ca782b59c74d088c7d2cce2f211bc00836c602", 0, b3trSwapVot3Event)
            // Same TX, different clause
            val b3trSwapB3trIndexedEvent =
                createIndexedEvent("0x5ef79995fe8a89e0812330e4378eb2660cede699", 1, b3trSwapB3trEvent)

            val events =
                listOf(b3trSwapB3trIndexedEvent to b3trSwapB3trEvent, b3trSwapVot3IndexedEvent to b3trSwapVot3Event)

            every { businessEventManager.getAllBusinessEvents() } returns mapOf("Event" to vot3SwapEventDefinition)

            // Act
            val result = processor.getOnlyBusinessEvents(events, emptyList())

            // Assert
            expectThat(result.size).isEqualTo(0)
        }
    }

    @Nested
    inner class OperatorTest {
        @Test
        fun `EQ operator should return true for equal values`() {
            val result = Operator.EQ.evaluate("test", "test")
            expectThat(result).isTrue()
        }

        @Test
        fun `EQ operator should return false for unequal values`() {
            val result = Operator.EQ.evaluate("test", "different")
            expectThat(result).isFalse()
        }

        @Test
        fun `NE operator should return true for unequal values`() {
            val result = Operator.NE.evaluate("test", "different")
            expectThat(result).isTrue()
        }

        @Test
        fun `NE operator should return false for equal values`() {
            val result = Operator.NE.evaluate("test", "test")
            expectThat(result).isFalse()
        }

        @Test
        fun `GT operator should return true when first value is greater`() {
            val result = Operator.GT.evaluate("b", "a")
            expectThat(result).isTrue()
        }

        @Test
        fun `GT operator should return false when first value is not greater`() {
            val result = Operator.GT.evaluate("a", "b")
            expectThat(result).isFalse()
        }

        @Test
        fun `LT operator should return true when first value is less`() {
            val result = Operator.LT.evaluate("a", "b")
            expectThat(result).isTrue()
        }

        @Test
        fun `LT operator should return false when first value is not less`() {
            val result = Operator.LT.evaluate("b", "a")
            expectThat(result).isFalse()
        }

        @Test
        fun `GE operator should return true when first value is greater`() {
            val result = Operator.GE.evaluate("b", "a")
            expectThat(result).isTrue()
        }

        @Test
        fun `GE operator should return true for equal values`() {
            val result = Operator.GE.evaluate("a", "a")
            expectThat(result).isTrue()
        }

        @Test
        fun `GE operator should return false when first value is less`() {
            val result = Operator.GE.evaluate("a", "b")
            expectThat(result).isFalse()
        }

        @Test
        fun `LE operator should return true when first value is less`() {
            val result = Operator.LE.evaluate("a", "b")
            expectThat(result).isTrue()
        }

        @Test
        fun `LE operator should return true for equal values`() {
            val result = Operator.LE.evaluate("a", "a")
            expectThat(result).isTrue()
        }

        @Test
        fun `LE operator should return false when first value is greater`() {
            val result = Operator.LE.evaluate("b", "a")
            expectThat(result).isFalse()
        }

        @Test
        fun `EQ operator should correctly compare numeric strings`() {
            expectThat(Operator.EQ.evaluate("5", "5")).isTrue()
            expectThat(Operator.EQ.evaluate("5", "10")).isFalse()
        }

        @Test
        fun `NE operator should correctly compare numeric strings`() {
            expectThat(Operator.NE.evaluate("5", "10")).isTrue()
            expectThat(Operator.NE.evaluate("5", "5")).isFalse()
        }

        @Test
        fun `GT operator should correctly compare numeric strings`() {
            expectThat(Operator.GT.evaluate("10", "5")).isTrue()
            expectThat(Operator.GT.evaluate("5", "10")).isFalse()
        }

        @Test
        fun `LT operator should correctly compare numeric strings`() {
            expectThat(Operator.LT.evaluate("5", "10")).isTrue()
            expectThat(Operator.LT.evaluate("10", "5")).isFalse()
        }

        @Test
        fun `GE operator should correctly compare numeric strings`() {
            expectThat(Operator.GE.evaluate("10", "5")).isTrue()
            expectThat(Operator.GE.evaluate("5", "5")).isTrue()
            expectThat(Operator.GE.evaluate("5", "10")).isFalse()
        }

        @Test
        fun `LE operator should correctly compare numeric strings`() {
            expectThat(Operator.LE.evaluate("5", "10")).isTrue()
            expectThat(Operator.LE.evaluate("5", "5")).isTrue()
            expectThat(Operator.LE.evaluate("10", "5")).isFalse()
        }
    }
}
