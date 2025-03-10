import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import java.math.BigInteger
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.vechain.indexer.EventMockFactory.arrayAbiElement
import org.vechain.indexer.EventMockFactory.arrayEventClause
import org.vechain.indexer.EventMockFactory.createMockBlockWithTransactions
import org.vechain.indexer.EventMockFactory.createMockTransaction
import org.vechain.indexer.EventMockFactory.intAbiElement
import org.vechain.indexer.EventMockFactory.intEventClause
import org.vechain.indexer.EventMockFactory.stringAbiElement
import org.vechain.indexer.EventMockFactory.stringEventClause
import org.vechain.indexer.EventMockFactory.transferAbiElement
import org.vechain.indexer.EventMockFactory.transferERC71EventClause
import org.vechain.indexer.EventMockFactory.transferERC721AbiElement
import org.vechain.indexer.EventMockFactory.transferEventClause
import org.vechain.indexer.EventMockFactory.tupleAbiElement
import org.vechain.indexer.EventMockFactory.tupleEventClause
import org.vechain.indexer.event.AbiManager
import org.vechain.indexer.event.GenericEventIndexer
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.model.abi.InputOutput
import org.vechain.indexer.event.model.generic.FilterCriteria
import org.vechain.indexer.event.types.Types
import org.vechain.indexer.event.utils.EventUtils
import org.vechain.indexer.thor.model.Block
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.message

@ExtendWith(MockKExtension::class)
internal class GenericEventIndexerTest {
    @MockK private lateinit var abiManager: AbiManager
    private lateinit var genericEventIndexer: GenericEventIndexer

    @BeforeEach
    fun setup() {
        genericEventIndexer = GenericEventIndexer(abiManager)
    }

    @Nested
    inner class GetEventsByFilters {
        @Test
        fun `getBlockEvents should call getBlockEventsByFilters with default parameters`() {
            // Mock dependencies
            val block = mockk<Block>(relaxed = true)
            val abiElement =
                AbiElement(
                    name = "EventName",
                    type = "event",
                    signature = "signature",
                    inputs = listOf()
                )
            val configuredEvents = listOf(abiElement)

            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Spy on the GenericEventIndexer to track internal calls
            val spyGenericEventIndexer = spyk(genericEventIndexer, recordPrivateCalls = true)

            // Execute the function under test
            val result = spyGenericEventIndexer.getBlockEvents(block)

            // Verify the result is not null
            expectThat(result).isNotEqualTo(null)

            // Verify that getBlockEventsByFilters was called with default parameters
            verify {
                spyGenericEventIndexer.getBlockEventsByFilters(
                    block,
                    filterCriteria =
                        FilterCriteria(
                            abiNames = emptyList(),
                            eventNames = emptyList(),
                            contractAddresses = emptyList(),
                            vetTransfers = false,
                        ),
                )
            }
        }

        @Test
        fun `getBlockEventsByFilters should process events based on configured ABIs`() {
            val block = mockk<Block>(relaxed = true)
            val abiElement =
                AbiElement(
                    name = "EventName",
                    type = "event",
                    signature = "signature",
                    inputs = listOf()
                )
            val configuredEvents = listOf(abiElement)

            every { abiManager.getEventsByNames(any(), any()) } returns configuredEvents
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(abiNames = listOf("AbiName"), eventNames = listOf("EventName")),
                )

            expectThat(result).isNotEqualTo(null)
        }

        @Test
        fun `getBlockEvents should return VET transfers as events if set to true`() {
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(arrayEventClause)))
                )

            every { abiManager.getEventsByNames(any(), any()) } returns emptyList()
            every { abiManager.getAbis() } returns emptyMap()

            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(vetTransfers = true),
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].params.getEventType()).isEqualTo("VET_TRANSFER")
        }

        @Test
        fun `getBlockEvents should return events and VET transfers if part of same output`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(stringEventClause, arrayEventClause)))
                )

            val configuredEvents = listOf(stringAbiElement, arrayAbiElement)

            // Mock AbiManager responses
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Execute the method under test
            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(
                        vetTransfers = true,
                    ),
                )

            expectThat(result.size).isEqualTo(5)
            expectThat(result[0].params.getEventType()).isEqualTo("RewardDistributed")
            expectThat(result[1].params.getEventType()).isEqualTo("VET_TRANSFER")
            expectThat(result[2].params.getEventType()).isEqualTo("VET_TRANSFER")
            expectThat(result[3].params.getEventType()).isEqualTo("AllocationVoteCast")
            expectThat(result[4].params.getEventType()).isEqualTo("VET_TRANSFER")
        }

        @Test
        fun `can filter for only VET Transfers by passing in VET_TRANSFER as abi name`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(stringEventClause, arrayEventClause)))
                )

            val configuredEvents = listOf(stringAbiElement, arrayAbiElement)

            // Mock AbiManager responses
            every { abiManager.getAbis() } returns mapOf("event" to configuredEvents)

            // Execute the method under test
            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(eventNames = listOf("VET_TRANSFER"), vetTransfers = true),
                )

            expectThat(result.size).isEqualTo(3)
            expectThat(result[0].params.getEventType()).isEqualTo("VET_TRANSFER")
            expectThat(result[1].params.getEventType()).isEqualTo("VET_TRANSFER")
            expectThat(result[2].params.getEventType()).isEqualTo("VET_TRANSFER")
        }

        @Test
        fun `should return empty list if no matching ABI event found`() {
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(arrayEventClause)))
                )
            val abiElement =
                AbiElement(
                    name = "EventName",
                    type = "event",
                    signature = "signature",
                    inputs = listOf()
                )
            val configuredEvents = listOf(abiElement)

            every { abiManager.getEventsByNames(any(), any()) } returns configuredEvents
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(
                        abiNames = listOf("AbiName"),
                        eventNames = listOf("NonExistentEvent")
                    ),
                )

            expectThat(result).isEqualTo(emptyList())
        }

        @Test
        fun `should only decode events of a specific ABI if ABI name is passed in`() {
            // Create block with transfer event and a different event
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(arrayEventClause, transferEventClause)))
                )

            every { abiManager.getEventsByNames(any(), any()) } returns listOf(transferAbiElement)
            every { abiManager.getAbis() } returns
                mapOf(
                    "AbiName" to listOf(transferAbiElement),
                    "OtherAbiName" to listOf(arrayAbiElement),
                )

            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(abiNames = listOf("OtherAbiName"))
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].params.getReturnValues())
                .isEqualTo(
                    mapOf(
                        "from" to "0x0000000000000000000000000000000000000000",
                        "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                        "value" to BigInteger("50000000000000000000"),
                    ),
                )
        }

        @Test
        fun `should only decode events emitted from a certain contract if a contract address is provided`() {
            // Create block with transfer event and a different event
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(arrayEventClause, transferEventClause)))
                )

            every { abiManager.getAbis() } returns
                mapOf(
                    "AbiName" to listOf(transferAbiElement),
                )

            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    filterCriteria =
                        FilterCriteria(
                            contractAddresses = listOf("0x76Ca782B59C74d088C7D2Cce2f211BC00836c602")
                        ),
                )

            expectThat(result.size).isEqualTo(1)
            expectThat(result[0].params.getReturnValues())
                .isEqualTo(
                    mapOf(
                        "from" to "0x0000000000000000000000000000000000000000",
                        "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                        "value" to BigInteger("50000000000000000000"),
                    ),
                )
        }

        @Test
        fun `Should use the correct event ABI for decoding when multiple ABIs have the same signature but differ in indexed inputs`() {
            // Create block with transfer event and a different event
            val block =
                createMockBlockWithTransactions(
                    listOf(
                        createMockTransaction(listOf(transferERC71EventClause, transferEventClause))
                    )
                )

            every { abiManager.getAbis() } returns
                mapOf(
                    "ERC20" to listOf(transferAbiElement),
                    "ERC721" to listOf(transferERC721AbiElement),
                )

            val result = genericEventIndexer.getBlockEventsByFilters(block)

            expectThat(result.size).isEqualTo(2)
            expectThat(result[0].params.getReturnValues())
                .isEqualTo(
                    mapOf(
                        "from" to "0x9c6c2435175dff3974fedce0d4c72c8db3bf5b74",
                        "to" to "0xc84c3f64f7eefafb62dc53f829219b7393464c45",
                        "value" to BigInteger("5886100000000000000"),
                    ),
                )
            expectThat(result[1].params.getReturnValues())
                .isEqualTo(
                    mapOf(
                        "from" to "0x0000000000000000000000000000000000000000",
                        "to" to "0x8d05673ac6b1dd2c65015893dfc0362f30bde8c5",
                        "value" to BigInteger("50000000000000000000"),
                    ),
                )
        }

        @Test
        fun `should throw error if number of topics in event does not correspond to number of indexed inputs`() {
            // Mock EventUtils.decodeEvent to throw an exception
            mockkObject(EventUtils)
            every { EventUtils.decodeEvent(any(), any()) } throws
                IllegalArgumentException("Invalid data")

            val block =
                createMockBlockWithTransactions(
                    listOf(
                        createMockTransaction(
                            listOf(
                                transferERC71EventClause,
                                transferEventClause,
                            ),
                        ),
                    ),
                )

            every { abiManager.getAbis() } returns
                mapOf(
                    "ERC20" to listOf(transferAbiElement),
                    "ERC721" to listOf(transferERC721AbiElement),
                )

            val result = genericEventIndexer.getBlockEventsByFilters(block)

            // Result should be empty since an exception was thrown
            expectThat(result).isEqualTo(emptyList())

            unmockkObject(EventUtils)
        }
    }

    @Nested
    inner class EventTypes {
        @Test
        fun `should process tuple events correctly`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(tupleEventClause)))
                )

            val configuredEvents = listOf(tupleAbiElement)

            // Mock AbiManager responses
            every { abiManager.getEventsByNames(any(), any()) } returns configuredEvents
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Execute the method under test
            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(
                        abiNames = listOf("AbiName"),
                    ),
                )

            // Assertions
            expectThat(result[0].params.name).isEqualTo("UpdateMarketItem")

            // Decoded values
            val returnValues = result[0].params.getReturnValues()

            val expectedReturnValues =
                mapOf(
                    "itemId" to BigInteger("92"),
                    "tokenId" to BigInteger("9034"),
                    "lister" to "0x71b8e51af9280e8bef933586b7de30eb01c0cd92",
                    "updatedItem" to
                        mapOf(
                            "tokenOwner" to "0x71b8e51af9280e8bef933586b7de30eb01c0cd92",
                            "itemId" to BigInteger("92"),
                            "tokenId" to BigInteger("9034"),
                            "startTime" to BigInteger("0"),
                            "endTime" to BigInteger("0"),
                            "reserveTokenPrice" to BigInteger("0"),
                            "buyoutTokenPrice" to BigInteger("65000000000000000000000"),
                            "listingType" to BigInteger("0"),
                        ),
                )

            expectThat(returnValues).isEqualTo(expectedReturnValues)
        }

        @Test
        fun `should process string events correctly`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(stringEventClause)))
                )

            val configuredEvents = listOf(stringAbiElement)

            // Mock AbiManager responses
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Execute the method under test
            val result = genericEventIndexer.getBlockEventsByFilters(block)

            // Assertions
            expectThat(result[0].params.name).isEqualTo("RewardDistributed")

            // Decoded values
            val returnValues = result[0].params.getReturnValues()

            val expectedReturnValues =
                mapOf(
                    "amount" to BigInteger("628742514970059800"),
                    "appId" to "0x2fc30c2ad41a2994061efaf218f1d52dc92bc4a31a0f02a4916490076a7a393a",
                    "receiver" to "0x7487c2a053f3bfe4626179b020ccff86ab2b1f5d",
                    "proof" to
                        "{\"version\": 2,\"description\": \"This is a photo of a reusable cup, read about the study on impacts here: https://keepcup-study.s3.eu-north-1.amazonaws.com/KeepCup+LCA+Report.pdf\",\"proof\": {\"image\":\"https://blurred-mugshots.s3.eu-north-1.amazonaws.com/1737440160116_image.png\"},\"impact\": {\"carbon\":37,\"energy\":263,\"timber\":23,\"plastic\":3}}",
                    "distributor" to "0xb6f43457600b1f3b7b98fc4394a9f1134ffc721d",
                )

            expectThat(returnValues).isEqualTo(expectedReturnValues)
        }

        @Test
        fun `should process int events correctly`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(intEventClause)))
                )

            val configuredEvents = listOf(intAbiElement)

            // Mock AbiManager responses
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Execute the method under test
            val result = genericEventIndexer.getBlockEventsByFilters(block)

            // Assertions
            expectThat(result[0].params.name).isEqualTo("Conversion")

            // Decoded values
            val returnValues = result[0].params.getReturnValues()

            val expectedReturnValues =
                mapOf(
                    "tradeType" to BigInteger("0"),
                    "_trader" to "0xfc5a8bbff0cfc616472772167024e7cd977f27f6",
                    "_sellAmount" to BigInteger("5000000000000000000000"),
                    "_return" to BigInteger("78889185749897320043721"),
                    "_conversionFee" to BigInteger("39464325037467393719"),
                )

            expectThat(returnValues).isEqualTo(expectedReturnValues)
        }

        @Test
        fun `should process array events correctly`() {
            // Define mock Block object
            val block =
                createMockBlockWithTransactions(
                    listOf(createMockTransaction(listOf(arrayEventClause)))
                )

            val configuredEvents = listOf(arrayAbiElement)

            // Mock AbiManager responses
            every { abiManager.getEventsByNames(any(), any()) } returns configuredEvents
            every { abiManager.getAbis() } returns mapOf("AbiName" to configuredEvents)

            // Execute the method under test
            val result =
                genericEventIndexer.getBlockEventsByFilters(
                    block,
                    FilterCriteria(
                        abiNames = listOf("AbiName"),
                    ),
                )

            // Assertions
            expectThat(result[0].params.name).isEqualTo("AllocationVoteCast")

            // Decoded values
            val returnValues = result[0].params.getReturnValues()

            val expectedReturnValues =
                mapOf(
                    "voter" to "0x54a0ed27c58e4dde7f6c8bbf1f156e1d73a8dc59",
                    "roundId" to BigInteger("27"),
                    "appsIds" to
                        listOf(
                            "0x9643ed1637948cc571b23f836ade2bdb104de88e627fa6e8e3ffef1ee5a1739a",
                            "0x899de0d0f0b39e484c8835b2369194c4c102b230c813862db383d44a4efe14d3",
                        ),
                    "voteWeights" to
                        listOf(
                            BigInteger("625149810000000000000"),
                            BigInteger("625149810000000000000")
                        ),
                )

            expectThat(returnValues).isEqualTo(expectedReturnValues)
        }

        @Test
        fun `should throw error for invalid address length`() {
            val invalidEncoded = "0x1234"
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.ADDRESS.decode(invalidEncoded, String::class.java, "test", null, 0, null)
                }

            expectThat(exception.message.subject)
                .isEqualTo("Invalid address length: $invalidEncoded")
        }

        @Test
        fun `should throw error for invalid bytes32 value`() {
            val invalidEncoded = "0x12346"
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.BYTES.decode(invalidEncoded, String::class.java, "test", null, 0, null)
                }

            expectThat(exception.message.subject).isEqualTo("Invalid bytes value: $invalidEncoded")
        }

        @Test
        fun `should throw error for invalid string decoding`() {
            val invalidData = "0x1234"
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.STRING.decode(
                        invalidData,
                        String::class.java,
                        "test",
                        invalidData,
                        0,
                        null
                    )
                }

            expectThat(exception.message.subject).isEqualTo("Error decoding string at offset: 0")
        }

        @Test
        fun `should throw error for missing components`() {
            val encoded = "0x1234"
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.TUPLE.decode(encoded, Any::class.java, "test", null, 0, null)
                }

            expectThat(exception.message.subject)
                .isEqualTo("Components must be provided for tuple types")
        }

        @Test
        fun `should throw error for data too short for tuple`() {
            val encoded = "0x1234"
            val components = listOf(InputOutput("address", "test", "address"))
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.TUPLE.decode(encoded, Any::class.java, "test", null, 0, components)
                }

            expectThat(exception.message.subject)
                .isEqualTo("Data too short for tuple component at offset 0")
        }

        @Test
        fun `should throw error for invalid array type`() {
            val invalidType = "not-an-array"
            val exception =
                expectThrows<IllegalArgumentException> {
                    Types.ARRAY.decode("0x", Any::class.java, invalidType, null, 0, null)
                }

            expectThat(exception.message.subject).isEqualTo("Invalid array type: $invalidType")
        }

        @Test
        fun `getClaas should return String class for ADDRESS`() {
            val clazz = Types.ADDRESS.getClaas()
            expectThat(clazz).isEqualTo(String::class.java)
        }

        @Test
        fun `getClaas should return BigInteger class for UINT`() {
            val clazz = Types.UINT.getClaas()
            expectThat(clazz).isEqualTo(BigInteger::class.java)
        }

        @Test
        fun `getClaas should return Boolean class for BOOL`() {
            val clazz = Types.BOOL.getClaas()
            expectThat(clazz).isEqualTo(Boolean::class.java)
        }

        @Test
        fun `getClaas should return String class for BYTES32`() {
            val clazz = Types.BYTES.getClaas()
            expectThat(clazz).isEqualTo(String::class.java)
        }

        @Test
        fun `getClaas should return String class for STRING`() {
            val clazz = Types.STRING.getClaas()
            expectThat(clazz).isEqualTo(String::class.java)
        }

        @Test
        fun `getClaas should return List class for ARRAY`() {
            val clazz = Types.ARRAY.getClaas()
            expectThat(clazz).isEqualTo(List::class.java)
        }
    }
}
