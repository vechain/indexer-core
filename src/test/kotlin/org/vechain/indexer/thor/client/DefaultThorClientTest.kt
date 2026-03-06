package org.vechain.indexer.thor.client

import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.fuel.core.FuelError
import com.github.kittinunf.fuel.core.Request
import com.github.kittinunf.fuel.core.Response
import com.github.kittinunf.result.Result
import io.mockk.CapturingSlot
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import io.mockk.unmockkAll
import io.mockk.verify
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vechain.indexer.exception.BlockNotFoundException
import org.vechain.indexer.exception.RateLimitException
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.BlockUnexpanded
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.EventMeta
import org.vechain.indexer.thor.model.InspectionRequest
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.thor.model.LogsOptions
import org.vechain.indexer.thor.model.LogsRange
import org.vechain.indexer.thor.model.Transaction
import org.vechain.indexer.thor.model.TransferCriteria
import org.vechain.indexer.thor.model.TransferLog
import org.vechain.indexer.thor.model.TransferLogsRequest
import org.vechain.indexer.thor.model.TxEvent
import org.vechain.indexer.thor.model.TxTransfer
import org.vechain.indexer.utils.JsonUtils
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsIgnoringCase
import strikt.assertions.isEqualTo

@OptIn(ExperimentalCoroutinesApi::class)
open class DefaultThorClientTest {

    private val baseUrl = "https://thor.node"
    private lateinit var client: ThorClient

    @BeforeEach
    fun setUp() {
        mockkObject(Fuel)
        client = DefaultThorClient(baseUrl)
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
        unmockkAll()
    }

    @Test
    fun `getBlock returns parsed block on success`() = runTest {
        val block = sampleBlock(18)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)))

        val result = client.getBlock(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        verify(exactly = 1) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `getBlock throws BlockNotFoundException when response body is empty`() = runTest {
        val endpoint = "${baseUrl}/blocks/42"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Success(ByteArray(0)))

        val exception =
            assertFailsWith<BlockNotFoundException> { client.getBlock(BlockRevision.Number(42)) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("not found")
        verify(exactly = 1) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `getBlock propagates failure result as exception`() = runTest {
        val endpoint = "${baseUrl}/blocks/9"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Failure(IllegalStateException("boom")))

        val exception = assertFailsWith<FuelError> { client.getBlock(BlockRevision.Number(9)) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("boom")
    }

    @Test
    fun `getBlockUnexpanded returns parsed block on success`() = runTest {
        val block = sampleBlockUnexpanded(18)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to false)
        stubFuelGet(endpoint, params, HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)))

        val result = client.getBlockUnexpanded(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        verify(exactly = 1) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `waitForBlockUnexpanded retries until block becomes available`() = runTest {
        val block = sampleBlockUnexpanded(101)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to false)
        stubFuelGet(
            endpoint,
            params,
            HttpResult.Success(ByteArray(0)),
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)),
        )

        val result = client.waitForBlockUnexpanded(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        expectThat(currentTime).isEqualTo(3_000L)
    }

    @Test
    fun `waitForBlock retries until block becomes available`() = runTest {
        val block = sampleBlock(101)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to true)
        stubFuelGet(
            endpoint,
            params,
            HttpResult.Success(ByteArray(0)),
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)),
        )

        val result = client.waitForBlock(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        expectThat(currentTime).isEqualTo(3_000L)
    }

    @Test
    fun `waitForBlock uses fixed poll delay while block is unavailable`() = runTest {
        val block = sampleBlock(7)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to true)
        val retryResponses =
            List(8) { HttpResult.Success(ByteArray(0)) } +
                HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block))
        stubFuelGet(endpoint, params, *retryResponses.toTypedArray())

        val result = client.waitForBlock(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        expectThat(currentTime).isEqualTo(24_000L)
    }

    @Test
    fun `waitForBlock uses fixed delay for unexpected errors`() = runTest {
        val block = sampleBlock(55)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to true)
        stubFuelGet(
            endpoint,
            params,
            HttpResult.Failure(IllegalStateException("boom")),
            HttpResult.Failure(RuntimeException("still boom")),
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)),
        )

        val result = client.waitForBlock(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        expectThat(currentTime).isEqualTo(20_000L)
        verify(exactly = 3) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `waitForBlock propagates cancellation immediately`() = runTest {
        val cancellingClient =
            object : DefaultThorClient(baseUrl) {
                override suspend fun getBlock(revision: BlockRevision): Block {
                    throw CancellationException("cancelled")
                }
            }

        val exception =
            assertFailsWith<CancellationException> {
                cancellingClient.waitForBlock(BlockRevision.Number(42))
            }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("cancelled")
        expectThat(currentTime).isEqualTo(0L)
    }

    @Test
    fun `getBlock throws RateLimitException on 429`() = runTest {
        val endpoint = "${baseUrl}/blocks/42"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Success(ByteArray(0), statusCode = 429))

        val exception =
            assertFailsWith<RateLimitException> { client.getBlock(BlockRevision.Number(42)) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("rate limited")
        verify(exactly = 1) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `waitForBlock uses rate limit backoff on 429`() = runTest {
        val block = sampleBlock(56)
        val endpoint = "${baseUrl}/blocks/${block.number}"
        val params = listOf("expanded" to true)
        stubFuelGet(
            endpoint,
            params,
            HttpResult.Success(ByteArray(0), statusCode = 429),
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)),
        )

        val result = client.waitForBlock(BlockRevision.Number(block.number))

        expectThat(result).isEqualTo(block)
        expectThat(currentTime).isEqualTo(10_000L)
        verify(exactly = 2) { Fuel.get(endpoint, params) }
    }

    @Test
    fun `getBestBlock returns parsed block`() = runTest {
        val block = sampleBlock(77)
        val endpoint = "${baseUrl}/blocks/best"
        val params = listOf("expanded" to true)
        stubFuelGet(
            endpoint,
            params,
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)),
        )

        val result = client.getBlock(BlockRevision.Keyword.BEST)

        expectThat(result).isEqualTo(block)
    }

    @Test
    fun `getBestBlock propagates failure`() = runTest {
        val endpoint = "${baseUrl}/blocks/best"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Failure(IllegalArgumentException("bad")))

        val exception = assertFailsWith<FuelError> { client.getBlock(BlockRevision.Keyword.BEST) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("bad")
    }

    @Test
    fun `getFinalizedBlock returns parsed block`() = runTest {
        val block = sampleBlock(88)
        val endpoint = "${baseUrl}/blocks/finalized"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(block)))

        val result = client.getBlock(BlockRevision.Keyword.FINALIZED)

        expectThat(result).isEqualTo(block)
    }

    @Test
    fun `getFinalizedBlock propagates failure`() = runTest {
        val endpoint = "${baseUrl}/blocks/finalized"
        val params = listOf("expanded" to true)
        stubFuelGet(endpoint, params, HttpResult.Failure(RuntimeException("oops")))

        val exception =
            assertFailsWith<FuelError> { client.getBlock(BlockRevision.Keyword.FINALIZED) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("oops")
    }

    @Test
    fun `getEventLogs returns parsed logs`() = runTest {
        val request =
            EventLogsRequest(
                range = LogsRange(unit = "block", from = 1, to = 10),
                options = LogsOptions(offset = 0, limit = 10),
                criteriaSet = listOf(EventCriteria(address = "0xabc")),
                order = "asc",
            )
        val logs =
            listOf(
                EventLog(
                    address = "0xabc",
                    topics = listOf("topic"),
                    data = "0xdeadbeef",
                    meta =
                        EventMeta(
                            blockID = "0x1",
                            blockNumber = 1,
                            blockTimestamp = 2,
                            txID = "0x2",
                            txOrigin = "0x3",
                            clauseIndex = 0,
                        ),
                ),
            )
        val endpoint = "${baseUrl}/logs/event"
        val bodySlot = slot<ByteArray>()
        stubFuelPost(
            endpoint,
            null,
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(logs)),
            bodySlot,
        )

        val result = client.getEventLogs(request)

        expectThat(result).containsExactly(*logs.toTypedArray())
        expectThat(JsonUtils.mapper.readValue(bodySlot.captured, EventLogsRequest::class.java))
            .isEqualTo(request)
    }

    @Test
    fun `getEventLogs propagates failure`() = runTest {
        val endpoint = "${baseUrl}/logs/event"
        stubFuelPost(endpoint, null, HttpResult.Failure(IllegalStateException("boom")))

        val exception = assertFailsWith<FuelError> { client.getEventLogs(sampleEventLogsRequest()) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("boom")
    }

    @Test
    fun `getVetTransfers returns parsed transfers`() = runTest {
        val request =
            TransferLogsRequest(
                range = LogsRange(unit = "block", from = 5, to = 15),
                options = LogsOptions(offset = 2, limit = 3),
                criteriaSet = listOf(TransferCriteria(sender = "0x1")),
                order = "desc",
            )
        val transfers =
            listOf(
                TransferLog(
                    sender = "0x1",
                    recipient = "0x2",
                    amount = "0xff",
                    meta =
                        EventMeta(
                            blockID = "0x10",
                            blockNumber = 6,
                            blockTimestamp = 9,
                            txID = "0x8",
                            txOrigin = "0x7",
                            clauseIndex = 1,
                        ),
                ),
            )
        val endpoint = "${baseUrl}/logs/transfer"
        val bodySlot = slot<ByteArray>()
        stubFuelPost(
            endpoint,
            null,
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(transfers)),
            bodySlot,
        )

        val result = client.getVetTransfers(request)

        expectThat(result).containsExactly(*transfers.toTypedArray())
        expectThat(
                JsonUtils.mapper.readValue(bodySlot.captured, TransferLogsRequest::class.java),
            )
            .isEqualTo(request)
    }

    @Test
    fun `getVetTransfers propagates failure`() = runTest {
        val endpoint = "${baseUrl}/logs/transfer"
        stubFuelPost(endpoint, null, HttpResult.Failure(RuntimeException("failure")))

        val exception =
            assertFailsWith<FuelError> { client.getVetTransfers(sampleTransferLogsRequest()) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("failure")
    }

    @Test
    fun `inspectClauses returns parsed inspection results`() = runTest {
        val clauses = listOf(Clause(to = "0xfeed", value = "0x0", data = "0xdead"))
        val blockId = "0x${"456".padStart(64, '0')}"
        val endpoint = "${baseUrl}/accounts/*"
        val parameters = listOf("revision" to blockId)
        val inspectionResults =
            listOf(
                InspectionResult(
                    data = "0x",
                    events = listOf(TxEvent(address = "0xfeed", topics = emptyList(), data = "0x")),
                    transfers =
                        listOf(TxTransfer(sender = "0x1", recipient = "0x2", amount = "0x1")),
                    gasUsed = 0,
                    reverted = false,
                    vmError = null,
                ),
            )
        val bodySlot = slot<ByteArray>()
        stubFuelPost(
            endpoint,
            parameters,
            HttpResult.Success(JsonUtils.mapper.writeValueAsBytes(inspectionResults)),
            bodySlot,
        )

        val result = client.inspectClauses(clauses, BlockRevision.Id(blockId))

        expectThat(result).containsExactly(*inspectionResults.toTypedArray())
        val capturedRequest =
            JsonUtils.mapper.readValue(bodySlot.captured, InspectionRequest::class.java)
        expectThat(capturedRequest.clauses).isEqualTo(clauses)
    }

    @Test
    fun `inspectClauses propagates failure`() = runTest {
        val id = "0x${123.toString(16).padStart(64, '0')}"
        val endpoint = "${baseUrl}/accounts/*"
        val parameters = listOf("revision" to id)
        stubFuelPost(endpoint, parameters, HttpResult.Failure(IllegalStateException("nope")))

        val exception =
            assertFailsWith<FuelError> { client.inspectClauses(emptyList(), BlockRevision.Id(id)) }

        expectThat(exception.message.orEmpty()).containsIgnoringCase("nope")
    }

    private fun sampleBlock(number: Long): Block =
        Block(
            number = number,
            id = "0x$number",
            size = 1,
            parentID = "0x${number - 1}",
            timestamp = 100 + number,
            gasLimit = 10,
            baseFeePerGas = "0x1",
            beneficiary = "0xbeneficiary",
            gasUsed = 5,
            totalScore = 1,
            txsRoot = "0xroot",
            txsFeatures = 0,
            stateRoot = "0xstate",
            receiptsRoot = "0xreceipts",
            com = false,
            signer = "0xsigner",
            isTrunk = true,
            isFinalized = false,
            transactions = emptyList<Transaction>(),
        )

    private fun sampleBlockUnexpanded(number: Long): BlockUnexpanded =
        BlockUnexpanded(
            number = number,
            id = "0x$number",
            size = 1,
            parentID = "0x${number - 1}",
            timestamp = 100 + number,
            gasLimit = 10,
            baseFeePerGas = "0x1",
            beneficiary = "0xbeneficiary",
            gasUsed = 5,
            totalScore = 1,
            txsRoot = "0xroot",
            txsFeatures = 0,
            stateRoot = "0xstate",
            receiptsRoot = "0xreceipts",
            com = false,
            signer = "0xsigner",
            isTrunk = true,
            isFinalized = false,
            transactions = listOf("0x1", "0x2"),
        )

    private fun sampleEventLogsRequest(): EventLogsRequest =
        EventLogsRequest(
            range = LogsRange(unit = null, from = null, to = null),
            options = null,
            criteriaSet = null,
            order = null,
        )

    private fun sampleTransferLogsRequest(): TransferLogsRequest =
        TransferLogsRequest(
            range = LogsRange(unit = null, from = null, to = null),
            options = null,
            criteriaSet = null,
            order = null,
        )

    private sealed class HttpResult {
        abstract val statusCode: Int

        data class Success(val body: ByteArray, override val statusCode: Int = 200) : HttpResult()

        data class Failure(
            val throwable: Throwable,
            override val statusCode: Int = 200,
        ) : HttpResult()
    }

    private fun stubFuelGet(
        url: String,
        parameters: List<Pair<String, Any?>>?,
        vararg results: HttpResult
    ) {
        require(results.isNotEmpty()) { "At least one result expected" }
        val requests =
            results.map { result ->
                val request = mockk<Request>(relaxed = true)
                val response = mockk<Response>(relaxed = true)
                every { request.appendHeader(*anyVararg()) } returns request
                every { response.statusCode } returns result.statusCode
                val payload =
                    when (result) {
                        is HttpResult.Success -> Result.Success(result.body)
                        is HttpResult.Failure ->
                            Result.Failure(FuelError.wrap(result.throwable, response))
                    }
                every { request.response() } returns Triple(request, response, payload)
                request
            }
        every { Fuel.get(url, parameters) } returnsMany requests
    }

    private fun stubFuelPost(
        path: String,
        parameters: List<Pair<String, Any?>>?,
        result: HttpResult,
        bodySlot: CapturingSlot<ByteArray>? = null,
    ) {
        val request = mockk<Request>(relaxed = true)
        val response = mockk<Response>(relaxed = true)
        every { Fuel.post(path = path, parameters = parameters) } returns request
        every { request.appendHeader(*anyVararg()) } returns request
        every { response.statusCode } returns result.statusCode
        if (bodySlot != null) {
            every { request.body(capture(bodySlot)) } returns request
        } else {
            every { request.body(any<ByteArray>()) } returns request
        }
        val payload =
            when (result) {
                is HttpResult.Success -> Result.Success(result.body)
                is HttpResult.Failure -> Result.Failure(FuelError.wrap(result.throwable, response))
            }
        every { request.response() } returns Triple(request, response, payload)
    }
}
