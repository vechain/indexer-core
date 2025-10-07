package org.vechain.indexer.thor

import kotlin.collections.ArrayDeque
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.vechain.indexer.BlockTestBuilder
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Clause
import org.vechain.indexer.thor.model.EventLog
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.InspectionResult
import org.vechain.indexer.thor.model.TransferLog
import org.vechain.indexer.thor.model.TransferLogsRequest
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan

@OptIn(ExperimentalCoroutinesApi::class)
class PrefetchingBlockStreamTest {

    @Test
    fun `subscription consumes sequential blocks starting from current provider`() = runTest {
        val blocks = ArrayDeque(createListOfBlocks(5, 10))
        val client = TestThorClient { number ->
            val next =
                blocks.removeFirstOrNull() ?: throw CancellationException("No block for $number")
            assertEquals(
                expected = number,
                actual = next.number,
                message = "Fetcher requested unexpected block number",
            )
            next
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { 5 },
                thorClient = client,
            )

        val subscription = stream.subscribe()
        advanceUntilIdle()

        try {
            val first = awaitNext(subscription)
            val second = awaitNext(subscription)

            expectThat(first.number).isEqualTo(5)
            expectThat(second.number).isEqualTo(6)
            expectThat(subscription.nextBlockNumber).isEqualTo(7)
            expectThat(client.requests.take(2)).containsExactly(5L, 6L)
        } finally {
            subscription.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `reset restarts fetcher from new starting block and allows new subscription`() = runTest {
        var currentStart = 3L
        var blockCounter = 3L
        val client = TestThorClient { _ -> block(blockCounter++) }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { currentStart },
                thorClient = client,
            )

        // First subscription
        val subscription1 = stream.subscribe()
        advanceUntilIdle()

        val first = awaitNext(subscription1)
        expectThat(first.number).isEqualTo(3)
        expectThat(subscription1.nextBlockNumber).isEqualTo(4)

        subscription1.close()
        advanceUntilIdle()

        // Change start block and reset
        currentStart = 10
        blockCounter = 10
        stream.reset()
        advanceUntilIdle()

        // New subscription after reset should start at 10
        val subscription2 = stream.subscribe()
        advanceUntilIdle()

        val second = awaitNext(subscription2)
        expectThat(second.number).isEqualTo(10)
        expectThat(subscription2.nextBlockNumber).isEqualTo(11)

        subscription2.close()
        advanceUntilIdle()
    }

    @Test
    fun `new subscription sees latest reset state`() = runTest {
        var currentStart = 4L
        val blocks = ArrayDeque<Block>()
        val client = TestThorClient { number ->
            blocks.removeFirstOrNull() ?: throw CancellationException("No block for $number")
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 1,
                currentBlockProvider = { currentStart },
                thorClient = client,
            )

        // First subscription to initialize
        blocks.addLast(block(4))
        val firstSub = stream.subscribe()
        advanceUntilIdle()
        awaitNext(firstSub)
        firstSub.close()
        advanceUntilIdle()

        // Reset to new start block
        currentStart = 20
        blocks.addLast(block(20))
        stream.reset()
        advanceUntilIdle()

        // New subscription should start at 20
        val subscriber = stream.subscribe()

        try {
            advanceUntilIdle()
            expectThat(subscriber.nextBlockNumber).isEqualTo(20)
        } finally {
            subscriber.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `only one subscription is allowed at a time`() = runTest {
        val blocks = ArrayDeque(listOf(block(1), block(2), block(3)))
        val client = TestThorClient { _ ->
            blocks.removeFirstOrNull() ?: throw CancellationException("No block")
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 3,
                currentBlockProvider = { 1 },
                thorClient = client,
            )

        val subA = stream.subscribe()
        advanceUntilIdle()

        try {
            // Attempting to create a second subscription should fail
            assertFailsWith<IllegalStateException> { stream.subscribe() }

            // First subscription should still work
            val first = awaitNext(subA)
            expectThat(first.number).isEqualTo(1)
        } finally {
            subA.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `can create new subscription after closing previous one`() = runTest {
        var requestCount = 0
        val client = TestThorClient { blockNum ->
            requestCount++
            block(blockNum)
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { 8 },
                thorClient = client,
            )

        // First subscription
        val subA = stream.subscribe()
        advanceUntilIdle()

        val firstBlock = awaitNext(subA)
        expectThat(firstBlock.number).isEqualTo(8)

        subA.close()
        advanceUntilIdle()

        // After closing, should be able to create a new subscription
        // It will start from block 8 again (based on currentBlockProvider)
        val subB = stream.subscribe()
        advanceUntilIdle()

        val secondBlock = awaitNext(subB)
        expectThat(secondBlock.number).isEqualTo(8)
        expectThat(subB.nextBlockNumber).isEqualTo(9)

        subB.close()
        advanceUntilIdle()

        // Should have made at least 2 requests for block 8
        expectThat(requestCount).isGreaterThan(1)
    }

    @Test
    fun `subscription fails when block number jumps forward`() = runTest {
        val client = TestThorClient { requested ->
            when (requested) {
                50L -> block(50)
                51L -> block(53)
                else -> throw CancellationException("unexpected request $requested")
            }
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { 50 },
                thorClient = client,
            )

        val subscription = stream.subscribe()
        advanceUntilIdle()

        try {
            val first = awaitNext(subscription)
            expectThat(first.number).isEqualTo(50)
            assertFailsWith<IllegalStateException> { awaitNext(subscription) }
        } finally {
            subscription.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `closing stream cancels active subscriptions`() = runTest {
        var blockNum = 0L
        val client = TestThorClient { requested -> block(blockNum++) }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 10,
                currentBlockProvider = { 0 },
                thorClient = client,
            )

        val subscription = stream.subscribe()
        advanceUntilIdle()

        // Get first block to ensure subscription is working
        val first = awaitNext(subscription)
        expectThat(first.number).isEqualTo(0)

        stream.close()
        advanceUntilIdle()

        // After stream close, channel should be closed
        // If there were buffered blocks, they might still be available
        // But eventually we should get a cancellation
        var receivedCancellation = false
        try {
            // Try to get next blocks, eventually channel will be closed
            repeat(20) { awaitNext(subscription) }
        } catch (e: CancellationException) {
            receivedCancellation = true
            expectThat(e.message.orEmpty().lowercase()).contains("closed")
        } finally {
            subscription.close()
            advanceUntilIdle()
        }

        expectThat(receivedCancellation).isEqualTo(true)
    }

    private suspend fun TestScope.awaitNext(subscription: BlockStreamSubscription): Block {
        advanceUntilIdle()
        val block = subscription.next()
        advanceUntilIdle()
        return block
    }

    private fun block(number: Long): Block = BlockTestBuilder.buildBlock(number)

    private class TestThorClient(
        private val responder: (Long) -> Block,
    ) : ThorClient {
        val requests = mutableListOf<Long>()

        override suspend fun waitForBlock(blockNumber: Long): Block {
            requests += blockNumber
            return responder(blockNumber)
        }

        override suspend fun getBlock(blockNumber: Long): Block =
            throw UnsupportedOperationException("Not needed in tests")

        override suspend fun getBestBlock(): Block =
            throw UnsupportedOperationException("Not needed in tests")

        override suspend fun getFinalizedBlock(): Block =
            throw UnsupportedOperationException("Not needed in tests")

        override suspend fun getEventLogs(req: EventLogsRequest): List<EventLog> =
            throw UnsupportedOperationException("Not needed in tests")

        override suspend fun getVetTransfers(req: TransferLogsRequest): List<TransferLog> =
            throw UnsupportedOperationException("Not needed in tests")

        override suspend fun inspectClauses(
            clauses: List<Clause>,
            blockID: String
        ): List<InspectionResult> = throw UnsupportedOperationException("Not needed in tests")
    }

    private fun createListOfBlocks(startBlock: Long, numBlocks: Int): List<Block> {
        return (startBlock until startBlock + numBlocks).map { block(it) }
    }
}
