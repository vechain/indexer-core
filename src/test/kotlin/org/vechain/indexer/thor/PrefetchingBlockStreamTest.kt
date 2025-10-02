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
            stream.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `reset restarts fetcher from new starting block`() = runTest {
        val responses = mutableMapOf<Long, Block>()
        var currentStart = 3L
        val client = TestThorClient { number ->
            responses.remove(number) ?: throw CancellationException("No block for $number")
        }

        fun enqueue(block: Block) {
            responses[block.number] = block
        }

        enqueue(block(3))

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { currentStart },
                thorClient = client,
            )

        val subscription = stream.subscribe()
        advanceUntilIdle()

        try {
            val first = awaitNext(subscription)
            expectThat(first.number).isEqualTo(3)
            expectThat(subscription.nextBlockNumber).isEqualTo(4)

            currentStart = 10
            enqueue(block(10))

            stream.reset()
            advanceUntilIdle()

            val second = awaitNext(subscription)
            expectThat(second.number).isEqualTo(10)
            expectThat(subscription.nextBlockNumber).isEqualTo(11)

            expectThat(client.requests.count { it == 3L }).isEqualTo(1)
            expectThat(client.requests.count { it == 10L }).isEqualTo(1)
        } finally {
            subscription.close()
            stream.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `new subscription sees latest reset state`() = runTest {
        var currentStart = 4L
        val client = TestThorClient { throw CancellationException("Terminate fetch") }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 1,
                currentBlockProvider = { currentStart },
                thorClient = client,
            )

        currentStart = 20
        stream.reset()
        advanceUntilIdle()

        val subscriber = stream.subscribe()

        try {
            advanceUntilIdle()
            expectThat(subscriber.nextBlockNumber).isEqualTo(20)
        } finally {
            subscriber.close()
            stream.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `multiple subscribers receive same blocks`() = runTest {
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
        val subB = stream.subscribe()
        advanceUntilIdle()

        try {
            val a1 = awaitNext(subA)
            val b1 = awaitNext(subB)
            val a2 = awaitNext(subA)
            val b2 = awaitNext(subB)

            expectThat(a1.number).isEqualTo(1)
            expectThat(b1.number).isEqualTo(1)
            expectThat(a2.number).isEqualTo(2)
            expectThat(b2.number).isEqualTo(2)
            expectThat(client.requests.take(3)).containsExactly(1L, 2L, 3L)
        } finally {
            subA.close()
            subB.close()
            stream.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `closing a subscription does not affect others`() = runTest {
        val blocks = ArrayDeque(createListOfBlocks(8, 3))
        val client = TestThorClient { _ ->
            blocks.removeFirstOrNull() ?: throw CancellationException("No block")
        }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 2,
                currentBlockProvider = { 8 },
                thorClient = client,
            )

        val subA = stream.subscribe()
        val subB = stream.subscribe()
        advanceUntilIdle()

        try {
            awaitNext(subA) // consume first block
            subA.close()

            val remainingARequestCount = client.requests.size

            val firstForB = awaitNext(subB)
            expectThat(firstForB.number).isEqualTo(8)

            val nextForB = awaitNext(subB)
            expectThat(nextForB.number).isEqualTo(9)
            expectThat(subB.nextBlockNumber).isEqualTo(10)
            expectThat(client.requests.size).isEqualTo(remainingARequestCount + 1)
        } finally {
            subB.close()
            stream.close()
            advanceUntilIdle()
        }
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
            stream.close()
            advanceUntilIdle()
        }
    }

    @Test
    fun `closing stream cancels active subscriptions`() = runTest {
        val client = TestThorClient { throw CancellationException("Stop fetch") }

        val stream =
            PrefetchingBlockStream(
                scope = backgroundScope,
                batchSize = 1,
                currentBlockProvider = { 0 },
                thorClient = client,
            )

        val subscription = stream.subscribe()

        try {
            stream.close()
            advanceUntilIdle()

            val error = assertFailsWith<CancellationException> { awaitNext(subscription) }
            expectThat(error.message.orEmpty().lowercase()).contains("cancelled")
        } finally {
            subscription.close()
            stream.close()
            advanceUntilIdle()
        }
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
