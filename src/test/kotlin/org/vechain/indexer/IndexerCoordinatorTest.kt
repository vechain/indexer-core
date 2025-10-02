package org.vechain.indexer

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.ArrayDeque
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Transaction

@OptIn(ExperimentalCoroutinesApi::class)
class IndexerCoordinatorTest {

    @Test
    fun `reset controller only triggers once`() {
        val invocations = AtomicInteger(0)
        val controller = ResetController { invocations.incrementAndGet() }

        controller.request()
        controller.request()

        assertTrue(controller.isRequested())
        assertEquals(1, invocations.get())
    }

    @Test
    fun `processGroup requests reset when indexer callback invoked`() = runTest {
        val resetCalls = AtomicInteger(0)
        val block = testBlock(1)
        val subscription = TestSubscription(mutableListOf(block))
        val indexer = mockBlockIndexer(Status.RUNNING)

        coEvery { indexer.processBlock(block, any()) } answers
            {
                val reset = invocation.args[1] as () -> Unit
                reset()
            }

        processGroup(
            group = listOf(indexer),
            subscription = subscription,
            allIndexers = listOf(indexer),
            maxBlockExclusive = null,
            onResetRequested = {
                resetCalls.incrementAndGet()
                true
            },
        )

        assertEquals(1, resetCalls.get())
        assertTrue(subscription.closed)
    }

    @Test
    fun `processGroup requests reset when indexer reports error`() = runTest {
        val resetCalls = AtomicInteger(0)
        val block = testBlock(2)
        val subscription = TestSubscription(mutableListOf(block))
        val status = AtomicReference(Status.RUNNING)
        val indexer = mockBlockIndexer(statusSupplier = { status.get() })

        coEvery { indexer.processBlock(block, any()) } answers { status.set(Status.ERROR) }

        processGroup(
            group = listOf(indexer),
            subscription = subscription,
            allIndexers = listOf(indexer),
            maxBlockExclusive = null,
            onResetRequested = {
                resetCalls.incrementAndGet()
                true
            },
        )

        assertEquals(1, resetCalls.get())
        assertTrue(subscription.closed)
    }

    @Test
    fun `group supervisor resets stream when reset requested`() = runTest {
        val firstBlock = testBlock(3)
        val secondBlock = testBlock(4)
        val stream =
            FakeBlockStream(
                sequences =
                    ArrayDeque(
                        listOf(
                            mutableListOf(firstBlock),
                            mutableListOf(secondBlock),
                        ),
                    ),
            )

        val indexer = mockBlockIndexer()
        val processedBlocks = AtomicInteger(0)

        coEvery { indexer.processBlock(any(), any()) } answers
            {
                val block = invocation.args[0] as Block
                processedBlocks.incrementAndGet()
                if (block.number == firstBlock.number && stream.resetCount == 0) {
                    val reset = invocation.args[1] as () -> Unit
                    reset()
                }
            }

        val supervisor =
            GroupSupervisor(
                parentScope = this,
                stream = stream,
                executionGroups = listOf(listOf(indexer)),
                allIndexers = listOf(indexer),
                maxBlockExclusive = secondBlock.number + 1,
            )

        supervisor.run()

        assertEquals(1, stream.resetCount)
        assertTrue(stream.closed)
        assertEquals(2, processedBlocks.get())
        assertTrue(stream.subscriptions.all { it.closed })
    }

    private fun testBlock(number: Long): Block =
        Block(
            number = number,
            id = "block-$number",
            size = 1,
            parentID = "parent-${number - 1}",
            timestamp = number,
            gasLimit = 1,
            baseFeePerGas = null,
            beneficiary = "beneficiary",
            gasUsed = 1,
            totalScore = 1,
            txsRoot = "root",
            txsFeatures = 0,
            stateRoot = "state",
            receiptsRoot = "receipts",
            com = false,
            signer = "signer",
            isTrunk = true,
            isFinalized = true,
            transactions = emptyList<Transaction>(),
        )

    private fun mockBlockIndexer(
        initialStatus: Status = Status.RUNNING,
        statusSupplier: (() -> Status)? = null,
    ): BlockIndexer {
        val indexer = mockk<BlockIndexer>(relaxed = true)
        val supplier = statusSupplier ?: { initialStatus }
        every { indexer.status } answers { supplier() }
        every { indexer.restartIfNeeded() } returns Unit
        every { indexer.handleError() } returns Unit
        every { indexer.logBlockFetchError(any(), any()) } returns Unit
        return indexer
    }

    private class TestSubscription(
        private val blocks: MutableList<Block>,
    ) : BlockStreamSubscription {
        private var cursor = blocks.firstOrNull()?.number ?: 0L
        var closed: Boolean = false
            private set

        override val nextBlockNumber: Long
            get() = cursor

        override suspend fun next(): Block {
            val block =
                if (blocks.isEmpty()) {
                    throw CancellationException("No more blocks")
                } else {
                    blocks.removeAt(0)
                }
            cursor = block.number + 1
            return block
        }

        override suspend fun close() {
            closed = true
        }
    }

    private class FakeBlockStream(
        private val sequences: ArrayDeque<MutableList<Block>>,
    ) : BlockStream {
        val subscriptions = mutableListOf<TestSubscription>()
        var resetCount: Int = 0
            private set

        var closed: Boolean = false
            private set

        override suspend fun subscribe(): BlockStreamSubscription {
            if (sequences.isEmpty()) {
                return TestSubscription(mutableListOf())
            }
            val subscription = TestSubscription(sequences.removeFirst())
            subscriptions += subscription
            return subscription
        }

        override suspend fun reset() {
            resetCount++
        }

        override suspend fun close() {
            closed = true
        }
    }
}
