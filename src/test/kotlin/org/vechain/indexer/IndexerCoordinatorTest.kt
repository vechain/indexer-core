package org.vechain.indexer

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.ArrayDeque
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
        val indexer = mockBlockIndexer(statusSupplier = { Status.RUNNING })

        every { indexer.currentBlockNumber } returns 1L

        coEvery { indexer.processBlock(block, any()) } answers
            {
                val reset = invocation.args[1] as () -> Unit
                reset()
            }

        processGroup(
            group = listOf(indexer),
            subscription = subscription,
            allIndexers = listOf(indexer),
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

        every { indexer.currentBlockNumber } returns 2L
        coEvery { indexer.processBlock(block, any()) } answers { status.set(Status.ERROR) }

        processGroup(
            group = listOf(indexer),
            subscription = subscription,
            allIndexers = listOf(indexer),
            onResetRequested = {
                resetCalls.incrementAndGet()
                true
            },
        )

        assertEquals(1, resetCalls.get())
        assertTrue(subscription.closed)
    }

    @Test
    fun `processGroup handles block fetch exceptions`() = runTest {
        val resetCalls = AtomicInteger(0)
        val failure = RuntimeException("boom")
        val subscription = FailingSubscription(nextBlock = 5, failure = failure)
        val indexer1 = mockBlockIndexer()
        val indexer2 = mockBlockIndexer()

        processGroup(
            group = listOf(indexer1),
            subscription = subscription,
            allIndexers = listOf(indexer1, indexer2),
            onResetRequested = {
                resetCalls.incrementAndGet()
                true
            },
        )

        verify(exactly = 1) { indexer1.logBlockFetchError(5, failure) }
        verify(exactly = 1) { indexer2.logBlockFetchError(5, failure) }
        verify(exactly = 1) { indexer1.handleError() }
        verify(exactly = 1) { indexer2.handleError() }
        assertEquals(1, resetCalls.get())
        assertTrue(subscription.closed)
    }

    @Test
    fun `processGroup rethrows cancellation when reset not requested`() {
        val failure = CancellationException("cancelled")
        val closed = AtomicBoolean(false)
        val subscription =
            object : BlockStreamSubscription {
                override val nextBlockNumber: Long = 3

                override suspend fun next(): Block {
                    throw failure
                }

                override suspend fun close() {
                    closed.set(true)
                }
            }
        val indexer = mockBlockIndexer()

        val thrown =
            assertThrows<CancellationException> {
                runTest {
                    processGroup(
                        group = listOf(indexer),
                        subscription = subscription,
                        allIndexers = listOf(indexer),
                        onResetRequested = { false },
                    )
                }
            }

        assertEquals(failure, thrown)
        assertTrue(closed.get())
    }

    @Test
    fun `runGroupForBlock cancels remaining indexers after reset`() = runTest {
        val controller = ResetController {}
        val block = testBlock(6)
        val indexer1 = mockBlockIndexer()
        val indexer2 = mockBlockIndexer()

        every { indexer1.currentBlockNumber } returns 6L
        every { indexer2.currentBlockNumber } returns 6L
        coEvery { indexer1.processBlock(block, any()) } answers
            {
                val reset = invocation.args[1] as () -> Unit
                reset()
            }

        runGroupForBlock(
            group = listOf(indexer1, indexer2),
            block = block,
            resetController = controller,
        )

        verify(exactly = 1) { indexer1.restartIfNeeded() }
        coVerify(exactly = 0) { indexer2.processBlock(any(), any()) }
        verify(exactly = 0) { indexer2.restartIfNeeded() }
        assertTrue(controller.isRequested())
    }

    @Test
    fun `runGroupForBlock requests reset when indexer enters error state`() = runTest {
        val controller = ResetController {}
        val block = testBlock(7)
        val status = AtomicReference(Status.RUNNING)
        val indexer = mockBlockIndexer(statusSupplier = { status.get() })

        every { indexer.currentBlockNumber } returns 7L
        coEvery { indexer.processBlock(block, any()) } answers { status.set(Status.ERROR) }

        runGroupForBlock(
            group = listOf(indexer),
            block = block,
            resetController = controller,
        )

        assertTrue(controller.isRequested())
    }

    @Test
    fun `group supervisor closes stream when groups finish`() = runTest {
        val block = testBlock(8)
        val stream = FakeBlockStream(ArrayDeque(listOf(mutableListOf(block))))
        val indexer = mockBlockIndexer()

        coEvery { indexer.processBlock(block, any()) } returns Unit

        val supervisor =
            GroupSupervisor(
                parentScope = this,
                stream = stream,
                executionGroups = listOf(listOf(indexer)),
                allIndexers = listOf(indexer),
            )

        supervisor.run()

        assertTrue(stream.closed)
        assertEquals(0, stream.resetCount)
        assertTrue(stream.subscriptions.all { it.closed })
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
        every { indexer.currentBlockNumber } returnsMany listOf(3L, 4L)
        val processedBlocks = AtomicInteger(0)

        coEvery { indexer.processBlock(any(), any()) } answers
            {
                val blockArg = invocation.args[0] as Block
                processedBlocks.incrementAndGet()
                if (blockArg.number == firstBlock.number && stream.resetCount == 0) {
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

        var nextCalls: Int = 0
            private set

        override val nextBlockNumber: Long
            get() = cursor

        override suspend fun next(): Block {
            nextCalls++
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

    private class FailingSubscription(
        private val nextBlock: Long,
        private val failure: RuntimeException,
    ) : BlockStreamSubscription {
        var closed: Boolean = false
            private set

        override val nextBlockNumber: Long
            get() = nextBlock

        override suspend fun next(): Block {
            throw failure
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
                val subscription = TestSubscription(mutableListOf())
                subscriptions += subscription
                return subscription
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
