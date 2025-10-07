package org.vechain.indexer.orchestration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.BlockIndexer
import org.vechain.indexer.Status
import org.vechain.indexer.fixtures.BlockFixtures.testBlock
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.model.Block

class GroupSupervisorTest {
    @Test
    fun `processGroup requests reset when indexer reports error`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val block = testBlock(2)
        val subscription = TestSubscription(mutableListOf(block))
        val status = AtomicReference(Status.SYNCING)
        val indexer = mockBlockIndexer(statusSupplier = { status.get() })

        every { indexer.getCurrentBlockNumber() } returns 2L
        coEvery { indexer.processBlock(block) } throws RuntimeException("boom")

        processGroup(
            group = listOf(indexer),
            subscription = subscription,
            interruptController = controller,
        )

        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertEquals(InterruptReason.Error, controller.currentReason())
        Assertions.assertTrue(subscription.closed)
    }

    @Test
    fun `processGroup handles block fetch exceptions`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val failure = RuntimeException("boom")
        val subscription = FailingSubscription(nextBlock = 5, failure = failure)
        val indexer1 = mockBlockIndexer()
        val indexer2 = mockBlockIndexer()

        processGroup(
            group = listOf(indexer1, indexer2),
            subscription = subscription,
            interruptController = controller,
        )

        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertEquals(InterruptReason.Error, controller.currentReason())
        Assertions.assertTrue(subscription.closed)
    }

    @Test
    fun `processGroup rethrows cancellation when reset not requested`() {
        val failure = kotlinx.coroutines.CancellationException("cancelled")
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
        val controller = InterruptController {}

        val thrown =
            assertThrows<CancellationException> {
                runTest {
                    processGroup(
                        group = listOf(indexer),
                        subscription = subscription,
                        interruptController = controller,
                    )
                }
            }

        Assertions.assertEquals(failure, thrown)
        Assertions.assertTrue(closed.get())
        Assertions.assertFalse(controller.isRequested())
    }

    @Test
    fun `runGroupForBlock requests reset when indexer throws an exception`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val block = testBlock(7)
        val status = AtomicReference(Status.SYNCING)
        val indexer = mockBlockIndexer(statusSupplier = { status.get() })

        every { indexer.getCurrentBlockNumber() } returns 7L
        coEvery { indexer.processBlock(block) } throws RuntimeException("boom")

        runGroupForBlock(
            group = listOf(indexer),
            block = block,
            interruptController = controller,
        )

        Assertions.assertTrue(controller.isRequested())
        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertEquals(InterruptReason.Error, controller.currentReason())
    }

    @Test
    fun `runGroupForBlock processes indexers sequentially in group order`() = runTest {
        val controller = InterruptController {}
        val block = testBlock(9)
        val indexer1 = mockBlockIndexer()
        val indexer2 = mockBlockIndexer()
        val firstCompleted = AtomicBoolean(false)

        every { indexer1.getCurrentBlockNumber() } returns block.number
        every { indexer2.getCurrentBlockNumber() } returns block.number

        coEvery { indexer1.processBlock(block) } coAnswers
            {
                delay(10)
                firstCompleted.set(true)
            }

        coEvery { indexer2.processBlock(block) } coAnswers
            {
                Assertions.assertTrue(firstCompleted.get())
            }

        runGroupForBlock(
            group = listOf(indexer1, indexer2),
            block = block,
            interruptController = controller,
        )

        Assertions.assertTrue(firstCompleted.get())
    }

    @Test
    fun `runGroupForBlock skips indexer that already processed block`() = runTest {
        val controller = InterruptController {}
        val block = testBlock(10)
        val indexer = mockBlockIndexer()

        every { indexer.getCurrentBlockNumber() } returns block.number + 1

        runGroupForBlock(
            group = listOf(indexer),
            block = block,
            interruptController = controller,
        )

        coVerify(exactly = 0) { indexer.processBlock(any()) }
        Assertions.assertFalse(controller.isRequested())
    }

    @Test
    fun `group supervisor closes stream when groups finish`() = runTest {
        val block = testBlock(8)
        val stream = FakeBlockStream(ArrayDeque(listOf(mutableListOf(block))))
        val indexer = mockBlockIndexer()
        val controller = InterruptController {}

        coEvery { indexer.processBlock(block) } returns Unit

        val supervisor =
            GroupSupervisor(
                scope = this,
                stream = stream,
                executionGroups = listOf(listOf(indexer)),
                interruptController = controller,
            )

        supervisor.run()

        Assertions.assertTrue(stream.closed)
        Assertions.assertEquals(0, stream.resetCount)
        Assertions.assertTrue(stream.subscriptions.all { it.closed })
    }

    private fun mockBlockIndexer(
        initialStatus: Status = Status.SYNCING,
        statusSupplier: (() -> Status)? = null,
    ): BlockIndexer {
        val indexer = mockk<BlockIndexer>(relaxed = true)
        val supplier = statusSupplier ?: { initialStatus }
        every { indexer.getStatus() } answers { supplier() }
        return indexer
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
}
