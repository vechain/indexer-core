package org.vechain.indexer.orchestration

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vechain.indexer.BlockIndexer
import org.vechain.indexer.Indexer
import org.vechain.indexer.Status
import org.vechain.indexer.initialiseAndSyncPhase
import org.vechain.indexer.thor.BlockStream
import org.vechain.indexer.thor.BlockStreamSubscription
import org.vechain.indexer.thor.model.Block
import org.vechain.indexer.thor.model.Transaction

@OptIn(ExperimentalCoroutinesApi::class)
class IndexerOrchestratorTest {

    @Test
    fun `initialiseAndSyncPhase requests error when fast sync fails`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val indexer = mockIndexer()
        coEvery { indexer.fastSync() } throws RuntimeException("boom")

        val thrown =
            runCatching {
                    initialiseAndSyncPhase(
                        scope = this,
                        indexers = listOf(indexer),
                        interruptController = controller,
                    )
                }
                .exceptionOrNull()

        Assertions.assertTrue(thrown is RuntimeException)
        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertEquals(InterruptReason.Error, controller.currentReason())
    }

    @Test
    fun `interruptible supervisor restarts phase after fast sync failure`() = runTest {
        val interrupts = mutableListOf<InterruptReason>()
        val controller = InterruptController { interrupts.add(it) }
        val attempts = AtomicInteger(0)
        val indexer = mockIndexer()
        coEvery { indexer.fastSync() } answers
            {
                if (attempts.incrementAndGet() == 1) {
                    throw RuntimeException("boom")
                }
            }

        val supervisor = InterruptibleSupervisor(scope = this, interruptController = controller)

        val thrown =
            runCatching {
                    supervisor.runPhases(
                        listOf {
                            initialiseAndSyncPhase(
                                scope = this,
                                indexers = listOf(indexer),
                                interruptController = controller,
                            )
                        }
                    )
                }
                .exceptionOrNull()

        Assertions.assertNull(thrown)
        Assertions.assertEquals(2, attempts.get())
        Assertions.assertEquals(listOf(InterruptReason.Error), interrupts)
        Assertions.assertFalse(controller.isRequested())
    }

    @Test
    fun `reset controller only triggers once`() {
        val invocations = AtomicInteger(0)
        val controller = InterruptController { invocations.incrementAndGet() }

        controller.request(InterruptReason.Error)
        controller.request(InterruptReason.Error)

        Assertions.assertTrue(controller.isRequested())
        Assertions.assertEquals(1, invocations.get())
    }

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
        initialStatus: Status = Status.SYNCING,
        statusSupplier: (() -> Status)? = null,
    ): BlockIndexer {
        val indexer = mockk<BlockIndexer>(relaxed = true)
        val supplier = statusSupplier ?: { initialStatus }
        every { indexer.getStatus() } answers { supplier() }
        return indexer
    }

    private fun mockIndexer(): Indexer =
        mockk(relaxed = true) { every { name } returns "test-indexer" }

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
