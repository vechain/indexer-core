package org.vechain.indexer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isTrue

internal class RunnerMetricsTest {

    private class StubIndexer(val name: String) {
        var status = Status.NOT_INITIALISED
        var block = 0L
        val mock =
            mockk<Indexer>(relaxed = true).also {
                every { it.name } returns name
                every { it.getStatus() } answers { status }
                every { it.getCurrentBlockNumber() } answers { block }
            }
    }

    private val registry = SimpleMeterRegistry()
    private val metrics = RunnerMetrics(registry)

    private fun gauge(name: String, indexer: String) =
        registry.get(name).tag(RunnerMetrics.INDEXER_TAG, indexer).gauge().value()

    /** The one `group` tag reading 1 for the indexer, or 0 when every gauge reads 0. */
    private fun group(name: String, indexer: String): Int =
        registry
            .find(name)
            .tag(RunnerMetrics.INDEXER_TAG, indexer)
            .gauges()
            .filter { it.value() == 1.0 }
            .map { it.id.getTag("group")!!.toInt() }
            .singleOrNull() ?: 0

    private fun processed(indexer: String) =
        registry
            .get("indexer.blocks.processed")
            .tag(RunnerMetrics.INDEXER_TAG, indexer)
            .functionCounter()
            .count()

    @Test
    fun `current block is NaN until initialised and status is one-hot`() {
        val a = StubIndexer("a")
        metrics.bind(listOf(a.mock))

        expectThat(gauge("indexer.current.block", "a").isNaN()).isTrue()

        a.status = Status.SYNCING
        a.block = 42
        expectThat(gauge("indexer.current.block", "a")).isEqualTo(42.0)
        Status.entries.forEach { s ->
            val value =
                registry
                    .get("indexer.sync.status")
                    .tags(RunnerMetrics.INDEXER_TAG, "a", "status", s.name)
                    .gauge()
                    .value()
            expectThat(value).isEqualTo(if (s == Status.SYNCING) 1.0 else 0.0)
        }
    }

    @Test
    fun `blocks processed counts forward progress only while processing`() {
        val a = StubIndexer("a")
        metrics.bind(listOf(a.mock))

        a.block = 1_000
        a.status = Status.READY_TO_SYNC
        expectThat(processed("a")).isEqualTo(0.0)

        a.status = Status.FAST_SYNCING
        expectThat(processed("a")).isEqualTo(0.0)
        a.block = 1_500
        expectThat(processed("a")).isEqualTo(500.0)

        a.block = 1_400 // rollback
        expectThat(processed("a")).isEqualTo(500.0)
        a.block = 1_410
        expectThat(processed("a")).isEqualTo(510.0)

        a.status = Status.READY_TO_SYNC
        a.block = 9_000
        expectThat(processed("a")).isEqualTo(510.0)
        a.status = Status.SYNCING
        a.block = 9_001
        expectThat(processed("a")).isEqualTo(510.0)
        a.block = 9_003
        expectThat(processed("a")).isEqualTo(512.0)
    }

    @Test
    fun `blocks processed keeps counting after a garbage collection`() {
        val a = StubIndexer("a")
        metrics.bind(listOf(a.mock))
        a.status = Status.SYNCING
        a.block = 100
        processed("a")

        repeat(3) { System.gc() }
        a.block = 150
        expectThat(processed("a")).isEqualTo(50.0)
    }

    @Test
    fun `catch-up pass and steady state drive phase and catch-up groups`() {
        val (a, b, c) = listOf("a", "b", "c").map { StubIndexer(it).mock }
        metrics.bind(listOf(a, b, c))

        metrics.catchUpPass(listOf(a), listOf(b), listOf(c))

        expectThat(registry.get("indexer.runner.phase").tag("phase", "catch_up").gauge().value())
            .isEqualTo(1.0)
        expectThat(listOf("a", "b", "c").map { group("indexer.runner.catch.up.group", it) })
            .isEqualTo(listOf(1, 2, 3))
        expectThat(
                registry.get("indexer.runner.regroups").tag("kind", "catch_up").counter().count()
            )
            .isEqualTo(1.0)

        metrics.steadyState()

        expectThat(registry.get("indexer.runner.phase").tag("phase", "steady").gauge().value())
            .isEqualTo(1.0)
        expectThat(registry.get("indexer.runner.phase").tag("phase", "catch_up").gauge().value())
            .isEqualTo(0.0)
        expectThat(listOf("a", "b", "c").map { group("indexer.runner.catch.up.group", it) })
            .isEqualTo(listOf(0, 0, 0))
    }

    @Test
    fun `proximity groups number members and clear indexers left out`() {
        val (a, b, c) = listOf("a", "b", "c").map { StubIndexer(it).mock }
        metrics.bind(listOf(a, b, c))

        metrics.proximityGroups(listOf(listOf(a), listOf(b, c)))
        expectThat(listOf("a", "b", "c").map { group("indexer.runner.proximity.group", it) })
            .isEqualTo(listOf(1, 2, 2))
        expectThat(registry.get("indexer.runner.proximity.groups").gauge().value()).isEqualTo(2.0)

        metrics.proximityGroups(listOf(listOf(b)))
        expectThat(listOf("a", "b", "c").map { group("indexer.runner.proximity.group", it) })
            .isEqualTo(listOf(0, 1, 0))
        expectThat(
                registry.get("indexer.runner.regroups").tag("kind", "proximity").counter().count()
            )
            .isEqualTo(2.0)
    }

    @Test
    fun `a runner without a registry records nothing and does not fail`() {
        val noop = RunnerMetrics(null)
        val a = StubIndexer("a").mock
        noop.bind(listOf(a))
        noop.catchUpPass(listOf(a), emptyList(), emptyList())
        noop.proximityGroups(listOf(listOf(a)))
        noop.restart(RunnerMetrics.RestartReason.REORG)
        noop.blockFailure(a)
        noop.startSample().stop(noop.blockTimer(a))
    }
}
