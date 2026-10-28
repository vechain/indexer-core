package org.vechain.indexer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

// Meters for IndexerRunner; see docs/Metrics.md.
internal class RunnerMetrics(meterRegistry: MeterRegistry?) {

    enum class Phase {
        CATCH_UP,
        STEADY,
    }

    enum class RestartReason {
        REORG,
        STUCK_BLOCK,
    }

    private val registry: MeterRegistry = meterRegistry ?: CompositeMeterRegistry()
    private val phase = AtomicReference<Phase?>(null)
    private val proximityGroupCount = AtomicInteger(0)
    private val catchUpGroup = ConcurrentHashMap<String, AtomicInteger>()
    private val proximityGroup = ConcurrentHashMap<String, AtomicInteger>()

    fun bind(indexers: List<Indexer>) {
        Phase.entries.forEach { p ->
            Gauge.builder("indexer.runner.phase", phase) { if (it.get() == p) 1.0 else 0.0 }
                .tag("phase", p.name.lowercase())
                .strongReference(true)
                .register(registry)
        }
        Gauge.builder("indexer.runner.proximity.groups", proximityGroupCount) { it.toDouble() }
            .strongReference(true)
            .register(registry)
        indexers.forEach(::bindIndexer)
    }

    private fun bindIndexer(indexer: Indexer) {
        val tags = Tags.of(INDEXER_TAG, indexer.name)
        Gauge.builder("indexer.current.block", indexer) {
                if (it.getStatus() == Status.NOT_INITIALISED) Double.NaN
                else it.getCurrentBlockNumber().toDouble()
            }
            .tags(tags)
            .strongReference(true)
            .register(registry)
        Status.entries.forEach { status ->
            Gauge.builder("indexer.sync.status", indexer) {
                    if (it.getStatus() == status) 1.0 else 0.0
                }
                .tags(tags.and("status", status.name))
                .strongReference(true)
                .register(registry)
        }
        FunctionCounter.builder("indexer.blocks.processed", BlockProgress(indexer)) { it.total() }
            .tags(tags)
            .register(registry)
        listOf(catchUpGroup, proximityGroup).forEach { it[indexer.name] = AtomicInteger(0) }
        Gauge.builder("indexer.runner.catch.up.group", catchUpGroup.getValue(indexer.name)) {
                it.toDouble()
            }
            .tags(tags)
            .strongReference(true)
            .register(registry)
        Gauge.builder("indexer.runner.proximity.group", proximityGroup.getValue(indexer.name)) {
                it.toDouble()
            }
            .tags(tags)
            .strongReference(true)
            .register(registry)
    }

    /** Groups are numbered as the runner names them: 1 fast-sync pending, 2 ready, 3 blocked. */
    fun catchUpPass(
        fastSyncPending: List<Indexer>,
        syncReady: List<Indexer>,
        blocked: List<Indexer>
    ) {
        phase.set(Phase.CATCH_UP)
        regroups("catch_up").increment()
        catchUpGroup.values.forEach { it.set(0) }
        listOf(fastSyncPending, syncReady, blocked).forEachIndexed { i, group ->
            group.forEach { catchUpGroup[it.name]?.set(i + 1) }
        }
    }

    fun steadyState() {
        phase.set(Phase.STEADY)
        catchUpGroup.values.forEach { it.set(0) }
    }

    /** [groups] arrive lowest block first, so group 1 is always the furthest behind. */
    fun proximityGroups(groups: List<List<Indexer>>) {
        regroups("proximity").increment()
        proximityGroupCount.set(groups.size)
        proximityGroup.values.forEach { it.set(0) }
        groups.forEachIndexed { i, group -> group.forEach { proximityGroup[it.name]?.set(i + 1) } }
    }

    fun restart(reason: RestartReason) {
        Counter.builder("indexer.runner.restarts")
            .tag("reason", reason.name.lowercase())
            .register(registry)
            .increment()
    }

    fun blockTimer(indexer: Indexer): Timer =
        Timer.builder("indexer.block.process").tag(INDEXER_TAG, indexer.name).register(registry)

    fun blockFailure(indexer: Indexer) {
        Counter.builder("indexer.block.process.failures")
            .tag(INDEXER_TAG, indexer.name)
            .register(registry)
            .increment()
    }

    fun startSample(): Timer.Sample = Timer.start(registry)

    private fun regroups(kind: String): Counter =
        Counter.builder("indexer.runner.regroups").tag("kind", kind).register(registry)

    // Read-time, so a fast sync counts as it advances; see docs/Metrics.md.
    private class BlockProgress(private val indexer: Indexer) {
        private var last: Long? = null
        private var total = 0.0

        @Synchronized
        fun total(): Double {
            if (indexer.getStatus() !in PROCESSING_STATUSES) {
                last = null
                return total
            }
            val current = indexer.getCurrentBlockNumber()
            last?.let { if (current > it) total += current - it }
            last = current
            return total
        }
    }

    companion object {
        const val INDEXER_TAG = "indexer.name"
        private val PROCESSING_STATUSES =
            setOf(Status.SYNCING, Status.FAST_SYNCING, Status.FULLY_SYNCED)
    }
}
