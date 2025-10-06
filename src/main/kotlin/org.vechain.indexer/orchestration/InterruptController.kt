package org.vechain.indexer.orchestration

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class InterruptController(onRequest: ((InterruptReason) -> Unit)? = null) {
    private val requested = AtomicReference<InterruptReason?>(null)
    private val listeners = CopyOnWriteArrayList<Channel<InterruptReason>>()
    private val callback = onRequest

    fun registerListener(): ReceiveChannel<InterruptReason> {
        val channel = Channel<InterruptReason>(capacity = Channel.CONFLATED)
        listeners.add(channel)
        requested.get()?.let { channel.trySend(it) }
        channel.invokeOnClose { listeners.remove(channel) }
        return channel
    }

    fun request(reason: InterruptReason) {
        var reasonToNotify: InterruptReason? = null
        while (true) {
            val current = requested.get()
            if (current == InterruptReason.Shutdown) {
                return
            }
            if (current == reason) {
                return
            }

            val updated =
                when (reason) {
                    InterruptReason.Error -> current ?: InterruptReason.Error
                    InterruptReason.Shutdown -> InterruptReason.Shutdown
                }

            if (requested.compareAndSet(current, updated)) {
                if (updated != current) {
                    reasonToNotify = updated
                }
                break
            }
        }

        reasonToNotify?.let {
            callback?.invoke(it)
            notifyListeners(it)
        }
    }

    fun clear(reason: InterruptReason) {
        requested.compareAndSet(reason, null)
    }

    fun isRequested(): Boolean = requested.get() != null

    fun currentReason(): InterruptReason? = requested.get()

    private fun notifyListeners(reason: InterruptReason) {
        listeners.forEach { it.trySend(reason) }
    }
}

enum class InterruptReason {
    Error,
    Shutdown,
}
