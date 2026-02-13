package dev.qr.util

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * The idea is this is a many-to-many router.
 * So many requests go in and will be distributed on single threaded tasks evenly.
 */
class BalancerMutex<I, O>(
    val size: Int,
    val suppliers: BalancerSupplier<I, O>
) {

    private val semaphore = Semaphore(size)
    private val mutex: Mutex = Mutex()
    private val active = ConcurrentLinkedQueue<Int>((0..<size).toSet())

    // This can't handle more than <code>supplies.size</code> requests.
    // So semaphore should be a perfect fit.
    suspend fun eval(input: I): O = semaphore.withPermit {
        while (active.isEmpty()) delay(10L)

        var id: Int = -1

        while (mutex.withLock {
                if (active.isEmpty()) return@withLock true

                id = active.poll()

                false
            }) delay(10L)

        // Should never happen, but you never know
        require(id != -1)

        try {
            suppliers.supply(input, id)
        } finally {
            mutex.withLock {
                active.add(id)
            }
        }
    }

}

interface BalancerSupplier<I, O> {
    suspend fun supply(input: I, thread: Int): O
}