package com.salesforce.androidsdk.mobilesync

import com.salesforce.androidsdk.mobilesync.manager.SyncManager
import com.salesforce.androidsdk.mobilesync.util.SyncState
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

@Throws(Exception::class)
suspend fun SyncManager.suspendCleanResyncGhosts(syncName: String): Int = suspendCoroutine { cont ->
    val callback = object : SyncManager.CleanResyncGhostsCallback {
        override fun onSuccess(numRecords: Int) {
            cont.resume(numRecords)
        }

        override fun onError(e: Exception?) {
            cont.resumeWithException(e ?: RuntimeException("Unknown error occurred cleaning resync ghosts for sync name $syncName"))
        }
    }

    cleanResyncGhosts(syncName, callback)
}

/**
 * What runs within the Flow builder does not need special Main-safety handling,
 * and the context the callback is run with is determined by the scheduling in
 * SyncManager.
 */
fun SyncManager.reSyncFlow(syncName: String): Flow<SyncState> = callbackFlow {
    val callback: (SyncState) -> Unit = {
        when (it.status) {
            SyncState.Status.DONE,
            SyncState.Status.STOPPED -> {
                trySendBlocking(it)
                channel.close()
            }

            // TODO need API decision about what states throw Exceptions and how to capture them here
            SyncState.Status.FAILED -> {
                channel.close(cause = java.lang.Exception(it.error))
            }

            SyncState.Status.NEW,
            SyncState.Status.RUNNING,
            null -> {
                trySendBlocking(it)
            }
        }
    }

    // Start sync and emit initial state. Exceptions thrown in this ProducerScope will propagate normally.
    trySendBlocking(reSync(syncName, callback))

    awaitClose() // Individual syncs cannot be cancelled, so no cleanup necessary
}
