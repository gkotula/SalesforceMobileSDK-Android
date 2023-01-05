package com.salesforce.androidsdk.rest

import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * TODO add actual javadoc comment
 *
 * Main-safety of this method depends on implementation of [RestClient.sendAsync], which in turn
 * depends on the scheduling in the HTTP client implementation.  It is almost assuredly safe in its
 * current state here.
 */
@Throws(Exception::class)
suspend fun RestClient.sendSuspend(request: RestRequest): RestResponse? = suspendCancellableCoroutine { cont ->
    val responseToConsumeInCornerCase = AtomicReference<RestResponse?>(null)

    val callback = object : RestClient.AsyncRequestCallback {
        override fun onSuccess(request: RestRequest?, response: RestResponse?) {
            if (cont.isCancelled) {
                response?.consumeQuietly() // must consume successful response if the parent coroutine is cancelled
            }
            responseToConsumeInCornerCase.set(response)
            cont.resume(response)
        }

        override fun onError(ex: Exception) {
            cont.resumeWithException(ex)
        }
    }

    val call = sendAsync(request, callback)

    cont.invokeOnCancellation {
        call.cancel()

        /* This covers a very rare corner-case where the continuation has received the successful
         * response and is awaiting dispatch back to the parent coroutine context, but the parent
         * coroutine is concurrently cancelled before dispatch. In that case, we cannot depend on
         * the parent coroutine to consume the successful response, but the response must be
         * consumed by something; hence, we do it here. */
        responseToConsumeInCornerCase.getAndSet(null)?.consumeQuietly()
    }
}
