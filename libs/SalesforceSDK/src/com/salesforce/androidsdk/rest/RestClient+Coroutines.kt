package com.salesforce.androidsdk.rest

import kotlinx.coroutines.suspendCancellableCoroutine
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Main safety of this method depends on implementation of [sendAsync], which in turn depends on
 * the scheduling in the HTTP client implementation.  It is almost assuredly safe in its current
 * state here.
 */
@Throws(Exception::class)
suspend fun RestClient.suspendSend(request: RestRequest): RestResponse? = suspendCancellableCoroutine { cont ->
    val callback = object : RestClient.AsyncRequestCallback {
        override fun onSuccess(request: RestRequest?, response: RestResponse?) {
            // TODO test case when coroutine is cancelled while consuming the response here.  I.e. does the response always get consumed if the coroutine is cancelled?
            try {
                response?.consume()
                cont.resume(response)
            } catch (ex: IOException) {
                cont.resumeWithException(ex)
            }
        }

        override fun onError(ex: Exception) {
            cont.resumeWithException(ex)
        }
    }

    val call = sendAsync(request, callback)

    cont.invokeOnCancellation {
        call.cancel()
    }
}
