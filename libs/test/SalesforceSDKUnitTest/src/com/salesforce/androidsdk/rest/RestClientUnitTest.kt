package com.salesforce.androidsdk.rest

import android.widget.TextView
import com.salesforce.androidsdk.auth.HttpAccess
import com.salesforce.androidsdk.rest.RestClient.UnauthenticatedClientInfo
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail
import java.io.IOException
import java.net.HttpURLConnection.HTTP_INTERNAL_ERROR
import java.net.HttpURLConnection.HTTP_OK

@Suppress("ObjectLiteralToLambda")
class RestClientUnitTest {
    private lateinit var restClient: RestClient
    private val mockClientInfo = UnauthenticatedClientInfo()
    private val authToken = "authToken"

    @AfterEach
    fun cleanup() {
        RestClient.clearCaches()
    }

    @Test
    fun `if response is success then coroutine succeeds`(): Unit = runBlocking {
        setRestClientWithInterceptor { chain ->
            Thread.sleep(100) // simulate blocking call
            Response.Builder()
                .request(chain.request())
                .protocol(Protocol.HTTP_2)
                .message(RESPONSE_MESSAGE_SUCCESS)
                .body(RESPONSE_BODY_SUCCESS.toResponseBody(TEXT_PLAIN))
                .code(HTTP_OK)
                .build()
        }

        val response = restClient.sendSuspend(simpleRequest)

        assertEquals(RESPONSE_BODY_SUCCESS, response!!.asString())
    }

    @Test
    fun `if response is error then coroutine succeeds`(): Unit = runBlocking {
        setRestClientWithInterceptor { chain ->
            Thread.sleep(100) // simulate blocking call
            Response.Builder()
                .request(chain.request())
                .protocol(Protocol.HTTP_2)
                .message(RESPONSE_MESSAGE_ERROR)
                .body(RESPONSE_BODY_ERROR.toResponseBody(TEXT_PLAIN))
                .code(HTTP_INTERNAL_ERROR)
                .build()
        }

        val response = restClient.sendSuspend(simpleRequest)

        assertEquals(RESPONSE_BODY_ERROR, response!!.asString())
    }

    @Test
    fun `if call throws then coroutine throws same exception`(): Unit = runBlocking {
        val expectedExceptionMsg = "expectedExceptionMessage"

        setRestClientWithInterceptor { Thread.sleep(100); throw IOException(expectedExceptionMsg) }

        // supervisorScope prevents runBlocking from being marked as failed from the IOException
        // while still allowing assertions to fail the test.
        supervisorScope {
            val response1Async = async { restClient.sendSuspend(simpleRequest) }
            val response2Async = async { restClient.sendSuspend(simpleRequest) }

            val exception1 = assertThrows<IOException> { response1Async.await() }
            val exception2 = assertThrows<IOException> { response2Async.await() }

            assertEquals(expectedExceptionMsg, exception1.message)
            assertEquals(expectedExceptionMsg, exception2.message)
        }
    }

    @Test
    fun `if coroutine is cancelled then call is cancelled`(): Unit = runBlocking {
        val callContinueSignal = Mutex(locked = true)
        val failureSignal = Mutex(locked = true)
        val msToWaitForCancellation = 1_000L

        setRestClientWithInterceptor { chain ->
            callContinueSignal.unlock()

            Thread.sleep(msToWaitForCancellation)

            if (!chain.call().isCanceled()) {
                failureSignal.unlock()
            }

            Response.Builder()
                .request(chain.request())
                .protocol(Protocol.HTTP_2)
                .message(RESPONSE_MESSAGE_SUCCESS)
                .body(RESPONSE_BODY_SUCCESS.toResponseBody(TEXT_PLAIN))
                .code(HTTP_OK)
                .build()
        }

        val job = launch {
            restClient.sendSuspend(simpleRequest)
            if (isActive) {
                fail { "Expected coroutine to be cancelled before suspending request finished." }
            }
        }

        /* This wait time should be tuned to be long enough for any hardware to schedule/run the
         * OkHttp call, even on resource-constrained virtualized operating systems used in CI/CD. */
        val msToWaitForCallContinue = 5_000L

        try {
            withTimeout(msToWaitForCallContinue) { callContinueSignal.lock() }
        } catch (_: TimeoutCancellationException) {
            fail { "Call not started within timeout of ${msToWaitForCallContinue}ms. This may indicate that the test run environment is too resource-constrained to schedule background tasks in a timely manner. Changing this timeout value may fix the issue." }
        }

        job.cancel()

        try {
            withTimeout(msToWaitForCancellation) {
                failureSignal.lock()
                fail { "Expected coroutine cancellation to have cancelled the OkHttp call." }
            }
        } catch (_: TimeoutCancellationException) {
            // success
        }
    }

    private fun setRestClientWithInterceptor(interceptor: (Interceptor.Chain) -> Response) {
        val httpAccess = object : HttpAccess(null, "user-agent") {
            override fun getOkHttpClientBuilder(): OkHttpClient.Builder = OkHttpClient.Builder()
                .addInterceptor(interceptor)
        }
        restClient = RestClient(mockClientInfo, authToken, httpAccess, null)
    }

    private companion object {
        private const val RESPONSE_BODY_SUCCESS = "success"
        private const val RESPONSE_BODY_ERROR = "error"
        private const val RESPONSE_MESSAGE_SUCCESS = "200 OK"
        private const val RESPONSE_MESSAGE_ERROR = "500 Internal Server Error"
        private val TEXT_PLAIN = "text/plain".toMediaType()
        private val simpleRequest = RestRequest(RestRequest.RestMethod.GET, "http://foo.com/path")
    }
}
