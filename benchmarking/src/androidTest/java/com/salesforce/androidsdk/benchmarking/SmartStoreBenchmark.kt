package com.salesforce.androidsdk.benchmarking

import android.content.Context
import androidx.benchmark.junit4.BenchmarkRule
import androidx.benchmark.junit4.measureRepeated
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.salesforce.androidsdk.analytics.EventBuilderHelper
import com.salesforce.androidsdk.smartstore.app.SmartStoreSDKManager
import com.salesforce.androidsdk.smartstore.store.*
import org.json.JSONObject
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.lang.IllegalStateException
import kotlin.random.Random

@RunWith(AndroidJUnit4::class)
class SmartStoreBenchmark {
    @get:Rule
    val benchmarkRule = BenchmarkRule()

    private lateinit var targetContext: Context
    private lateinit var store: SmartStore
    private lateinit var dbOpenHelper: DBOpenHelper
    private lateinit var dbHelper: DBHelper

    private val stableRandom = Random(STABLE_RANDOM_SEED)

    @Before
    fun setup() {
        EventBuilderHelper.enableDisable(false)

        targetContext = InstrumentationRegistry.getInstrumentation().targetContext
        val dbPath = targetContext.applicationInfo.dataDir + "/databases"
        val fileDir = File(dbPath)

        SmartStoreSDKManager.initNative(targetContext.applicationContext, null)

        DBOpenHelper.deleteAllUserDatabases(targetContext)
        DBOpenHelper.deleteDatabase(targetContext, null)
        DBOpenHelper.removeAllFiles(fileDir)

        dbOpenHelper = DBOpenHelper.getOpenHelper(targetContext, null)
        dbHelper = DBHelper.getInstance(dbOpenHelper.getWritableDatabase(""))
        store = SmartStore(dbOpenHelper, "")

        store.createTestSoups()
    }

    @After
    fun tearDown() {
        store.dropAllSoups()
        dbOpenHelper.close()
        dbHelper.clearMemoryCache()
        DBOpenHelper.deleteDatabase(targetContext, null)
    }

    @Test
    fun benchmarkUpsertingSoupEntries() {
        var soup1Entries: List<JSONObject> = emptyList()
        var soup2Entries: List<JSONObject> = emptyList()
        var soup3Entries: List<JSONObject> = emptyList()
        var soup4Entries: List<JSONObject> = emptyList()

        benchmarkRule.measureRepeated {
            runWithTimingDisabled {
                soup1Entries = (0 until 50).map { generateSoupEntry(SOUP_1_NAME) }
                soup2Entries = (0 until 50).map { generateSoupEntry(SOUP_2_NAME) }
                soup3Entries = (0 until 50).map { generateSoupEntry(SOUP_3_NAME) }
                soup4Entries = (0 until 50).map { generateSoupEntry(SOUP_4_NAME) }
            }

            soup1Entries.forEach { store.upsert(SOUP_1_NAME, it) }
            soup2Entries.forEach { store.upsert(SOUP_2_NAME, it) }
            soup3Entries.forEach { store.upsert(SOUP_3_NAME, it) }
            soup4Entries.forEach { store.upsert(SOUP_4_NAME, it) }

            runWithTimingDisabled {
                store.clearSoup(SOUP_1_NAME)
                store.clearSoup(SOUP_2_NAME)
                store.clearSoup(SOUP_3_NAME)
                store.clearSoup(SOUP_4_NAME)
            }
        }
    }

    private fun SmartStore.createTestSoups() {
        val soup1Specs = listOf(
            IndexSpec("string1", SmartStore.Type.string),
            IndexSpec("string2", SmartStore.Type.string),
            IndexSpec("integer1", SmartStore.Type.integer),
            IndexSpec("floating1", SmartStore.Type.floating),
            IndexSpec("string3", SmartStore.Type.string),
            IndexSpec("full_text1", SmartStore.Type.full_text),
            IndexSpec("floating2", SmartStore.Type.floating),
            IndexSpec("integer2", SmartStore.Type.integer),
            IndexSpec("full_text2", SmartStore.Type.full_text),
            IndexSpec("integer3", SmartStore.Type.integer),
            IndexSpec("floating3", SmartStore.Type.floating),
        )

        val soup2Specs = listOf(
            IndexSpec("string1", SmartStore.Type.string),
            IndexSpec("floating1", SmartStore.Type.floating),
            IndexSpec("full_text1", SmartStore.Type.full_text),
            IndexSpec("string2", SmartStore.Type.string),
            IndexSpec("full_text2", SmartStore.Type.full_text),
            IndexSpec("integer1", SmartStore.Type.integer),
            IndexSpec("floating2", SmartStore.Type.floating),
            IndexSpec("integer2", SmartStore.Type.integer),
            IndexSpec("integer3", SmartStore.Type.integer),
            IndexSpec("string3", SmartStore.Type.string),
        )

        val soup3Specs = listOf(
            IndexSpec("integer1", SmartStore.Type.integer),
            IndexSpec("integer2", SmartStore.Type.integer),
            IndexSpec("floating1", SmartStore.Type.floating),
            IndexSpec("floating2", SmartStore.Type.floating),
            IndexSpec("string1", SmartStore.Type.string),
            IndexSpec("string2", SmartStore.Type.string),
            IndexSpec("floating3", SmartStore.Type.floating),
            IndexSpec("floating4", SmartStore.Type.floating),
        )

        val soup4Specs = listOf(
            IndexSpec("integer1", SmartStore.Type.integer),
            IndexSpec("string1", SmartStore.Type.string),
            IndexSpec("string2", SmartStore.Type.string),
            IndexSpec("full_text1", SmartStore.Type.full_text),
            IndexSpec("full_text2", SmartStore.Type.full_text),
            IndexSpec("string3", SmartStore.Type.string),
            IndexSpec("string4", SmartStore.Type.string),
            IndexSpec("string5", SmartStore.Type.string),
            IndexSpec("string6", SmartStore.Type.string)
        )

        registerSoup(SOUP_1_NAME, soup1Specs.toTypedArray())
        registerSoup(SOUP_2_NAME, soup2Specs.toTypedArray())
        registerSoup(SOUP_3_NAME, soup3Specs.toTypedArray())
        registerSoup(SOUP_4_NAME, soup4Specs.toTypedArray())
    }

    /**
     * Avoid calling this from multiple threads/execution contexts to preserve stable random generation
     */
    private fun generateSoupEntry(forSoupName: String): JSONObject {
        val result = JSONObject()

        store.getSoupIndexSpecs(forSoupName).forEach { spec ->
            when (spec.type!!) {
                SmartStore.Type.full_text,
                SmartStore.Type.string -> result.put(
                    spec.path,
                    stableRandom.generateString(minLength = 1u, maxLength = 10u)
                )

                SmartStore.Type.integer -> result.put(spec.path, stableRandom.nextInt())
                SmartStore.Type.floating -> result.put(spec.path, stableRandom.nextFloat())
                SmartStore.Type.json1 -> throw IllegalStateException("JSON1 type not supported in this test")
            }
        }

        return result
    }

    private fun Random.generateString(minLength: UInt = 50u, maxLength: UInt = 400u): String {
        val safeMaxLength = maxLength.coerceIn(0u until Int.MAX_VALUE.toUInt()).toInt()
        val characterCount = nextInt(from = minLength.toInt(), until = safeMaxLength)

        // Following from: https://stackoverflow.com/a/54400933/17313401 CC BY-SA 4.0
        return (0 until characterCount)
            .map { ASCII_ALLOWED.random(this) }
            .joinToString("")
    }

    companion object {
        private val ASCII_ALLOWED = ' '..'~' // ASCII 0x20..0x126

        // seed chosen via random.org (range 1 - 1,000,000), a.k.a. as close to true randomness as possible, but repeatable
        private const val STABLE_RANDOM_SEED = 502965L

        private const val SOUP_1_NAME = "soup1"
        private const val SOUP_2_NAME = "soup2"
        private const val SOUP_3_NAME = "soup3"
        private const val SOUP_4_NAME = "soup4"
    }
}
