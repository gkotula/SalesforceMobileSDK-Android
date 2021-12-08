package com.salesforce.androidsdk.benchmarking

import android.content.Context
import android.util.Log
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

/**
 * Benchmark, which will execute on an Android device.
 *
 * The body of [BenchmarkRule.measureRepeated] is measured in a loop, and Studio will
 * output the result. Modify your code to see how it affects performance.
 */
@RunWith(AndroidJUnit4::class)
class ExampleBenchmark {
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

        createSoups(store)
    }

    @After
    fun tearDown() {
        store.dropAllSoups()
        dbOpenHelper.close()
        dbHelper.clearMemoryCache()
        DBOpenHelper.deleteDatabase(targetContext, null)
    }

    @Test
    fun log() {
        benchmarkRule.measureRepeated {
            Log.d("LogBenchmark", "the cost of writing this log method will be measured")
        }
    }

    @Test
    fun foo() {
        val startingEntries = (0..2).map { generateSoupEntry(forSoupName = SOUP_1_NAME) }
        startingEntries.forEach {
            store.upsert(SOUP_1_NAME, it)
        }

        benchmarkRule.measureRepeated {
            Log.d(
                "ExampleBenchmark",
                store.query(
                    QuerySpec.buildAllQuerySpec(
                        SOUP_1_NAME,
                        "_soupEntryId",
                        QuerySpec.Order.ascending,
                        10
                    ), 0
                ).toString()
            )
        }
    }

    private fun createSoups(store: SmartStore) {
//        val stableRandom = Random(STABLE_RANDOM_SEED)
//        val numSoups = stableRandom.nextInt(from = 100, until = 200)
//        for (soupNum in 0..numSoups) {
//            val numIndexes = stableRandom.nextInt(from = 10, until = 20)
//            val indexSpecs = (0..numIndexes).map {
//                IndexSpec(
//                    "path_$it",
//                    SmartStore.Type.values()[stableRandom.nextInt(from = 0, until = 4)]
//                )
//            }.toTypedArray()
//
//            store.registerSoup("soup_$soupNum", indexSpecs)
//        }
//
//        val objToUpsert = JSONObject()
//        store.getSoupIndexSpecs("soup_1").map {
//            it.toJSON()
//        }
//
//        TODO("This method needs to set things up to provide a way to later support CRUD on these randomly-generated soups.")
//
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

//        store.clearSoup(SOUP_1_NAME)
//        store.clearSoup(SOUP_2_NAME)
//        store.clearSoup(SOUP_3_NAME)
//        store.clearSoup(SOUP_4_NAME)

        store.registerSoup(SOUP_1_NAME, soup1Specs.toTypedArray())
        store.registerSoup(SOUP_2_NAME, soup2Specs.toTypedArray())
        store.registerSoup(SOUP_3_NAME, soup3Specs.toTypedArray())
        store.registerSoup(SOUP_4_NAME, soup4Specs.toTypedArray())
    }

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

        // https://stackoverflow.com/a/54400933/17313401 , CC BY-SA 4.0
        return (0 until characterCount)
            .map { ASCII_ALLOWED.random(this) }
            .joinToString("")
    }
}

private val ASCII_ALLOWED = ' '..'~' // ASCII 0x20..0x126

// seed chosen via random.org (range 1 - 1,000,000), a.k.a. as close to true randomness as possible, but repeatable
private const val STABLE_RANDOM_SEED = 502965L
private const val SOUP_1_NAME = "soup1"

private data class Soup1Entry(
    val string1: String,
    val string2: String,
    val integer1: Int,
    val floating1: Double,
    val string3: String,
    val fullText1: String,
    val floating2: Double,
    val integer2: Int,
    val fullText2: String,
    val integer3: Int,
    val floating3: Double
)

private const val SOUP_2_NAME = "soup2"

private data class Soup2Entry(
    val string1: String,
    val floating1: Double,
    val fullText1: String,
    val string2: String,
    val fullText2: String,
    val integer1: Int,
    val floating2: Double,
    val integer2: Int,
    val integer3: Int,
    val string3: String,
)

private const val SOUP_3_NAME = "soup3"

private data class Soup3Entry(
    val integer1: Int,
    val integer2: Int,
    val floating1: Double,
    val floating2: Double,
    val string1: String,
    val string2: String,
    val floating3: Double,
    val floating4: Double
)

private const val SOUP_4_NAME = "soup4"

private data class Soup4Entry(
    val integer1: Int,
    val string1: String,
    val string2: String,
    val fullText1: String,
    val fullText2: String,
    val string3: String,
    val string4: String,
    val string5: String,
    val string6: String
)
