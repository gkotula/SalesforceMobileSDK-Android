/*
 * Copyright (c) 2012-present, salesforce.com, inc.
 * All rights reserved.
 * Redistribution and use of this software in source and binary forms, with or
 * without modification, are permitted provided that the following conditions
 * are met:
 * - Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * - Neither the name of salesforce.com, inc. nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission of salesforce.com, inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.androidsdk.smartstore.store

import com.salesforce.androidsdk.smartstore.store.SmartStore.FtsExtension
import com.salesforce.androidsdk.smartstore.store.DBHelper
import org.json.JSONObject
import com.salesforce.androidsdk.smartstore.store.DBOpenHelper
import com.salesforce.androidsdk.smartstore.store.IndexSpec
import com.salesforce.androidsdk.smartstore.store.SoupSpec
import com.salesforce.androidsdk.smartstore.store.SmartStore.SmartStoreException
import android.content.ContentValues
import android.database.Cursor
import com.salesforce.androidsdk.smartstore.store.SmartStore
import com.salesforce.androidsdk.app.SalesforceSDKManager
import org.json.JSONArray
import org.json.JSONException
import com.salesforce.androidsdk.smartstore.util.SmartStoreLogger
import com.salesforce.androidsdk.analytics.EventBuilderHelper
import com.salesforce.androidsdk.smartstore.store.SmartStore.TypeGroup
import android.text.TextUtils
import com.salesforce.androidsdk.smartstore.store.LongOperation
import com.salesforce.androidsdk.smartstore.store.LongOperation.LongOperationType
import kotlin.Throws
import com.salesforce.androidsdk.smartstore.store.AlterSoupLongOperation
import com.salesforce.androidsdk.smartstore.store.QuerySpec
import com.salesforce.androidsdk.smartstore.store.SmartSqlHelper
import net.sqlcipher.database.SQLiteDatabase
import net.sqlcipher.database.SQLiteOpenHelper
import java.io.File
import java.lang.Exception
import java.lang.RuntimeException
import java.lang.StringBuilder
import java.util.ArrayList
import java.util.concurrent.Executors
import kotlin.jvm.Synchronized

/**
 * Smart store
 *
 * Provides a secure means for SalesforceMobileSDK Container-based applications to store objects in a persistent
 * and searchable manner. Similar in some ways to CouchDB, SmartStore stores documents as JSON values.
 * SmartStore is inspired by the Apple Newton OS Soup/Store model.
 * The main challenge here is how to effectively store documents with dynamic fields, and still allow indexing and searching.
 */
class SmartStore(val dbOpenHelper: SQLiteOpenHelper, val encryptionKey: String?) {
    // Backing database
    val database: SQLiteDatabase = dbOpenHelper.getWritableDatabase(encryptionKey)
    /**
     * @return ftsX to be used when creating the virtual table to support full_text queries
     */
    /**
     * Sets the ftsX to be used when creating the virtual table to support full_text queries
     * NB: only used in tests
     * @param ftsExtension
     */
    // FTS extension to use
    var ftsExtension = FtsExtension.fts5

    // background executor
    private val threadPool = Executors.newFixedThreadPool(1)

    /**
     * If turned on, explain query plan is run before executing a query and stored in lastExplainQueryPlan
     * and also get logged
     * @param captureExplainQueryPlan true to turn capture on and false to turn off
     */
    fun setCaptureExplainQueryPlan(captureExplainQueryPlan: Boolean) {
        DBHelper.getInstance(database).setCaptureExplainQueryPlan(captureExplainQueryPlan)
    }

    /**
     * @return explain query plan for last query run (if captureExplainQueryPlan is true)
     */
    val lastExplainQueryPlan: JSONObject
        get() = DBHelper.getInstance(database).lastExplainQueryPlan// XXX That cast will be trouble if the file is more than 2GB

    /**
     * Get database size
     */
    val databaseSize: Int
        get() {
            var size = File(database.path).length()
                .toInt() // XXX That cast will be trouble if the file is more than 2GB
            if (dbOpenHelper is DBOpenHelper) {
                size += (dbOpenHelper as DBOpenHelper).getSizeOfDir(null)
            }
            return size
        }

    /**
     * Start transaction
     * NB: to avoid deadlock, caller should have synchronized(store.getDatabase()) around the whole transaction
     */
    fun beginTransaction() {
        database.beginTransaction()
    }

    /**
     * End transaction (commit or rollback)
     */
    fun endTransaction() {
        database.endTransaction()
    }

    /**
     * Mark transaction as successful (next call to endTransaction will be a commit)
     */
    fun setTransactionSuccessful() {
        database.setTransactionSuccessful()
    }

    /**
     * Register a soup without any features. Use [.registerSoupWithSpec] to enable features such as external storage, etc.
     *
     * Create table for soupName with a column for the soup itself and columns for paths specified in indexSpecs
     * Create indexes on the new table to make lookup faster
     * Create rows in soup index map table for indexSpecs
     * @param soupName
     * @param indexSpecs
     */
    fun registerSoup(soupName: String?, indexSpecs: Array<IndexSpec>) {
        registerSoupWithSpec(SoupSpec(soupName), indexSpecs)
    }

    /**
     * Register a soup using the given soup specifications. This allows the soup to use extra features such as external storage.
     *
     * Create table for soupName with a column for the soup itself and columns for paths specified in indexSpecs
     * Create indexes on the new table to make lookup faster
     * Create rows in soup index map table for indexSpecs
     * @param soupSpec
     * @param indexSpecs
     */
    @Deprecated(
        "We are removing external storage and soup spec in 11.0",
        replaceWith = "registerSoup(soupName: String?, indexSpecs: Array<IndexSpec>)"
    )
    fun registerSoupWithSpec(soupSpec: SoupSpec, indexSpecs: Array<IndexSpec>) {
        val db = database
        synchronized(db) {
            val soupName = soupSpec.soupName
            if (soupName == null) throw SmartStoreException("Bogus soup name:$soupName")
            if (indexSpecs.size == 0) throw SmartStoreException("No indexSpecs specified for soup: $soupName")
            if (IndexSpec.hasJSON1(indexSpecs) && soupSpec.features.contains(SoupSpec.FEATURE_EXTERNAL_STORAGE)) throw SmartStoreException(
                "Can't have JSON1 index specs in externally stored soup:$soupName"
            )
            if (hasSoup(soupName)) return  // soup already exist - do nothing

            // First get a table name
            var soupTableName: String? = null
            val soupMapValues = ContentValues()
            soupMapValues.put(SOUP_NAME_COL, soupName)

            // Register features from soup spec
            for (feature: String? in soupSpec.features) {
                soupMapValues.put(feature, 1)
            }
            try {
                db.beginTransaction()
                val soupId = DBHelper.getInstance(db).insert(db, SOUP_ATTRS_TABLE, soupMapValues)
                soupTableName = getSoupTableName(soupId)

                // Do the rest - create table / indexes
                registerSoupUsingTableName(soupSpec, indexSpecs, soupTableName)
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
            if (SalesforceSDKManager.getInstance().isTestRun) {
                logRegisterSoupEvent(soupSpec, indexSpecs)
            } else {
                threadPool.execute(Runnable { logRegisterSoupEvent(soupSpec, indexSpecs) })
            }
        }
    }

    /**
     * Log the soup event.
     * @param soupSpec
     * @param indexSpecs
     */
    private fun logRegisterSoupEvent(soupSpec: SoupSpec, indexSpecs: Array<IndexSpec>) {
        val features = JSONArray()
        if (IndexSpec.hasJSON1(indexSpecs)) {
            features.put("JSON1")
        }
        if (IndexSpec.hasFTS(indexSpecs)) {
            features.put("FTS")
        }
        if (soupSpec.features.contains(SoupSpec.FEATURE_EXTERNAL_STORAGE)) {
            features.put("ExternalStorage")
        }
        val attributes = JSONObject()
        try {
            attributes.put("features", features)
        } catch (e: JSONException) {
            SmartStoreLogger.e(TAG, "Exception thrown while building page object", e)
        }
        EventBuilderHelper.createAndStoreEventSync("registerSoup", null, TAG, attributes)
    }

    /**
     * Helper method for registerSoup using soup spec
     *
     * @param soupSpec
     * @param indexSpecs
     * @param soupTableName
     */
    fun registerSoupUsingTableName(
        soupSpec: SoupSpec,
        indexSpecs: Array<IndexSpec>,
        soupTableName: String?
    ) {
        // Prepare SQL for creating soup table and its indices
        val createTableStmt = StringBuilder() // to create new soup table
        val createFtsStmt = StringBuilder() // to create fts table
        val createIndexStmts: MutableList<String> =
            ArrayList() // to create indices on new soup table
        val soupIndexMapInserts: MutableList<ContentValues> =
            ArrayList() // to be inserted in soup index map table
        val indexSpecsToCache = arrayOfNulls<IndexSpec>(indexSpecs.size)
        val columnsForFts: MutableList<String?> = ArrayList()
        val soupName = soupSpec.soupName
        createTableStmt.append("CREATE TABLE ").append(soupTableName).append(" (")
            .append(ID_COL).append(" INTEGER PRIMARY KEY AUTOINCREMENT")
        if (!usesExternalStorage(soupName)) {
            // If external storage is used, do not add column for soup in the db since it will be empty.
            createTableStmt.append(", ").append(SOUP_COL).append(" TEXT")
        }
        createTableStmt.append(", ").append(CREATED_COL).append(" INTEGER")
            .append(", ").append(LAST_MODIFIED_COL).append(" INTEGER")
        val createIndexFormat = "CREATE INDEX %s_%s_idx on %s ( %s )"
        for (col in arrayOf(CREATED_COL, LAST_MODIFIED_COL)) {
            createIndexStmts.add(
                String.format(
                    createIndexFormat,
                    soupTableName,
                    col,
                    soupTableName,
                    col
                )
            )
        }
        var i = 0
        for (indexSpec in indexSpecs) {
            // Column name or expression the db index is on
            var columnName = soupTableName + "_" + i
            if (TypeGroup.value_indexed_with_json_extract.isMember(indexSpec.type)) {
                columnName = "json_extract(" + SOUP_COL + ", '$." + indexSpec.path + "')"
            }

            // for create table
            if (TypeGroup.value_extracted_to_column.isMember(indexSpec.type)) {
                val columnType = indexSpec.type.columnType
                createTableStmt.append(", ").append(columnName).append(" ").append(columnType)
            }

            // for fts
            if (indexSpec.type == Type.full_text) {
                columnsForFts.add(columnName)
            }

            // for insert
            val values = ContentValues()
            values.put(SOUP_NAME_COL, soupName)
            values.put(PATH_COL, indexSpec.path)
            values.put(COLUMN_NAME_COL, columnName)
            values.put(COLUMN_TYPE_COL, indexSpec.type.toString())
            soupIndexMapInserts.add(values)

            // for create index
            createIndexStmts.add(
                String.format(
                    createIndexFormat,
                    soupTableName,
                    "" + i,
                    soupTableName,
                    columnName
                )
            )

            // for the cache
            indexSpecsToCache[i] = IndexSpec(indexSpec.path, indexSpec.type, columnName)
            i++
        }
        createTableStmt.append(")")

        // fts
        if (columnsForFts.size > 0) {
            createFtsStmt.append(
                String.format(
                    "CREATE VIRTUAL TABLE %s%s USING %s(%s)",
                    soupTableName,
                    FTS_SUFFIX,
                    ftsExtension,
                    TextUtils.join(",", columnsForFts)
                )
            )
        }

        // Run SQL for creating soup table and its indices
        val db = database
        db.execSQL(createTableStmt.toString())
        if (columnsForFts.size > 0) {
            db.execSQL(createFtsStmt.toString())
        }
        for (createIndexStmt in createIndexStmts) {
            db.execSQL(createIndexStmt)
        }
        try {
            db.beginTransaction()
            for (values in soupIndexMapInserts) {
                DBHelper.getInstance(db).insert(db, SOUP_INDEX_MAP_TABLE, values)
            }
            if (usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                (dbOpenHelper as DBOpenHelper).createExternalBlobsDirectory(soupTableName)
            }
            db.setTransactionSuccessful()

            // Add to soupNameToTableNamesMap
            DBHelper.getInstance(db).cacheTableName(soupName, soupTableName)

            // Add to soupNameToIndexSpecsMap
            DBHelper.getInstance(db).cacheIndexSpecs(soupName, indexSpecsToCache)
        } finally {
            db.endTransaction()
        }
    }

    /**
     * Finish long operations that were interrupted
     */
    fun resumeLongOperations() {
        val db = database
        synchronized(db) {
            for (longOperation: LongOperation in longOperations) {
                try {
                    longOperation.run()
                } catch (e: Exception) {
                    SmartStoreLogger.e(TAG, "Unexpected error", e)
                }
            }
        }
    }

    /**
     * @return unfinished long operations
     */
    val longOperations: Array<LongOperation>
        get() {
            val db = database
            val longOperations: MutableList<LongOperation> = ArrayList()
            synchronized(db) {
                var cursor: Cursor? = null
                try {
                    cursor = DBHelper.getInstance(db).query(
                        db,
                        LONG_OPERATIONS_STATUS_TABLE,
                        arrayOf(ID_COL, TYPE_COL, DETAILS_COL, STATUS_COL),
                        null,
                        null,
                        null
                    )
                    if (cursor.moveToFirst()) {
                        do {
                            try {
                                val rowId = cursor.getLong(0)
                                val operationType = LongOperationType.valueOf(cursor.getString(1))
                                val details = JSONObject(cursor.getString(2))
                                val statusStr = cursor.getString(3)
                                longOperations.add(
                                    operationType.getOperation(
                                        this,
                                        rowId,
                                        details,
                                        statusStr
                                    )
                                )
                            } catch (e: Exception) {
                                SmartStoreLogger.e(TAG, "Unexpected error", e)
                            }
                        } while (cursor.moveToNext())
                    }
                } finally {
                    safeClose(cursor)
                }
            }
            return longOperations.toTypedArray()
        }

    /**
     * Alter soup using only soup name without extra soup features.
     *
     * @param soupName
     * @param indexSpecs array of index specs
     * @param reIndexData
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun alterSoup(
        soupName: String?, indexSpecs: Array<IndexSpec?>?,
        reIndexData: Boolean
    ) {
        alterSoup(soupName, SoupSpec(soupName, *arrayOfNulls(0)), indexSpecs, reIndexData)
    }

    /**
     * Alter soup with new soup spec.
     *
     * @param soupName name of soup to alter
     * @param soupSpec
     * @param indexSpecs array of index specs
     * @param reIndexData
     * @throws JSONException
     */
    @Deprecated(
        message = "We are removing external storage and soup spec in 11.0",
        replaceWith = "alterSoup(soupName: String?, indexSpecs: Array<IndexSpec>, reIndexData: Boolean)"
    )
    @Throws(JSONException::class)
    fun alterSoup(
        soupName: String?, soupSpec: SoupSpec?, indexSpecs: Array<IndexSpec?>?,
        reIndexData: Boolean
    ) {
        val operation = AlterSoupLongOperation(this, soupName, soupSpec, indexSpecs, reIndexData)
        operation.run()
    }

    /**
     * Re-index all soup elements for passed indexPaths
     * NB: only indexPath that have IndexSpec on them will be indexed
     *
     * @param soupName
     * @param indexPaths
     * @param handleTx
     */
    fun reIndexSoup(soupName: String, indexPaths: Array<String>, handleTx: Boolean) {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")

            // Getting index specs from indexPaths skipping json1 index specs
            val mapAllSpecs = IndexSpec.mapForIndexSpecs(getSoupIndexSpecs(soupName))
            val indexSpecsList: MutableList<IndexSpec> = ArrayList()
            for (indexPath: String in indexPaths) {
                if (mapAllSpecs.containsKey(indexPath)) {
                    val indexSpec = mapAllSpecs[indexPath]
                    if (TypeGroup.value_extracted_to_column.isMember(indexSpec!!.type)) {
                        indexSpecsList.add(indexSpec)
                    }
                } else {
                    SmartStoreLogger.w(
                        TAG,
                        "Can not re-index $indexPath - it does not have an index"
                    )
                }
            }
            val indexSpecs: Array<IndexSpec> = indexSpecsList.toTypedArray()
            if (indexSpecs.size == 0) {
                // Nothing to do
                return
            }
            val hasFts = IndexSpec.hasFTS(indexSpecs)
            if (handleTx) {
                db.beginTransaction()
            }
            var cursor: Cursor? = null
            try {
                val projection: Array<String>
                if (usesExternalStorage(soupName)) {
                    projection = arrayOf(ID_COL)
                } else {
                    projection = arrayOf(ID_COL, SOUP_COL)
                }
                cursor =
                    DBHelper.getInstance(db).query(db, soupTableName, projection, null, null, null)
                if (cursor.moveToFirst()) {
                    do {
                        val soupEntryId = cursor.getString(0)
                        try {
                            var soupElt: JSONObject
                            if (usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                                soupElt = (dbOpenHelper as DBOpenHelper).loadSoupBlob(
                                    soupTableName,
                                    soupEntryId.toLong(),
                                    encryptionKey
                                )
                            } else {
                                val soupRaw = cursor.getString(1)
                                soupElt = JSONObject(soupRaw)
                            }
                            val contentValues = ContentValues()
                            projectIndexedPaths(
                                soupElt,
                                contentValues,
                                indexSpecs,
                                TypeGroup.value_extracted_to_column
                            )
                            DBHelper.getInstance(db).update(
                                db,
                                soupTableName,
                                contentValues,
                                ID_PREDICATE,
                                soupEntryId + ""
                            )

                            // Fts
                            if (hasFts) {
                                val soupTableNameFts = soupTableName + FTS_SUFFIX
                                val contentValuesFts = ContentValues()
                                projectIndexedPaths(
                                    soupElt,
                                    contentValuesFts,
                                    indexSpecs,
                                    TypeGroup.value_extracted_to_fts_column
                                )
                                DBHelper.getInstance(db).update(
                                    db,
                                    soupTableNameFts,
                                    contentValuesFts,
                                    ROWID_PREDICATE,
                                    soupEntryId + ""
                                )
                            }
                        } catch (e: JSONException) {
                            SmartStoreLogger.w(TAG, "Could not parse soup element $soupEntryId", e)
                            // Should not have happen - just keep going
                        }
                    } while (cursor.moveToNext())
                }
            } finally {
                if (handleTx) {
                    db.setTransactionSuccessful()
                    db.endTransaction()
                }
                safeClose(cursor)
            }
        }
    }

    /**
     * Return indexSpecs of soup
     *
     * @param soupName
     * @return
     */
    fun getSoupIndexSpecs(soupName: String): Array<IndexSpec> {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            return DBHelper.getInstance(db).getIndexSpecs(db, soupName)
        }
    }

    /**
     * Return true if the given path is indexed on the given soup
     *
     * @param soupName
     * @param path
     * @return
     */
    fun hasIndexForPath(soupName: String?, path: String?): Boolean {
        val db = database
        synchronized(db) { return DBHelper.getInstance(db).hasIndexForPath(db, soupName, path) }
    }

    /**
     * Clear all rows from a soup
     * @param soupName
     */
    fun clearSoup(soupName: String) {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            db.beginTransaction()
            try {
                DBHelper.getInstance(db).delete(db, soupTableName, null)
                if (hasFTS(soupName)) {
                    DBHelper.getInstance(db).delete(db, soupTableName + FTS_SUFFIX, null)
                }
                if (dbOpenHelper is DBOpenHelper) {
                    (dbOpenHelper as DBOpenHelper).removeExternalBlobsDirectory(soupTableName)
                }
            } finally {
                db.setTransactionSuccessful()
                db.endTransaction()
            }
        }
    }

    /**
     * Check if soup exists
     *
     * @param soupName
     * @return true if soup exists, false otherwise
     */
    fun hasSoup(soupName: String?): Boolean {
        val db = database
        synchronized(db) { return DBHelper.getInstance(db).getSoupTableName(db, soupName) != null }
    }

    /**
     * Destroy a soup
     *
     * Drop table for soupName
     * Cleanup entries in soup index map table
     * @param soupName
     */
    fun dropSoup(soupName: String) {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
            if (soupTableName != null) {
                db.execSQL("DROP TABLE IF EXISTS $soupTableName")
                if (hasFTS(soupName)) {
                    db.execSQL("DROP TABLE IF EXISTS " + soupTableName + FTS_SUFFIX)
                }
                try {
                    db.beginTransaction()
                    DBHelper.getInstance(db)
                        .delete(db, SOUP_ATTRS_TABLE, SOUP_NAME_PREDICATE, soupName)
                    DBHelper.getInstance(db)
                        .delete(db, SOUP_INDEX_MAP_TABLE, SOUP_NAME_PREDICATE, soupName)
                    if (dbOpenHelper is DBOpenHelper) {
                        (dbOpenHelper as DBOpenHelper).removeExternalBlobsDirectory(soupTableName)
                    }
                    db.setTransactionSuccessful()

                    // Remove from cache
                    DBHelper.getInstance(db).removeFromCache(soupName)
                } finally {
                    db.endTransaction()
                }
            }
        }
    }

    /**
     * Destroy all the soups in the smartstore
     */
    fun dropAllSoups() {
        val db = database
        synchronized(db) {
            val soupNames = allSoupNames
            for (soupName: String in soupNames) {
                dropSoup(soupName)
            }
        }
    }

    /**
     * @return all soup names in the smartstore
     */
    val allSoupNames: List<String>
        get() {
            val db = database
            synchronized(db) {
                val soupNames: MutableList<String> = ArrayList()
                var cursor: Cursor? = null
                try {
                    cursor = DBHelper.getInstance(db).query(
                        db,
                        SOUP_ATTRS_TABLE,
                        arrayOf(SOUP_NAME_COL),
                        SOUP_NAME_COL,
                        null,
                        null
                    )
                    if (cursor.moveToFirst()) {
                        do {
                            soupNames.add(cursor.getString(0))
                        } while (cursor.moveToNext())
                    }
                } finally {
                    safeClose(cursor)
                }
                return soupNames
            }
        }

    /**
     * Returns the entire SoupSpec of the given soup.
     * @param soupName
     * @return SoupSpec for given soup name.
     */
    @Deprecated(message = "We are removing external storage and soup spec in 11.0")
    fun getSoupSpec(soupName: String?): SoupSpec {
        val db = database
        val features = DBHelper.getInstance(db).getFeatures(db, soupName)
        return SoupSpec(soupName, *features.toTypedArray())
    }

    /**
     * Run a query given by its query spec
     * Returns results from selected page
     *
     * @param querySpec the query to run
     * @param pageIndex the page to return
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun query(querySpec: QuerySpec, pageIndex: Int): JSONArray {
        return queryWithArgs(querySpec, pageIndex, null)
    }

    /**
     * Run a query given by its query spec with optional "where args" (i.e. bind args)
     * Provided bind args will be substituted to the ? found in the query
     * NB: Bind args are only supported for smart queries
     * Returns results from selected page
     *
     * @param querySpec the query to run
     * @param pageIndex the page to return
     * @param whereArgs the bind args (optional - only supported for smart queries)
     *
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun queryWithArgs(querySpec: QuerySpec, pageIndex: Int, vararg whereArgs: String?): JSONArray {
        if (whereArgs.filterNotNull().isNotEmpty() && querySpec.queryType != QuerySpec.QueryType.smart) {
            throw SmartStoreException("whereArgs can only be provided for smart queries")
        }
        val resultAsArray = JSONArray()
        runQuery(resultAsArray, null, querySpec, pageIndex, *whereArgs)
        return resultAsArray
    }

    /**
     * Run a query given by its query Spec
     * Returns results from selected page without deserializing any JSON
     *
     * @param resultBuilder string builder to which results are appended
     * @param querySpec
     * @param pageIndex
     */
    fun queryAsString(resultBuilder: StringBuilder?, querySpec: QuerySpec, pageIndex: Int) {
        try {
            runQuery(null, resultBuilder, querySpec, pageIndex, null)
        } catch (e: JSONException) {
            // shouldn't happen since we call runQuery with a string builder
            throw SmartStoreException("Unexpected json exception", e)
        }
    }

    @Throws(JSONException::class)
    private fun runQuery(
        resultAsArray: JSONArray?,
        resultAsStringBuilder: StringBuilder?,
        querySpec: QuerySpec,
        pageIndex: Int,
        vararg whereArgs: String?
    ) {
        val computeResultAsString = resultAsStringBuilder != null
        val db = database
        synchronized(db) {
            val qt = querySpec.queryType
            val sql = convertSmartSql(querySpec.smartSql)

            // Page
            val offsetRows = querySpec.pageSize * pageIndex
            val numberRows = querySpec.pageSize
            val limit = "$offsetRows,$numberRows"
            var cursor: Cursor? = null
            try {
                cursor = DBHelper.getInstance(db).limitRawQuery(
                    db,
                    sql,
                    limit,
                    *if (querySpec.args != null) querySpec.args else whereArgs
                )
                if (computeResultAsString) {
                    resultAsStringBuilder!!.append("[")
                }
                var currentRow = 0
                if (cursor.moveToFirst()) {
                    do {
                        if (computeResultAsString && currentRow > 0) {
                            resultAsStringBuilder!!.append(", ")
                        }
                        currentRow++

                        // Smart queries
                        if (qt == QuerySpec.QueryType.smart || querySpec.selectPaths != null) {
                            if (computeResultAsString) {
                                getDataFromRow(null, resultAsStringBuilder, cursor)
                            } else {
                                val rowArray = JSONArray()
                                getDataFromRow(rowArray, null, cursor)
                                resultAsArray!!.put(rowArray)
                            }
                        } else {
                            var rowAsString: String? = null
                            if (cursor.getColumnIndex(SoupSpec.FEATURE_EXTERNAL_STORAGE) >= 0) {
                                // Presence of external storage column implies we must fetch from storage. Soup name and entry id values can be extracted
                                val soupTableName =
                                    cursor.getString(cursor.getColumnIndex(SoupSpec.FEATURE_EXTERNAL_STORAGE))
                                val soupEntryId =
                                    cursor.getLong(cursor.getColumnIndex(SOUP_ENTRY_ID))
                                rowAsString =
                                    (dbOpenHelper as DBOpenHelper?)!!.loadSoupBlobAsString(
                                        soupTableName,
                                        soupEntryId,
                                        encryptionKey
                                    )
                            } else {
                                rowAsString = cursor.getString(0)
                            }
                            if (computeResultAsString) {
                                resultAsStringBuilder!!.append(rowAsString)
                            } else {
                                resultAsArray!!.put(JSONObject(rowAsString))
                            }
                        }
                    } while (cursor.moveToNext())
                }
                if (computeResultAsString) {
                    resultAsStringBuilder!!.append("]")
                }
            } finally {
                safeClose(cursor)
            }
        }
    }

    @Throws(JSONException::class)
    private fun getDataFromRow(
        resultAsArray: JSONArray?,
        resultAsStringBuilder: StringBuilder?,
        cursor: Cursor?
    ) {
        val computeResultAsString = resultAsStringBuilder != null
        val columnCount = cursor!!.columnCount
        if (computeResultAsString) {
            resultAsStringBuilder!!.append("[")
        }
        var i = 0
        while (i < columnCount) {
            if (computeResultAsString && i > 0) {
                resultAsStringBuilder!!.append(",")
            }
            val valueType = cursor.getType(i)
            val columnName = cursor.getColumnName(i)
            if (valueType == Cursor.FIELD_TYPE_NULL) {
                if (computeResultAsString) {
                    resultAsStringBuilder!!.append("null")
                } else {
                    resultAsArray!!.put(null as Any?)
                }
            } else if (valueType == Cursor.FIELD_TYPE_STRING) {
                var raw = cursor.getString(i)
                if (columnName == SoupSpec.FEATURE_EXTERNAL_STORAGE) {
                    // Presence of external storage column implies we must fetch from storage. Soup name and entry id values can be extracted
                    val soupTableName = cursor.getString(i)
                    val soupEntryId = cursor.getLong(i + 1)
                    if (computeResultAsString) {
                        resultAsStringBuilder!!.append(
                            (dbOpenHelper as DBOpenHelper?)!!.loadSoupBlobAsString(
                                soupTableName,
                                soupEntryId,
                                encryptionKey
                            )
                        )
                    } else {
                        resultAsArray!!.put(
                            (dbOpenHelper as DBOpenHelper?)!!.loadSoupBlob(
                                soupTableName,
                                soupEntryId,
                                encryptionKey
                            )
                        )
                    }
                    i++ // skip next column (_soupEntryId)
                } else if (columnName == SOUP_COL || columnName.startsWith(SOUP_COL + ":") /* :num is appended to column name when result set has more than one column with same name */) {
                    if (computeResultAsString) {
                        resultAsStringBuilder!!.append(raw)
                    } else {
                        resultAsArray!!.put(JSONObject(raw))
                    }
                    // Note: we could end up returning a string if you aliased the column
                } else {
                    if (computeResultAsString) {
                        raw = escapeStringValue(raw)
                        resultAsStringBuilder!!.append("\"").append(raw).append("\"")
                    } else {
                        resultAsArray!!.put(raw)
                    }
                }
            } else if (valueType == Cursor.FIELD_TYPE_INTEGER) {
                if (computeResultAsString) {
                    resultAsStringBuilder!!.append(cursor.getLong(i))
                } else {
                    resultAsArray!!.put(cursor.getLong(i))
                }
            } else if (valueType == Cursor.FIELD_TYPE_FLOAT) {
                if (computeResultAsString) {
                    resultAsStringBuilder!!.append(cursor.getDouble(i))
                } else {
                    resultAsArray!!.put(cursor.getDouble(i))
                }
            }
            i++
        }
        if (computeResultAsString) {
            resultAsStringBuilder!!.append("]")
        }
    }

    private fun escapeStringValue(raw: String): String {
        val sb = StringBuilder()
        for (i in 0 until raw.length) {
            val c = raw[i]
            when (c) {
                '\\', '"' -> {
                    sb.append('\\')
                    sb.append(c)
                }
                '/' -> {
                    sb.append('\\')
                    sb.append(c)
                }
                '\b' -> sb.append("\\b")
                '\t' -> sb.append("\\t")
                '\n' -> sb.append("\\n")
                '\u000c' -> sb.append("\\f") // Kotlin does not support '\f', so we must use unicode code point.
                '\r' -> sb.append("\\r")
                else -> if (c < ' ') {
                    val t = "000" + Integer.toHexString(c.toInt())
                    sb.append("\\u" + t.substring(t.length - 4))
                } else {
                    sb.append(c)
                }
            }
        }
        return sb.toString()
    }

    /**
     * @param querySpec
     * @return count of results for a query
     */
    fun countQuery(querySpec: QuerySpec): Int {
        val db = database
        synchronized(db) {
            val countSql = convertSmartSql(querySpec.countSmartSql)
            return DBHelper.getInstance(db).countRawCountQuery(db, countSql, *querySpec.args)
        }
    }

    /**
     * @param smartSql
     * @return
     */
    fun convertSmartSql(smartSql: String?): String {
        val db = database
        synchronized(db) { return SmartSqlHelper.getInstance(db).convertSmartSql(db, smartSql) }
    }

    /**
     * Create (and commits)
     * Note: Passed soupElt is modified (last modified date and soup entry id fields)
     * @param soupName
     * @param soupElt
     * @return soupElt created or null if creation failed
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun create(soupName: String, soupElt: JSONObject): JSONObject? {
        val db = database
        synchronized(db) { return create(soupName, soupElt, true) }
    }

    /**
     * Create
     * Note: Passed soupElt is modified (last modified date and soup entry id fields)
     * @param soupName
     * @param soupElt
     * @return
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun create(soupName: String, soupElt: JSONObject, handleTx: Boolean): JSONObject? {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            val indexSpecs = DBHelper.getInstance(db).getIndexSpecs(db, soupName)
            try {
                if (handleTx) {
                    db.beginTransaction()
                }
                val now = System.currentTimeMillis()
                val soupEntryId = DBHelper.getInstance(db).getNextId(db, soupTableName)

                // Adding fields to soup element
                soupElt.put(SOUP_ENTRY_ID, soupEntryId)
                soupElt.put(SOUP_LAST_MODIFIED_DATE, now)
                val contentValues = ContentValues()
                contentValues.put(ID_COL, soupEntryId)
                contentValues.put(CREATED_COL, now)
                contentValues.put(LAST_MODIFIED_COL, now)
                if (!usesExternalStorage(soupName)) {
                    contentValues.put(SOUP_COL, soupElt.toString())
                }
                projectIndexedPaths(
                    soupElt,
                    contentValues,
                    indexSpecs,
                    TypeGroup.value_extracted_to_column
                )

                // Inserting into database
                var success =
                    DBHelper.getInstance(db).insert(db, soupTableName, contentValues) == soupEntryId

                // Fts
                if (success && hasFTS(soupName)) {
                    val soupTableNameFts = soupTableName + FTS_SUFFIX
                    val contentValuesFts = ContentValues()
                    contentValuesFts.put(ROWID_COL, soupEntryId)
                    projectIndexedPaths(
                        soupElt,
                        contentValuesFts,
                        indexSpecs,
                        TypeGroup.value_extracted_to_fts_column
                    )
                    // InsertHelper not working against virtual fts table
                    db.insert(soupTableNameFts, null, contentValuesFts)
                }

                // Add to external storage if applicable
                if (success && usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                    success = (dbOpenHelper as DBOpenHelper).saveSoupBlob(
                        soupTableName,
                        soupEntryId,
                        soupElt,
                        encryptionKey
                    )
                }

                // Commit if successful
                if (success) {
                    if (handleTx) {
                        db.setTransactionSuccessful()
                    }
                    return soupElt
                } else {
                    return null
                }
            } finally {
                if (handleTx) {
                    db.endTransaction()
                }
            }
        }
    }

    /**
     * @soupName
     * @return true if soup has at least one full-text search index
     */
    private fun hasFTS(soupName: String): Boolean {
        val db = database
        synchronized(db) { return DBHelper.getInstance(db).hasFTS(db, soupName) }
    }

    /**
     * Populate content values by projecting index specs that have a type in typeGroup
     * @param soupElt
     * @param contentValues
     * @param indexSpecs
     * @param typeGroup
     */
    private fun projectIndexedPaths(
        soupElt: JSONObject,
        contentValues: ContentValues,
        indexSpecs: Array<IndexSpec>,
        typeGroup: TypeGroup
    ) {
        for (indexSpec in indexSpecs) {
            if (typeGroup.isMember(indexSpec.type)) {
                projectIndexedPath(soupElt, contentValues, indexSpec)
            }
        }
    }

    /**
     * @param soupElt
     * @param contentValues
     * @param indexSpec
     */
    private fun projectIndexedPath(
        soupElt: JSONObject,
        contentValues: ContentValues,
        indexSpec: IndexSpec
    ) {
        val value = project(soupElt, indexSpec.path)
        contentValues.put(indexSpec.columnName, null as String?) // fall back
        if (value != null) {
            try {
                when (indexSpec.type) {
                    Type.integer -> contentValues.put(
                        indexSpec.columnName,
                        (value as Number).toLong()
                    )
                    Type.string, Type.full_text -> contentValues.put(
                        indexSpec.columnName,
                        value.toString()
                    )
                    Type.floating -> contentValues.put(
                        indexSpec.columnName,
                        (value as Number).toDouble()
                    )
                }
            } catch (e: Exception) {
                // Ignore (will use the null value)
                SmartStoreLogger.e(TAG, "Unexpected error", e)
            }
        }
    }

    /**
     * Retrieve
     * @param soupName
     * @param soupEntryIds
     * @return JSONArray of JSONObject's with the given soupEntryIds
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun retrieve(soupName: String, vararg soupEntryIds: Long?): JSONArray {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            val result = JSONArray()
            if (usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                for (soupEntryId: Long? in soupEntryIds) {
                    val raw = (dbOpenHelper as DBOpenHelper).loadSoupBlob(
                        soupTableName,
                        soupEntryId!!,
                        encryptionKey
                    )
                    if (raw != null) {
                        result.put(raw)
                    }
                }
            } else {
                var cursor: Cursor? = null
                try {
                    cursor = DBHelper.getInstance(db).query(
                        db,
                        soupTableName,
                        arrayOf(SOUP_COL),
                        null,
                        null,
                        getSoupEntryIdsPredicate(soupEntryIds),
                        null
                    )
                    if (!cursor.moveToFirst()) {
                        return result
                    }
                    do {
                        val raw = cursor.getString(cursor.getColumnIndex(SOUP_COL))
                        result.put(JSONObject(raw))
                    } while (cursor.moveToNext())
                } finally {
                    safeClose(cursor)
                }
            }
            return result
        }
    }

    /**
     * Update (and commits)
     * Note: Passed soupElt is modified (last modified date and soup entry id fields)
     * @param soupName
     * @param soupElt
     * @param soupEntryId
     * @return soupElt updated or null if update failed
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun update(soupName: String, soupElt: JSONObject, soupEntryId: Long): JSONObject? {
        val db = database
        synchronized(db) { return update(soupName, soupElt, soupEntryId, true) }
    }

    /**
     * Update
     * Note: Passed soupElt is modified (last modified date and soup entry id fields)
     * @param soupName
     * @param soupElt
     * @param soupEntryId
     * @param handleTx
     * @return
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun update(
        soupName: String,
        soupElt: JSONObject,
        soupEntryId: Long,
        handleTx: Boolean
    ): JSONObject? {
        val db = database
        synchronized(db) {
            try {
                if (handleTx) {
                    db.beginTransaction()
                }
                val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                    ?: throw SmartStoreException("Soup: $soupName does not exist")
                val indexSpecs = DBHelper.getInstance(db).getIndexSpecs(db, soupName)
                val now = System.currentTimeMillis()

                // In the case of an upsert with external id, _soupEntryId won't be in soupElt
                soupElt.put(SOUP_ENTRY_ID, soupEntryId)
                // Updating last modified field in soup element
                soupElt.put(SOUP_LAST_MODIFIED_DATE, now)

                // Preparing data for row
                val contentValues = ContentValues()
                contentValues.put(LAST_MODIFIED_COL, now)
                projectIndexedPaths(
                    soupElt,
                    contentValues,
                    indexSpecs,
                    TypeGroup.value_extracted_to_column
                )
                if (!usesExternalStorage(soupName)) {
                    contentValues.put(SOUP_COL, soupElt.toString())
                }

                // Updating database
                var success = DBHelper.getInstance(db).update(
                    db,
                    soupTableName,
                    contentValues,
                    ID_PREDICATE,
                    soupEntryId.toString() + ""
                ) == 1

                // Fts
                if (success && hasFTS(soupName)) {
                    val soupTableNameFts = soupTableName + FTS_SUFFIX
                    val contentValuesFts = ContentValues()
                    projectIndexedPaths(
                        soupElt,
                        contentValuesFts,
                        indexSpecs,
                        TypeGroup.value_extracted_to_fts_column
                    )
                    success = DBHelper.getInstance(db).update(
                        db,
                        soupTableNameFts,
                        contentValuesFts,
                        ROWID_PREDICATE,
                        soupEntryId.toString() + ""
                    ) == 1
                }

                // Add to external storage if applicable
                if (success && usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                    success = (dbOpenHelper as DBOpenHelper).saveSoupBlob(
                        soupTableName,
                        soupEntryId,
                        soupElt,
                        encryptionKey
                    )
                }
                if (success) {
                    if (handleTx) {
                        db.setTransactionSuccessful()
                    }
                    return soupElt
                } else {
                    return null
                }
            } finally {
                if (handleTx) {
                    db.endTransaction()
                }
            }
        }
    }

    /**
     * Upsert (and commits)
     * @param soupName
     * @param soupElt
     * @param externalIdPath
     * @return soupElt upserted or null if upsert failed
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun upsert(soupName: String, soupElt: JSONObject, externalIdPath: String): JSONObject? {
        val db = database
        synchronized(db) { return upsert(soupName, soupElt, externalIdPath, true) }
    }

    /**
     * Upsert (and commits) expecting _soupEntryId in soupElt for updates
     * @param soupName
     * @param soupElt
     * @return
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun upsert(soupName: String, soupElt: JSONObject): JSONObject? {
        val db = database
        synchronized(db) { return upsert(soupName, soupElt, SOUP_ENTRY_ID) }
    }

    /**
     * Upsert
     * @param soupName
     * @param soupElt
     * @param externalIdPath
     * @param handleTx
     * @return
     * @throws JSONException
     */
    @Throws(JSONException::class)
    fun upsert(
        soupName: String,
        soupElt: JSONObject,
        externalIdPath: String,
        handleTx: Boolean
    ): JSONObject? {
        val db = database
        synchronized(db) {
            var entryId: Long = -1
            if ((externalIdPath == SOUP_ENTRY_ID)) {
                if (soupElt.has(SOUP_ENTRY_ID)) {
                    entryId = soupElt.getLong(SOUP_ENTRY_ID)
                }
            } else {
                val externalIdObj = project(soupElt, externalIdPath)
                if (externalIdObj != null) {
                    entryId =
                        lookupSoupEntryId(soupName, externalIdPath, externalIdObj.toString() + "")
                } else {
                    // Cannot have empty values for user-defined external ID upsert.
                    throw SmartStoreException(
                        String.format(
                            "For upsert with external ID path '%s', value cannot be empty for any entries.",
                            externalIdPath
                        )
                    )
                }
            }

            // If we have an entryId, let's do an update, otherwise let's do a create
            return if (entryId != -1L) {
                update(soupName, soupElt, entryId, handleTx)
            } else {
                create(soupName, soupElt, handleTx)
            }
        }
    }

    /**
     * Look for a soup element where fieldPath's value is fieldValue
     * Return its soupEntryId
     * Return -1 if not found
     * Throw an exception if fieldName is not indexed
     * Throw an exception if more than one soup element are found
     *
     * @param soupName
     * @param fieldPath
     * @param fieldValue
     */
    fun lookupSoupEntryId(soupName: String, fieldPath: String?, fieldValue: String): Long {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            val columnName = DBHelper.getInstance(db).getColumnNameForPath(db, soupName, fieldPath)
            var cursor: Cursor? = null
            try {
                cursor = db.query(
                    soupTableName,
                    arrayOf(ID_COL),
                    "$columnName = ?",
                    arrayOf(fieldValue),
                    null,
                    null,
                    null
                )
                if (cursor.getCount() > 1) {
                    throw SmartStoreException(
                        String.format(
                            "There are more than one soup elements where %s is %s",
                            fieldPath,
                            fieldValue
                        )
                    )
                }
                return if (cursor.moveToFirst()) {
                    cursor.getLong(0)
                } else {
                    -1 // not found
                }
            } finally {
                safeClose(cursor)
            }
        }
    }

    /**
     * Delete soup elements given by their ids (and commits)
     * @param soupName
     * @param soupEntryIds
     */
    fun delete(soupName: String, vararg soupEntryIds: Long?) {
        val db = database
        synchronized(db) { delete(soupName, soupEntryIds, true) }
    }

    /**
     * Delete soup elements given by their ids
     * @param soupName
     * @param soupEntryIds
     * @param handleTx
     */
    fun delete(soupName: String, soupEntryIds: Array<out Long?>, handleTx: Boolean) {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            if (handleTx) {
                db.beginTransaction()
            }
            try {
                DBHelper.getInstance(db)
                    .delete(db, soupTableName, getSoupEntryIdsPredicate(soupEntryIds))
                if (hasFTS(soupName)) {
                    DBHelper.getInstance(db)
                        .delete(db, soupTableName + FTS_SUFFIX, getRowIdsPredicate(soupEntryIds))
                }
                if (usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                    (dbOpenHelper as DBOpenHelper).removeSoupBlob(soupTableName, soupEntryIds)
                }
                if (handleTx) {
                    db.setTransactionSuccessful()
                }
            } finally {
                if (handleTx) {
                    db.endTransaction()
                }
            }
        }
    }

    /**
     * Delete soup elements selected by querySpec (and commits)
     * @param soupName
     * @param querySpec Query returning entries to delete (if querySpec uses smartSQL, it must select soup entry ids)
     */
    fun deleteByQuery(soupName: String, querySpec: QuerySpec) {
        val db = database
        synchronized(db) { deleteByQuery(soupName, querySpec, true) }
    }

    /**
     * Delete soup elements selected by querySpec
     * @param soupName
     * @param querySpec
     * @param handleTx
     */
    fun deleteByQuery(soupName: String, querySpec: QuerySpec, handleTx: Boolean) {
        val db = database
        synchronized(db) {
            val soupTableName = DBHelper.getInstance(db).getSoupTableName(db, soupName)
                ?: throw SmartStoreException("Soup: $soupName does not exist")
            if (handleTx) {
                db.beginTransaction()
            }
            try {
                val subQuerySql = String.format(
                    "SELECT %s FROM (%s) LIMIT %d",
                    ID_COL,
                    convertSmartSql(querySpec.idsSmartSql),
                    querySpec.pageSize
                )
                val args = querySpec.args
                if (usesExternalStorage(soupName) && dbOpenHelper is DBOpenHelper) {
                    // Query list of ids and remove them from external storage
                    var c: Cursor? = null
                    try {
                        c = db.query(
                            soupTableName,
                            arrayOf(ID_COL),
                            buildInStatement(ID_COL, subQuerySql),
                            args,
                            null,
                            null,
                            null
                        )
                        if (c.moveToFirst()) {
                            val ids = arrayOfNulls<Long>(c.getCount())
                            var counter = 0
                            do {
                                ids[counter++] = c.getLong(0)
                            } while (c.moveToNext())
                            (dbOpenHelper as DBOpenHelper).removeSoupBlob(soupTableName, ids)
                        }
                    } finally {
                        c?.close()
                    }
                }
                DBHelper.getInstance(db)
                    .delete(db, soupTableName, buildInStatement(ID_COL, subQuerySql), *args)
                if (hasFTS(soupName)) {
                    DBHelper.getInstance(db).delete(
                        db, soupTableName + FTS_SUFFIX, buildInStatement(
                            ROWID_COL, subQuerySql
                        ), *args
                    )
                }
                if (handleTx) {
                    db.setTransactionSuccessful()
                }
            } finally {
                if (handleTx) {
                    db.endTransaction()
                }
            }
        }
    }

    /**
     * @return predicate to match soup entries by id
     */
    private fun getSoupEntryIdsPredicate(soupEntryIds: Array<out Long?>): String {
        return buildInStatement(ID_COL, TextUtils.join(",", soupEntryIds))
    }

    /**
     * @return predicate to match entries by rowid
     */
    private fun getRowIdsPredicate(rowids: Array<out Long?>): String {
        return buildInStatement(ROWID_COL, TextUtils.join(",", rowids))
    }

    /**
     * @param col
     * @param inPredicate
     * @return in statement
     */
    private fun buildInStatement(col: String, inPredicate: String): String {
        return String.format("%s IN (%s)", col, inPredicate)
    }

    /**
     * @param cursor
     */
    private fun safeClose(cursor: Cursor?) {
        cursor?.close()
    }

    /**
     * Enum for column type
     */
    enum class Type(val columnType: String?) {
        string("TEXT"), integer("INTEGER"), floating("REAL"), full_text("TEXT"), json1(null);

    }

    /**
     * Enum for type groups
     */
    enum class TypeGroup {
        value_extracted_to_column {
            override fun isMember(type: Type): Boolean {
                return type == Type.string || type == Type.integer || type == Type.floating || type == Type.full_text
            }
        },
        value_extracted_to_fts_column {
            override fun isMember(type: Type): Boolean {
                return type == Type.full_text
            }
        },
        value_indexed_with_json_extract {
            override fun isMember(type: Type): Boolean {
                return type == Type.json1
            }
        };

        abstract fun isMember(type: Type): Boolean
    }

    /**
     * Enum for fts extensions
     */
    enum class FtsExtension {
        fts4, fts5
    }

    /**
     * Exception thrown by smart store
     *
     */
    open class SmartStoreException : RuntimeException {
        constructor(message: String?) : super(message) {}
        constructor(message: String?, t: Throwable?) : super(message, t) {}

        companion object {
            private const val serialVersionUID = -6369452803270075464L
        }
    }

    /**
     * Determines if the given soup features external storage.
     *
     * @param soupName Name of the soup to determine external storage enablement.
     *
     * @return  True if soup uses external storage; false otherwise.
     */
    @Deprecated(message = "We are removing external storage and soup spec in 11.0")
    fun usesExternalStorage(soupName: String?): Boolean {
        val db = database
        synchronized(db) {
            return DBHelper.getInstance(db).getFeatures(db, soupName)
                .contains(SoupSpec.FEATURE_EXTERNAL_STORAGE)
        }
    }

    /**
     * Get SQLCipher runtime settings
     *
     * @return list of SQLCipher runtime settings
     */
    val runtimeSettings: List<String?>
        get() = queryPragma("cipher_settings")

    /**
     * Get SQLCipher compile options
     *
     * @return list of SQLCipher compile options
     */
    val compileOptions: List<String?>
        get() = queryPragma("compile_options")

    /**
     * Get SQLCipher version
     *
     * @return SQLCipher version
     */
    val sQLCipherVersion: String
        get() = TextUtils.join(" ", queryPragma("cipher_version"))

    private fun queryPragma(pragma: String): List<String?> {
        val db = database
        val results = ArrayList<String?>()
        var c: Cursor? = null
        try {
            c = db.rawQuery("PRAGMA $pragma", null)
            while (c.moveToNext()) {
                results.add(c.getString(0))
            }
        } finally {
            safeClose(c)
        }
        return results
    }

    companion object {
        private const val TAG = "SmartStore"

        // Table to keep track of soup names and attributes.
        const val SOUP_ATTRS_TABLE = "soup_attrs"

        // Fts table suffix
        const val FTS_SUFFIX = "_fts"

        // Table to keep track of soup's index specs
        const val SOUP_INDEX_MAP_TABLE = "soup_index_map"

        // Table to keep track of status of long operations in flight
        const val LONG_OPERATIONS_STATUS_TABLE = "long_operations_status"

        // Columns of the soup index map table
        const val SOUP_NAME_COL = "soupName"
        const val PATH_COL = "path"
        const val COLUMN_NAME_COL = "columnName"
        const val COLUMN_TYPE_COL = "columnType"

        // Columns of a soup table
        const val ID_COL = "id"
        const val CREATED_COL = "created"
        const val LAST_MODIFIED_COL = "lastModified"
        const val SOUP_COL = "soup"

        // Column of a fts soup table
        const val ROWID_COL = "rowid"

        // Columns of long operations status table
        const val TYPE_COL = "type"
        const val DETAILS_COL = "details"
        const val STATUS_COL = "status"

        // JSON fields added to soup element on insert/update
        const val SOUP_ENTRY_ID = "_soupEntryId"
        const val SOUP_LAST_MODIFIED_DATE = "_soupLastModifiedDate"
        const val SOUP_CREATED_DATE = "_soupCreatedDate"

        // Predicates
        const val SOUP_NAME_PREDICATE = SOUP_NAME_COL + " = ?"
        const val ID_PREDICATE = ID_COL + " = ?"
        protected val ROWID_PREDICATE = ROWID_COL + " =?"

        /**
         * Changes the encryption key on the smartstore.
         *
         * @param db Database object.
         * @param oldKey Old encryption key.
         * @param newKey New encryption key.
         */
        @Synchronized
        fun changeKey(db: SQLiteDatabase?, oldKey: String?, newKey: String?) {
            synchronized(db!!) {
                if (!TextUtils.isEmpty(newKey)) {
                    DBOpenHelper.changeKey(db, oldKey, newKey)
                    DBOpenHelper.reEncryptAllFiles(db, oldKey, newKey)
                }
            }
        }

        /**
         * Create soup index map table to keep track of soups' index specs
         * Create soup name map table to keep track of soup name to table name mappings
         * Called when the database is first created
         *
         * @param db
         */
        fun createMetaTables(db: SQLiteDatabase) {
            synchronized(db) {

                // Create soup_index_map table
                var sb = StringBuilder()
                sb.append("CREATE TABLE ").append(SOUP_INDEX_MAP_TABLE).append(" (")
                    .append(SOUP_NAME_COL).append(" TEXT")
                    .append(",").append(PATH_COL).append(" TEXT")
                    .append(",").append(COLUMN_NAME_COL).append(" TEXT")
                    .append(",").append(COLUMN_TYPE_COL).append(" TEXT")
                    .append(")")
                db.execSQL(sb.toString())
                // Add index on soup_name column
                db.execSQL(
                    String.format(
                        "CREATE INDEX %s on %s ( %s )",
                        SOUP_INDEX_MAP_TABLE + "_0",
                        SOUP_INDEX_MAP_TABLE,
                        SOUP_NAME_COL
                    )
                )

                // Create soup_names table
                // The table name for the soup will simply be table_<soupId>
                sb = StringBuilder()
                sb.append("CREATE TABLE ").append(SOUP_ATTRS_TABLE).append(" (")
                    .append(ID_COL).append(" INTEGER PRIMARY KEY AUTOINCREMENT")
                    .append(",").append(SOUP_NAME_COL).append(" TEXT")

                // Create columns for all possible soup features
                for (feature: String? in SoupSpec.ALL_FEATURES) {
                    sb.append(",").append(feature).append(" INTEGER DEFAULT 0")
                }
                sb.append(")")
                db.execSQL(sb.toString())
                // Add index on soup_name column
                db.execSQL(
                    String.format(
                        "CREATE INDEX %s on %s ( %s )",
                        SOUP_ATTRS_TABLE + "_0",
                        SOUP_ATTRS_TABLE,
                        SOUP_NAME_COL
                    )
                )

                // Create alter_soup_status table
                createLongOperationsStatusTable(db)
            }
        }

        /**
         * Create long_operations_status table
         * @param db
         */
        fun createLongOperationsStatusTable(db: SQLiteDatabase) {
            synchronized(db) {
                val sb = StringBuilder()
                sb.append("CREATE TABLE IF NOT EXISTS ").append(LONG_OPERATIONS_STATUS_TABLE)
                    .append(" (")
                    .append(ID_COL).append(" INTEGER PRIMARY KEY AUTOINCREMENT")
                    .append(",").append(TYPE_COL).append(" TEXT")
                    .append(",").append(DETAILS_COL).append(" TEXT")
                    .append(",").append(STATUS_COL).append(" TEXT")
                    .append(", ").append(CREATED_COL).append(" INTEGER")
                    .append(", ").append(LAST_MODIFIED_COL).append(" INTEGER")
                    .append(")")
                db.execSQL(sb.toString())
            }
        }

        /**
         * @param soupId
         * @return
         */
        fun getSoupTableName(soupId: Long): String {
            return "TABLE_$soupId"
        }

        /**
         * @param soup
         * @param path
         * @return object at path in soup
         *
         * Examples (in pseudo code):
         *
         * json = {"a": {"b": [{"c":"xx"}, {"c":"xy"}, {"d": [{"e":1}, {"e":2}]}, {"d": [{"e":3}, {"e":4}]}] }}
         * projectIntoJson(jsonObj, "a") = {"b": [{"c":"xx"}, {"c":"xy"}, {"d": [{"e":1}, {"e":2}]}, {"d": [{"e":3}, {"e":4}]} ]}
         * projectIntoJson(json, "a.b") = [{c:"xx"}, {c:"xy"}, {"d": [{"e":1}, {"e":2}]}, {"d": [{"e":3}, {"e":4}]}]
         * projectIntoJson(json, "a.b.c") = ["xx", "xy"]                                     // new in 4.1
         * projectIntoJson(json, "a.b.d") = [[{"e":1}, {"e":2}], [{"e":3}, {"e":4}]]         // new in 4.1
         * projectIntoJson(json, "a.b.d.e") = [[1, 2], [3, 4]]                               // new in 4.1
         */
        fun project(soup: JSONObject?, path: String?): Any? {
            val result = projectReturningNULLObject(soup, path)
            return if (result === JSONObject.NULL) null else result
        }

        /**
         * Same as project but returns JSONObject.NULL if node found but without value and null if node not found
         * @param soup
         * @param path
         * @return
         */
        fun projectReturningNULLObject(soup: JSONObject?, path: String?): Any? {
            if (soup == null) {
                return null
            }
            if (path == null || path == "") {
                return soup
            }
            val pathElements = path.split("[.]").toTypedArray()
            return projectRecursive(soup, pathElements, 0)
        }

        private fun projectRecursive(jsonObj: Any?, pathElements: Array<String>, index: Int): Any? {
            var result: Any? = null
            if (index == pathElements.size) {
                return jsonObj
            }
            if (null != jsonObj) {
                val pathElement = pathElements[index]
                if (jsonObj is JSONObject) {
                    val dictVal = jsonObj.opt(pathElement)
                    result = projectRecursive(dictVal, pathElements, index + 1)
                } else if (jsonObj is JSONArray) {
                    val jsonArr = jsonObj
                    result = JSONArray()
                    for (i in 0 until jsonArr.length()) {
                        val arrayElt = jsonArr.opt(i)
                        val resultPart = projectRecursive(arrayElt, pathElements, index)
                        if (resultPart != null) {
                            result.put(resultPart)
                        }
                    }
                    if (result.length() == 0) {
                        result = null
                    }
                }
            }
            return result
        }

        /**
         * Updates the given table with a new name and adds columns if any.
         *
         * @param db Database to update
         * @param oldName Old name of the table to be renamed, null if table should not be renamed.
         * @param newName New name of the table to be renamed, null if table should not be renamed.
         * @param columns Columns to add. Null if no new columns should be added.
         */
        fun updateTableNameAndAddColumns(
            db: SQLiteDatabase,
            oldName: String?,
            newName: String?,
            columns: Array<String?>?
        ) {
            synchronized(SmartStore::class.java) {
                var sb = StringBuilder()
                if (columns != null && columns.size > 0) {
                    for (column: String? in columns) {
                        sb.append("ALTER TABLE ").append(oldName).append(" ADD COLUMN ")
                            .append(column).append(" INTEGER DEFAULT 0;")
                    }
                    db.execSQL(sb.toString())
                }
                if (oldName != null && newName != null) {
                    sb = StringBuilder()
                    sb.append("ALTER TABLE ").append(oldName).append(" RENAME TO ").append(newName)
                        .append(';')
                    db.execSQL(sb.toString())
                }
            }
        }
    }
}
