package com.salesforce.androidsdk.smartstore.store

import android.util.Log
import net.zetetic.database.sqlcipher.SQLiteDatabase

fun SQLiteDatabase.fullCheckpointNow() {
    Log.d("SQLiteDatabaseExt", "full checkpoint now")
    query("PRAGMA wal_checkpoint(RESTART)").moveToNext()
}
