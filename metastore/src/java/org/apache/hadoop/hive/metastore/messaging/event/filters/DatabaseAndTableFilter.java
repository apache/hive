package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

/**
 * Utility function that constructs a notification filter to match a given db name and/or table name.
 * If dbName == null, fetches all warehouse events.
 * If dnName != null, but tableName == null, fetches all events for the db
 * If dbName != null && tableName != null, fetches all events for the specified table
 */
public class DatabaseAndTableFilter extends BasicFilter {
  private final String databaseName, tableName;

  public DatabaseAndTableFilter(final String databaseName, final String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    if (databaseName == null) {
      return true; // if our dbName is null, we're interested in all wh events
    }
    if (databaseName.equalsIgnoreCase(event.getDbName())) {
      if ((tableName == null)
          // if our dbName is equal, but tableName is blank, we're interested in this db-level event
          || (tableName.equalsIgnoreCase(event.getTableName()))
        // table level event that matches us
          ) {
        return true;
      }
    }
    return false;
  }
}
