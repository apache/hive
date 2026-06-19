/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.regex.Pattern;

/**
 * Utility function that constructs a notification filter to match a given db name and/or table name.
 * If dbName == null, fetches all warehouse events.
 * If dnName != null, but tableName == null, fetches all events for the db
 * If dbName != null &amp;&amp; tableName != null, fetches all events for the specified table
 */
public class DatabaseAndTableFilter extends BasicFilter {
  private final String tableName;
  private final Pattern dbPattern;

  public DatabaseAndTableFilter(final String databaseNameOrPattern, final String tableName) {
    // we convert the databaseNameOrPattern to lower case because events will have these names in lower case.
    this.dbPattern = (databaseNameOrPattern == null || databaseNameOrPattern.equals("*"))
        ? null
        : Pattern.compile(databaseNameOrPattern, Pattern.CASE_INSENSITIVE);
    this.tableName = tableName;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    if ((dbPattern == null) || isTxnRelatedEvent(event)) {
      return true;
    }
    if (dbPattern.matcher(event.getDbName()).matches()) {
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
