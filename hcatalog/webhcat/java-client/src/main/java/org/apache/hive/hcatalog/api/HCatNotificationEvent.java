/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

/**
 * A wrapper class for {@link org.apache.hadoop.hive.metastore.api.NotificationEvent},
 * so that if that class changes we can still keep this one constant for backward compatibility
 */
public class HCatNotificationEvent {
  private long eventId;
  private int eventTime;
  private String eventType;
  private String dbName;
  private String tableName;
  private String message;

  public enum Scope { DB, TABLE, UNKNOWN };

  public HCatNotificationEvent(NotificationEvent event) {
    eventId = event.getEventId();
    eventTime = event.getEventTime();
    eventType = event.getEventType();
    dbName = event.getDbName();
    tableName = event.getTableName();
    message = event.getMessage();
  }

  public long getEventId() {
    return eventId;
  }

  public Scope getEventScope() {
    // Eventually, we want this to be a richer description of having
    // a DB, TABLE, ROLE, etc scope. For now, we have a trivial impl
    // of having only DB and TABLE scopes, as determined by whether
    // or not the tableName is null.
    if (dbName != null){
      if (tableName != null){
        return Scope.TABLE;
      }
      return Scope.DB;
    }
    return Scope.UNKNOWN;
  }

  public int getEventTime() {
    return eventTime;
  }

  public String getEventType() {
    return eventType;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("eventId:");
    buf.append(eventId);
    buf.append(" eventTime:");
    buf.append(eventTime);
    buf.append(" eventType:<");
    buf.append(eventType);
    buf.append("> dbName:<");
    buf.append(dbName);
    buf.append("> tableName:<");
    buf.append(tableName);
    buf.append("> message:<");
    buf.append(message);
    buf.append(">");
    return buf.toString();
  }
}
