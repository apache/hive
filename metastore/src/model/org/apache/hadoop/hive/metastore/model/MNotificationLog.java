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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.model;

public class MNotificationLog {

  private long eventId; // This is not the datanucleus id, but the id assigned by the sequence
  private int eventTime;
  private String eventType;
  private String dbName;
  private String tableName;
  private String message;
  private String messageFormat;

  public MNotificationLog() {
  }

  public MNotificationLog(int eventId, String eventType, String dbName, String tableName,
                          String message) {
    this.eventId = eventId;
    this.eventType = eventType;
    this.dbName = dbName;
    this.tableName = tableName;
    this.message = message;
  }

  public void setEventId(long eventId) {
    this.eventId = eventId;
  }

  public long getEventId() {
    return eventId;

  }

  public int getEventTime() {
    return eventTime;
  }

  public void setEventTime(int eventTime) {
    this.eventTime = eventTime;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessageFormat() {
    return messageFormat;
  }

  public void setMessageFormat(String messageFormat) {
    this.messageFormat = messageFormat;
  }
}
