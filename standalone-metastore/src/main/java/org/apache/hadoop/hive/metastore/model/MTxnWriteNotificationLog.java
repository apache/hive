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
package org.apache.hadoop.hive.metastore.model;

/**
 * MTxnWriteNotificationLog
 * DN table for ACID write events.
 */
public class MTxnWriteNotificationLog {
  private long txnId;
  private long writeId;
  private int eventTime;
  private String database;
  private String table;
  private String partition;
  private String tableObject;
  private String partObject;
  private String files;

  public MTxnWriteNotificationLog() {
  }

  public MTxnWriteNotificationLog(long txnId, long writeId, int eventTime, String database, String table,
                               String partition, String tableObject, String partObject, String files) {
    this.txnId = txnId;
    this.writeId = writeId;
    this.eventTime = eventTime;
    this.database = database;
    this.table = table;
    this.partition = partition;
    this.tableObject = tableObject;
    this.partObject = partObject;
    this.files = files;
  }

  public long getTxnId() {
    return txnId;
  }

  public void setTxnId(long txnId) {
    this.txnId = txnId;
  }

  public long getWriteId() {
    return writeId;
  }

  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  public int getEventTime() {
    return eventTime;
  }

  public void setEventTime(int eventTime) {
    this.eventTime = eventTime;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public String getTableObject() {
    return tableObject;
  }

  public void setTableObject(String tableObject) {
    this.tableObject = tableObject;
  }

  public String getPartObject() {
    return partObject;
  }

  public void setPartObject(String partObject) {
    this.partObject = partObject;
  }

  public String getFiles() {
    return files;
  }

  public void setFiles(String files) {
    this.files = files;
  }
}

