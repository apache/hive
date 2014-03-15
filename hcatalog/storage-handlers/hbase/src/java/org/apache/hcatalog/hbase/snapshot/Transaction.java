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
package org.apache.hcatalog.hbase.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is responsible for storing information related to
 * transactions.
 */
public class Transaction implements Serializable {

  private String tableName;
  private List<String> columnFamilies = new ArrayList<String>();
  private long timeStamp;
  private long keepAlive;
  private long revision;


  Transaction(String tableName, List<String> columnFamilies, long revision, long timestamp) {
    this.tableName = tableName;
    this.columnFamilies = columnFamilies;
    this.timeStamp = timestamp;
    this.revision = revision;
  }

  /**
   * @return The revision number associated with a transaction.
   */
  public long getRevisionNumber() {
    return this.revision;
  }

  /**
   * @return The table name associated with a transaction.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return The column families associated with a transaction.
   */
  public List<String> getColumnFamilies() {
    return columnFamilies;
  }

  /**
   * For wire serialization only
   */
  long getTimeStamp() {
    return timeStamp;
  }

  /**
   * @return The expire timestamp associated with a transaction.
   */
  long getTransactionExpireTimeStamp() {
    return this.timeStamp + this.keepAlive;
  }

  void setKeepAlive(long seconds) {
    this.keepAlive = seconds;
  }

  /**
   * Gets the keep alive value.
   *
   * @return long  The keep alive value for the transaction.
   */
  public long getKeepAliveValue() {
    return this.keepAlive;
  }

  /**
   * Gets the family revision info.
   *
   * @return FamilyRevision An instance of FamilyRevision associated with the transaction.
   */
  FamilyRevision getFamilyRevisionInfo() {
    return new FamilyRevision(revision, getTransactionExpireTimeStamp());
  }

  /**
   * Keep alive transaction. This methods extends the expire timestamp of a
   * transaction by the "keep alive" amount.
   */
  void keepAliveTransaction() {
    this.timeStamp = this.timeStamp + this.keepAlive;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Revision : ");
    sb.append(this.getRevisionNumber());
    sb.append(" Timestamp : ");
    sb.append(this.getTransactionExpireTimeStamp());
    sb.append("\n").append("Table : ");
    sb.append(this.tableName).append("\n");
    sb.append("Column Families : ");
    sb.append(this.columnFamilies.toString());
    return sb.toString();
  }
}
