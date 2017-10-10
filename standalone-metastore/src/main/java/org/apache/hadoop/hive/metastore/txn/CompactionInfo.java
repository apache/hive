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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.ValidCompactorTxnList;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Information on a possible or running compaction.
 */
public class CompactionInfo implements Comparable<CompactionInfo> {
  public long id;
  public String dbname;
  public String tableName;
  public String partName;
  char state;
  public CompactionType type;
  String workerId;
  long start;
  public String runAs;
  public String properties;
  public boolean tooManyAborts = false;
  /**
   * {@code 0} means it wasn't set (e.g. in case of upgrades, since ResultSet.getLong() will return 0 if field is NULL) 
   * See {@link TxnStore#setCompactionHighestTxnId(CompactionInfo, long)} for precise definition.
   * See also {@link TxnUtils#createValidCompactTxnList(GetOpenTxnsInfoResponse)} and
   * {@link ValidCompactorTxnList#highWatermark}
   */
  public long highestTxnId;
  byte[] metaInfo;
  String hadoopJobId;

  private String fullPartitionName = null;
  private String fullTableName = null;

  public CompactionInfo(String dbname, String tableName, String partName, CompactionType type) {
    this.dbname = dbname;
    this.tableName = tableName;
    this.partName = partName;
    this.type = type;
  }
  CompactionInfo(long id, String dbname, String tableName, String partName, char state) {
    this(dbname, tableName, partName, null);
    this.id = id;
    this.state = state;
  }
  CompactionInfo() {}
  
  public String getFullPartitionName() {
    if (fullPartitionName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      if (partName != null) {
        buf.append('.');
        buf.append(partName);
      }
      fullPartitionName = buf.toString();
    }
    return fullPartitionName;
  }

  public String getFullTableName() {
    if (fullTableName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      fullTableName = buf.toString();
    }
    return fullTableName;
  }
  public boolean isMajorCompaction() {
    return CompactionType.MAJOR == type;
  }

  @Override
  public int compareTo(CompactionInfo o) {
    return getFullPartitionName().compareTo(o.getFullPartitionName());
  }
  public String toString() {
    return "id:" + id + "," +
      "dbname:" + dbname + "," +
      "tableName:" + tableName + "," +
      "partName:" + partName + "," +
      "state:" + state + "," +
      "type:" + type + "," +
      "properties:" + properties + "," +
      "runAs:" + runAs + "," +
      "tooManyAborts:" + tooManyAborts + "," +
      "highestTxnId:" + highestTxnId;
  }

  /**
   * loads object from a row in Select * from COMPACTION_QUEUE
   * @param rs ResultSet after call to rs.next()
   * @throws SQLException
   */
  static CompactionInfo loadFullFromCompactionQueue(ResultSet rs) throws SQLException {
    CompactionInfo fullCi = new CompactionInfo();
    fullCi.id = rs.getLong(1);
    fullCi.dbname = rs.getString(2);
    fullCi.tableName = rs.getString(3);
    fullCi.partName = rs.getString(4);
    fullCi.state = rs.getString(5).charAt(0);//cq_state
    fullCi.type = TxnHandler.dbCompactionType2ThriftType(rs.getString(6).charAt(0));
    fullCi.properties = rs.getString(7);
    fullCi.workerId = rs.getString(8);
    fullCi.start = rs.getLong(9);
    fullCi.runAs = rs.getString(10);
    fullCi.highestTxnId = rs.getLong(11);
    fullCi.metaInfo = rs.getBytes(12);
    fullCi.hadoopJobId = rs.getString(13);
    return fullCi;
  }
  static void insertIntoCompletedCompactions(PreparedStatement pStmt, CompactionInfo ci, long endTime) throws SQLException {
    pStmt.setLong(1, ci.id);
    pStmt.setString(2, ci.dbname);
    pStmt.setString(3, ci.tableName);
    pStmt.setString(4, ci.partName);
    pStmt.setString(5, Character.toString(ci.state));
    pStmt.setString(6, Character.toString(TxnHandler.thriftCompactionType2DbType(ci.type)));
    pStmt.setString(7, ci.properties);
    pStmt.setString(8, ci.workerId);
    pStmt.setLong(9, ci.start);
    pStmt.setLong(10, endTime);
    pStmt.setString(11, ci.runAs);
    pStmt.setLong(12, ci.highestTxnId);
    pStmt.setBytes(13, ci.metaInfo);
    pStmt.setString(14, ci.hadoopJobId);
  }
}
