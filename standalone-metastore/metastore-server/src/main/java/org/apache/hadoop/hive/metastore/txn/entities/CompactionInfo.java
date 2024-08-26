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
package org.apache.hadoop.hive.metastore.txn.entities;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Information on a possible or running compaction.
 */
public class CompactionInfo implements Comparable<CompactionInfo> {

  /**
   *  Modifying this variables or adding new ones should be done in sync
   *  with the static methods {@code compactionStructToInfo()} and
   *  {@code compactionInfoToStruct()}. This class is going to be deserialized
   *  and serialized so missing this may result in the value of the field
   *  being resetted. This will be fixed at HIVE-21056.
   */
  public long id;
  public String dbname;
  public String tableName;
  public String partName;
  public char state;
  public CompactionType type;
  public String workerId;
  public String workerVersion;
  public String initiatorId;
  public String initiatorVersion;
  public long enqueueTime;
  public long start;
  public String runAs;
  public String properties;
  public boolean tooManyAborts = false;
  public boolean hasOldAbort = false;
  public long retryRetention = 0;
  public long nextTxnId = 0;
  public long minOpenWriteId = -1;
  public long txnId = 0;
  public long commitTime = 0;
  public String poolName;
  public int numberOfBuckets = 0;
  public String orderByClause;
  public long minOpenWriteTxnId = 0;

  /**
   * The highest write id that the compaction job will pay attention to.
   * {@code 0} means it wasn't set (e.g. in case of upgrades, since ResultSet.getLong() will return 0 if field is NULL)
   * See also {@link TxnUtils#createValidCompactWriteIdList(TableValidWriteIds)} and
   * {@link ValidCompactorWriteIdList#highWatermark}.
   */
  public long highestWriteId;
  public Set<Long> writeIds;
  public boolean hasUncompactedAborts;

  public byte[] metaInfo;
  public String hadoopJobId;
  public String errorMessage;

  private String fullPartitionName = null;
  private String fullTableName = null;
  private StringableMap propertiesMap;

  public CompactionInfo(String dbname, String tableName, String partName, CompactionType type) {
    this.dbname = dbname;
    this.tableName = tableName;
    this.partName = partName;
    this.type = type;
  }
  public CompactionInfo(long id, String dbname, String tableName, String partName, char state) {
    this(dbname, tableName, partName, null);
    this.id = id;
    this.state = state;
  }
  public CompactionInfo() {}

  public String getProperty(String key) {
    if (propertiesMap == null) {
      propertiesMap = new StringableMap(properties);
    }
    return propertiesMap.get(key);
  }

  public void setProperty(String key, String value) {
    if (propertiesMap == null) {
      propertiesMap = new StringableMap(properties);
    }
    propertiesMap.put(key, value);
    properties = propertiesMap.toString();
  }

  public String getFullPartitionName() {
    if (fullPartitionName == null) {
      StringBuilder buf = new StringBuilder();
      buf.append(dbname);
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

  public boolean isRebalanceCompaction() {
    return CompactionType.REBALANCE == type;
  }

  @Override
  public int compareTo(CompactionInfo o) {
    return getFullPartitionName().compareTo(o.getFullPartitionName());
  }

  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("dbname", dbname)
        .append("tableName", tableName)
        .append("partName", partName)
        .append("state", state)
        .append("type", type)
        .append("enqueueTime", enqueueTime)
        .append("commitTime", commitTime)
        .append("start", start)
        .append("properties", properties)
        .append("runAs", runAs)
        .append("tooManyAborts", tooManyAborts)
        .append("hasOldAbort", hasOldAbort)
        .append("highestWriteId", highestWriteId)
        .append("errorMessage", errorMessage)
        .append("workerId", workerId)
        .append("workerVersion", workerVersion)
        .append("initiatorId", initiatorId)
        .append("initiatorVersion", initiatorVersion)
        .append("retryRetention", retryRetention)
        .append("txnId", txnId)
        .append("nextTxnId", nextTxnId)
        .append("poolName", poolName)
        .append("numberOfBuckets", numberOfBuckets)
        .append("orderByClause", orderByClause)
        .append("minOpenWriteTxnId", minOpenWriteTxnId)
        .build();
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + this.getFullPartitionName().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof CompactionInfo)) {
      return false;
    }
    CompactionInfo info = (CompactionInfo) obj;
    return this.compareTo(info) == 0;
  }

  /**
   * loads object from a row in Select * from COMPACTION_QUEUE
   * @param rs ResultSet after call to rs.next()
   * @throws SQLException
   */
  public static CompactionInfo loadFullFromCompactionQueue(ResultSet rs) throws SQLException {
    CompactionInfo fullCi = new CompactionInfo();
    fullCi.id = rs.getLong(1);
    fullCi.dbname = rs.getString(2);
    fullCi.tableName = rs.getString(3);
    fullCi.partName = rs.getString(4);
    fullCi.state = rs.getString(5).charAt(0);//cq_state
    fullCi.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(6).charAt(0));
    fullCi.properties = rs.getString(7);
    fullCi.workerId = rs.getString(8);
    fullCi.start = rs.getLong(9);
    fullCi.runAs = rs.getString(10);
    fullCi.highestWriteId = rs.getLong(11);
    fullCi.metaInfo = rs.getBytes(12);
    fullCi.hadoopJobId = rs.getString(13);
    fullCi.errorMessage = rs.getString(14);
    fullCi.enqueueTime = rs.getLong(15);
    fullCi.workerVersion = rs.getString(16);
    fullCi.initiatorId = rs.getString(17);
    fullCi.initiatorVersion = rs.getString(18);
    fullCi.retryRetention = rs.getLong(19);
    fullCi.nextTxnId = rs.getLong(20);
    fullCi.txnId = rs.getLong(21);
    fullCi.commitTime = rs.getLong(22);
    fullCi.poolName = rs.getString(23);
    fullCi.numberOfBuckets = rs.getInt(24);
    fullCi.orderByClause = rs.getString(25);
    return fullCi;
  }
  static void insertIntoCompletedCompactions(PreparedStatement pStmt, CompactionInfo ci, long endTime) throws SQLException, MetaException {
    pStmt.setLong(1, ci.id);
    pStmt.setString(2, ci.dbname);
    pStmt.setString(3, ci.tableName);
    pStmt.setString(4, ci.partName);
    pStmt.setString(5, Character.toString(ci.state));
    pStmt.setString(6, Character.toString(TxnUtils.thriftCompactionType2DbType(ci.type)));
    pStmt.setString(7, ci.properties);
    pStmt.setString(8, ci.workerId);
    pStmt.setLong(9, ci.start);
    pStmt.setLong(10, endTime);
    pStmt.setString(11, ci.runAs);
    pStmt.setLong(12, ci.highestWriteId);
    pStmt.setBytes(13, ci.metaInfo);
    pStmt.setString(14, ci.hadoopJobId);
    pStmt.setString(15, ci.errorMessage);
    pStmt.setLong(16, ci.enqueueTime);
    pStmt.setString(17, ci.workerVersion);
    pStmt.setString(18, ci.initiatorId);
    pStmt.setString(19, ci.initiatorVersion);
    pStmt.setLong(20, ci.nextTxnId);
    pStmt.setLong(21, ci.txnId);
    pStmt.setLong(22, ci.commitTime);
    pStmt.setString(23, ci.poolName);
    pStmt.setInt(24, ci.numberOfBuckets);
    pStmt.setString(25, ci.orderByClause);
  }

  public static CompactionInfo compactionStructToInfo(CompactionInfoStruct cr) {
    if (cr == null) {
      return null;
    }
    CompactionInfo ci = new CompactionInfo(cr.getDbname(), cr.getTablename(), cr.getPartitionname(), cr.getType());
    ci.id = cr.getId();
    ci.runAs = cr.getRunas();
    ci.properties = cr.getProperties();
    if (cr.isSetToomanyaborts()) {
      ci.tooManyAborts = cr.isToomanyaborts();
    }
    if (cr.isSetHasoldabort()) {
      ci.hasOldAbort = cr.isHasoldabort();
    }
    if (cr.isSetState() && cr.getState().length() != 1) {
      throw new IllegalStateException("State should only be one character but it was set to " + cr.getState());
    } else if (cr.isSetState()) {
      ci.state = cr.getState().charAt(0);
    }
    ci.workerId = cr.getWorkerId();
    if (cr.isSetStart()) {
      ci.start = cr.getStart();
    }
    if (cr.isSetHighestWriteId()) {
      ci.highestWriteId = cr.getHighestWriteId();
    }
    if (cr.isSetErrorMessage()) {
      ci.errorMessage = cr.getErrorMessage();
    }
    if (cr.isSetEnqueueTime()) {
      ci.enqueueTime = cr.getEnqueueTime();
    }
    if (cr.isSetRetryRetention()) {
      ci.retryRetention = cr.getRetryRetention();
    }
    if (cr.isSetPoolname()) {
      ci.poolName = cr.getPoolname();
    }
    if (cr.isSetNumberOfBuckets()) {
      ci.numberOfBuckets = cr.getNumberOfBuckets();
    }
    if (cr.isSetOrderByClause()) {
      ci.orderByClause = cr.getOrderByClause();
    }
    return ci;
  }

  public static CompactionInfoStruct compactionInfoToStruct(CompactionInfo ci) {
    if (ci == null) {
      return null;
    }
    CompactionInfoStruct cr = new CompactionInfoStruct(ci.id, ci.dbname, ci.tableName, ci.type);
    cr.setPartitionname(ci.partName);
    cr.setRunas(ci.runAs);
    cr.setProperties(ci.properties);
    cr.setToomanyaborts(ci.tooManyAborts);
    cr.setHasoldabort(ci.hasOldAbort);
    cr.setStart(ci.start);
    cr.setState(Character.toString(ci.state));
    cr.setWorkerId(ci.workerId);
    cr.setHighestWriteId(ci.highestWriteId);
    cr.setErrorMessage(ci.errorMessage);
    cr.setEnqueueTime(ci.enqueueTime);
    cr.setRetryRetention(ci.retryRetention);
    cr.setPoolname(ci.poolName);
    cr.setNumberOfBuckets(ci.numberOfBuckets);
    cr.setOrderByClause(ci.orderByClause);
    return cr;
  }

  public static OptionalCompactionInfoStruct compactionInfoToOptionalStruct(CompactionInfo ci) {
    CompactionInfoStruct cis = compactionInfoToStruct(ci);
    OptionalCompactionInfoStruct ocis = new OptionalCompactionInfoStruct();
    if (cis != null) {
      ocis.setCi(cis);
    }
    return ocis;
  }
  public static CompactionInfo optionalCompactionInfoStructToInfo(OptionalCompactionInfoStruct ocis) {
    if (ocis.isSetCi()) {
      return compactionStructToInfo(ocis.getCi());
    }
    return null;
  }

  public void setWriteIds(boolean hasUncompactedAborts, Set<Long> writeIds) {
    this.hasUncompactedAborts = hasUncompactedAborts;
    this.writeIds = writeIds;
  }

  public boolean isAbortedTxnCleanup() {
    return type == CompactionType.ABORT_TXN_CLEANUP;
  }
}
