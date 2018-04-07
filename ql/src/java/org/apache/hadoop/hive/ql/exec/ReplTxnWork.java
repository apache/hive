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
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.util.List;

/**
 * ReplTxnTask.
 * Used for replaying the transaction related events.
 */
@Explain(displayName = "Replication Transaction", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ReplTxnWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dbName;
  private String tableName;
  private String replPolicy;
  private List<Long> txnIds;
  private List<TxnToWriteId> txnToWriteIdList;

  /**
   * OperationType.
   * Different kind of events supported for replaying.
   */
  public enum OperationType {
    REPL_OPEN_TXN, REPL_ABORT_TXN, REPL_COMMIT_TXN, REPL_ALLOC_WRITE_ID
  }

  OperationType operation;

  public ReplTxnWork(String dbName, String tableName, List<Long> txnIds, OperationType type,
                     List<TxnToWriteId> txnToWriteIdList) {
    this.txnIds = txnIds;
    this.dbName = dbName;
    this.tableName = tableName;
    this.operation = type;
    this.replPolicy = HiveUtils.getReplPolicy(dbName, tableName);
    this.txnToWriteIdList = txnToWriteIdList;
  }

  public ReplTxnWork(String dbName, String tableName, List<Long> txnIds, OperationType type) {
    this(dbName, tableName, txnIds, type, null);
  }

  public ReplTxnWork(String dbName, String tableName, Long txnId, OperationType type) {
    this(dbName, tableName, Lists.newArrayList(txnId), type, null);
  }

  public ReplTxnWork(String dbName, String tableName, OperationType type, List<TxnToWriteId> txnToWriteIdList) {
    this(dbName, tableName, null, type, txnToWriteIdList);
  }

  public List<Long> getTxnIds() {
    return txnIds;
  }

  public Long getTxnId(int idx) {
    return txnIds.get(idx);
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName()  {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getReplPolicy()  {
    return replPolicy;
  }

  public OperationType getOperationType() {
    return operation;
  }

  public List<TxnToWriteId> getTxnToWriteIdList() {
    return txnToWriteIdList;
  }

  public void setTxnToWriteIdList(List<TxnToWriteId> txnToWriteIdList) {
    this.txnToWriteIdList = txnToWriteIdList;
  }
}
