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

import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import java.util.Collections;
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
  private ReplicationSpec replicationSpec;

  /**
   * OperationType.
   * Different kind of events supported for replaying.
   */
  public enum OperationType {
    REPL_OPEN_TXN, REPL_ABORT_TXN, REPL_COMMIT_TXN, REPL_ALLOC_WRITE_ID
  }

  OperationType operation;

  public ReplTxnWork(String replPolicy, String dbName, String tableName, List<Long> txnIds, OperationType type,
                     List<TxnToWriteId> txnToWriteIdList, ReplicationSpec replicationSpec) {
    this.txnIds = txnIds;
    this.dbName = dbName;
    this.tableName = tableName;
    this.operation = type;
    this.replPolicy = replPolicy;
    this.txnToWriteIdList = txnToWriteIdList;
    this.replicationSpec = replicationSpec;
  }

  public ReplTxnWork(String replPolicy, String dbName, String tableName, List<Long> txnIds, OperationType type,
                     ReplicationSpec replicationSpec) {
    this(replPolicy, dbName, tableName, txnIds, type, null, replicationSpec);
  }

  public ReplTxnWork(String replPolicy, String dbName, String tableName, Long txnId,
                     OperationType type, ReplicationSpec replicationSpec) {
    this(replPolicy, dbName, tableName, Collections.singletonList(txnId), type, null, replicationSpec);
  }

  public ReplTxnWork(String replPolicy, String dbName, String tableName, OperationType type,
                     List<TxnToWriteId> txnToWriteIdList, ReplicationSpec replicationSpec) {
    this(replPolicy, dbName, tableName, null, type, txnToWriteIdList, replicationSpec);
  }

  public List<Long> getTxnIds() {
    return txnIds;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName()  {
    return tableName;
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

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }
}
