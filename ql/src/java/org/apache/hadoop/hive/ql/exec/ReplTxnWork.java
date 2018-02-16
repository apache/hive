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
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

@Explain(displayName = "Replication Transaction", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ReplTxnWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String dbName;
  private String tableName;
  private List<Long> txnIds;

  public enum OperationType {
    REPL_OPEN_TXN("REPL_OPEN_TXN"),
    REPL_ABORT_TXN("REPL_ABORT_TXN"),
    REPL_COMMI_TXN("REPL_COMMI_TXN");

    String type = null;
    OperationType(String type) {
      this.type = type;
    }

    @Override
    public String toString(){
      return type;
    }
  }

  OperationType operation;

  public ReplTxnWork(String dbName, String tableName, Iterator<Long> txnIdsItr, OperationType type) {
    this.txnIds = Lists.newArrayList(txnIdsItr);
    this.dbName = dbName;
    this.tableName = tableName;
    this.operation = type;
  }

  public ReplTxnWork(String dbName, String tableName, Long txnId, OperationType type) {
    this.txnIds = Lists.newArrayList();
    this.txnIds.add(txnId);
    this.dbName = dbName;
    this.tableName = tableName;
    this.operation = type;
  }

  public Iterator<Long> getTxnIds() {
    return txnIds.iterator();
  }

  public Long getTxnId(int idx) {
    return txnIds.get(idx);
  }

  public int getNumTxns() {
    return txnIds.size();
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName()  {
    return tableName;
  }

  public String getReplPolicy() {
    if (dbName == null) {
      return null;
    } else if (tableName == null) {
      return dbName.toLowerCase() + ".*";
    } else {
      return dbName.toLowerCase() + "." + tableName.toLowerCase();
    }
  }

  public OperationType getOperationType() {
    return operation;
  }
}
