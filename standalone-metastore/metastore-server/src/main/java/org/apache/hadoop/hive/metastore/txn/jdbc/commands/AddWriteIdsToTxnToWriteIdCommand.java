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
package org.apache.hadoop.hive.metastore.txn.jdbc.commands;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedBatchCommand;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class AddWriteIdsToTxnToWriteIdCommand implements ParameterizedBatchCommand<Object[]> {

  private final List<Object[]> params;

  public AddWriteIdsToTxnToWriteIdCommand(String dbName, String tableName, long writeId, List<Long> txnIds, List<TxnToWriteId> txnToWriteIds) {
    this.params = new ArrayList<>();
    for (long txnId : txnIds) {
      params.add(new Object[]{ txnId, dbName, tableName, writeId });
      txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
      writeId++;
    }
  }

  
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\",  \"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (?, ?, ?, ?)";
  }

  @Override
  public List<Object[]> getQueryParameters() {
    return params;
  }

  @Override
  public ParameterizedPreparedStatementSetter<Object[]> getPreparedStatementSetter() {
    return (ps, argument) -> {
      ps.setLong(1, (Long)argument[0]);
      ps.setString(2, argument[1].toString());
      ps.setString(3, argument[2].toString());
      ps.setLong(4, (Long)argument[3]);
    };
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return null;
  }
}
