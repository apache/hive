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
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.ConditionalCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AddWriteIdsToMinHistoryCommand implements ParameterizedBatchCommand<Object[]>, ConditionalCommand {

  private static final String MIN_HISTORY_WRITE_ID_INSERT_QUERY = "INSERT INTO \"MIN_HISTORY_WRITE_ID\" (\"MH_TXNID\", " +
      "\"MH_DATABASE\", \"MH_TABLE\", \"MH_WRITEID\") VALUES (?, ?, ?, ?)";
  
  private final List<Object[]> params;

  public AddWriteIdsToMinHistoryCommand(long txnId, Map<String, Long> minOpenWriteIds) {
    this.params = new ArrayList<>();
    for (Map.Entry<String, Long> validWriteId : minOpenWriteIds.entrySet()) {
      String[] names = TxnUtils.getDbTableName(validWriteId.getKey());
      params.add(new Object[]{ txnId, names[0], names[1], validWriteId.getValue() });
    }
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return MIN_HISTORY_WRITE_ID_INSERT_QUERY;
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
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }

  @Override
  public boolean shouldBeUsed(DatabaseProduct databaseProduct) {
    return TxnHandler.ConfVars.useMinHistoryWriteId();
  }

  @Override
  public void onError(DatabaseProduct databaseProduct, Exception e) {
    if (databaseProduct.isTableNotExistsError(e)) {
      // If the table does not exists anymore, we disable the flag and start to work the new way
      // This enables to switch to the new functionality without a restart
      TxnHandler.ConfVars.setUseMinHistoryWriteId(false);
    }
  }
  
}
