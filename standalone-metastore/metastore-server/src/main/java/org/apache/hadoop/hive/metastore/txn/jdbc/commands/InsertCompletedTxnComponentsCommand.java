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
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.util.List;
import java.util.function.Function;

public class InsertCompletedTxnComponentsCommand implements ParameterizedBatchCommand<WriteEventInfo> {

  private final long txnId;
  private final char isUpdateDelete;
  private final List<WriteEventInfo> infos;

  public InsertCompletedTxnComponentsCommand(long txnId, char isUpdateDelete, List<WriteEventInfo> infos) {
    this.txnId = txnId;
    this.isUpdateDelete = isUpdateDelete;
    this.infos = infos;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return
        "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" " +
        "(\"CTC_TXNID\", \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\") " +
        "VALUES (?, ?, ?, ?, ?, ?)";
  }

  @Override
  public List<WriteEventInfo> getQueryParameters() {
    return infos;
  }

  @Override
  public ParameterizedPreparedStatementSetter<WriteEventInfo> getPreparedStatementSetter() {
    return (ps, argument) -> {
      ps.setLong(1, txnId);
      ps.setString(2, argument.getDatabase());
      ps.setString(3, argument.getTable());
      ps.setString(4, argument.getPartition());
      ps.setLong(5, argument.getWriteId());
      ps.setString(6, Character.toString(isUpdateDelete));
    };
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }
}
