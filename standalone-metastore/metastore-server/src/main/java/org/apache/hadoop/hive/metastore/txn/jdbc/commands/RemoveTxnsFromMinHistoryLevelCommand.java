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
import org.apache.hadoop.hive.metastore.txn.jdbc.ConditionalCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.InClauseBatchCommand;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.List;

public class RemoveTxnsFromMinHistoryLevelCommand extends InClauseBatchCommand<Long> implements ConditionalCommand {

  public RemoveTxnsFromMinHistoryLevelCommand(List<Long> txnids) {
    super("DELETE FROM \"MIN_HISTORY_LEVEL\" WHERE \"MHL_TXNID\" IN (:txnIds)",
        new MapSqlParameterSource().addValue("txnIds", txnids), "txnIds", Long::compareTo);
  }

  @Override
  public boolean shouldBeUsed(DatabaseProduct databaseProduct) {
    return TxnHandler.ConfVars.useMinHistoryLevel();
  }

  @Override
  public void onError(DatabaseProduct databaseProduct, Exception e) {
    if (databaseProduct.isTableNotExistsError(e)) {
      TxnHandler.ConfVars.setUseMinHistoryLevel(false);
    }
  }  
  
}
