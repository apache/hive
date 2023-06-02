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
package org.apache.hadoop.hive.metastore.txn.impl.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.txn.TxnHandlingFeatures;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.BatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ConditionalCommand;

import java.util.ArrayList;
import java.util.List;

public class RemoveWriteIdsFromMinHistoryCommand implements BatchCommand, ConditionalCommand {

  private final Configuration conf;
  private final List<Long> txnids;

  public RemoveWriteIdsFromMinHistoryCommand(Configuration conf, List<Long> txnids) {
    this.conf = conf;
    this.txnids = txnids;
  }

  @Override
  public List<String> getQueryStrings(DatabaseProduct databaseProduct) {
    List<String> queries = new ArrayList<>();
    TxnUtils.buildQueryWithINClause(conf, queries, new StringBuilder("DELETE FROM \"MIN_HISTORY_WRITE_ID\" WHERE "), 
        new StringBuilder(), txnids, "\"MH_TXNID\"", false, false);
    return queries;
  }

  @Override
  public boolean shouldUse(DatabaseProduct databaseProduct) {
    return TxnHandlingFeatures.useMinHistoryWriteId();
  }

  @Override
  public void onError(DatabaseProduct databaseProduct, Exception e) {
    if (databaseProduct.isTableNotExistsError(e)) {
      // If the table does not exists anymore, we disable the flag and start to work the new way
      // This enables to switch to the new functionality without a restart
      TxnHandlingFeatures.setUseMinHistoryWriteId(false);
    }
  }

}
