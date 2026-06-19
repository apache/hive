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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

public class DeleteReplTxnMapEntryCommand implements ParameterizedCommand {
    
  private final long sourceTxnId;
  private final String replicationPolicy;

  public DeleteReplTxnMapEntryCommand(long sourceTxnId, String replicationPolicy) {
    this.sourceTxnId = sourceTxnId;
    this.replicationPolicy = replicationPolicy;
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return null;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "DELETE FROM \"REPL_TXN_MAP\" WHERE \"RTM_SRC_TXN_ID\" = :sourceTxnId AND \"RTM_REPL_POLICY\" = :replPolicy";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("sourceTxnId", sourceTxnId)
        .addValue("replPolicy", replicationPolicy);
  }
}
