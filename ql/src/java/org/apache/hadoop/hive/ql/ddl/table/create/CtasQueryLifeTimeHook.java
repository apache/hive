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

package org.apache.hadoop.hive.ql.ddl.table.create;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IF_PURGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;

public class CtasQueryLifeTimeHook implements QueryLifeTimeHook {

  private static final Logger LOG = LoggerFactory.getLogger(CtasQueryLifeTimeHook.class);

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx) {

  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {

  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {

  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    HiveConf conf = ctx.getHiveConf();
    PrivateHookContext privateHookContext = (PrivateHookContext) ctx.getHookContext();
    Context context = privateHookContext.getContext();

    if (hasError && HiveConf.getBoolVar(conf, HiveConf.ConfVars.TXN_CTAS_X_LOCK)) {
      Table table = context.getDestinationTable();
      if (table != null) {
        LOG.info("Performing cleanup as part of rollback: {}", table.getFullTableName().toString());
        try {
          TxnStore txnHandler = TxnUtils.getTxnStore(conf);
          CompactionRequest rqst = new CompactionRequest(
                  table.getDbName(), table.getTableName(), CompactionType.MAJOR);
          rqst.setRunas(TxnUtils.findUserToRunAs(table.getSd().getLocation(),
                  table.getTTable(), conf));

          rqst.putToProperties(META_TABLE_LOCATION, table.getSd().getLocation());
          rqst.putToProperties(IF_PURGE, Boolean.toString(true));
          txnHandler.submitForCleanup(rqst, table.getTTable().getWriteId(),
                  privateHookContext.getQueryState().getTxnManager().getCurrentTxnId());
        } catch (InterruptedException | IOException | MetaException e) {
          throw new RuntimeException("Not able to submit cleanup operation of directory written by CTAS due to: ", e);
        }
      }
    }
  }
}
