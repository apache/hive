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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class HiveQueryLifeTimeHook implements QueryLifeTimeHook {

  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryLifeTimeHook.class);

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
    if (hasError) {
      checkAndRollbackCTAS(ctx);
    }
  }

  private void checkAndRollbackCTAS(QueryLifeTimeHookContext ctx) {
    HiveConf conf = ctx.getHiveConf();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.TXN_CTAS_X_LOCK)) {
      PrivateHookContext pCtx = (PrivateHookContext) ctx.getHookContext();
      Table table = pCtx.getContext().getDestinationTable();
      if (table != null) {
        LOG.info("Performing cleanup as part of rollback: {}", table.getFullTableName().toString());
        try {
          boolean success = Hive.get(conf).getMSC().submitForCleanup(table.getDbName(), table.getTableName(),
                  CompactionType.MAJOR, table.getDataLocation().toString(),
                  TxnUtils.findUserToRunAs(table.getDataLocation().toString(), table.getTTable(), conf),
                  table.getTTable().getWriteId(),
                  pCtx.getQueryState().getTxnManager().getCurrentTxnId());
          if (success) {
            LOG.info("The cleanup request has been submitted");
          }
        } catch (HiveException | IOException | InterruptedException | TException e) {
          throw new RuntimeException("Not able to submit cleanup operation of directory written by CTAS due to: ", e);
        }
      }
    }
  }
}
