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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE_PATTERN;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IF_PURGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;

import java.util.Optional;

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
    QueryPlan queryPlan = ctx.getHookContext().getQueryPlan();
    boolean isCTAS = Optional.ofNullable(queryPlan.getQueryProperties())
        .map(queryProps -> queryProps.isCTAS()).orElse(false);

    PrivateHookContext pCtx = (PrivateHookContext) ctx.getHookContext();
    Path tblPath = pCtx.getContext().getLocation();

    if (isCTAS && tblPath != null) {
      try {
        FileSystem fs = tblPath.getFileSystem(conf);
        if (!fs.exists(tblPath)) {
          return;
        }
      } catch (Exception e) {
        throw new RuntimeException("Not able to check whether the CTAS table directory exists due to: ", e);
      }

      boolean isSoftDeleteEnabled = tblPath.getName().matches("(.*)" + SOFT_DELETE_TABLE_PATTERN);

      if ((HiveConf.getBoolVar(conf, HiveConf.ConfVars.TXN_CTAS_X_LOCK) || isSoftDeleteEnabled)
              && CollectionUtils.isNotEmpty(queryPlan.getAcidSinks())) {

        FileSinkDesc fileSinkDesc = queryPlan.getAcidSinks().iterator().next();
        Table table = fileSinkDesc.getTable();
        long writeId = fileSinkDesc.getTableWriteId();

        if (table != null) {
          LOG.info("Performing cleanup as part of rollback: {}", table.getFullTableName().toString());
          try {
            CompactionRequest request = new CompactionRequest(table.getDbName(), table.getTableName(), CompactionType.MAJOR);
            request.setRunas(TxnUtils.findUserToRunAs(tblPath.toString(), table.getTTable(), conf));
            request.putToProperties(META_TABLE_LOCATION, tblPath.toString());
            request.putToProperties(IF_PURGE, Boolean.toString(true));
            boolean success = Hive.get(conf).getMSC().submitForCleanup(request, writeId,
                    pCtx.getQueryState().getTxnManager().getCurrentTxnId());
            if (success) {
              LOG.info("The cleanup request has been submitted");
            }
          } catch (Exception e) {
            throw new RuntimeException("Not able to submit cleanup operation of directory written by CTAS due to: ", e);
          }
        }
      }
    }
  }
}
