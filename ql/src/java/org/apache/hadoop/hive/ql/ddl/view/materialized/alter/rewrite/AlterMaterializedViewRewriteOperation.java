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

package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rewrite;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

/**
 * Operation process of enabling/disabling materialized view rewrite.
 */
public class AlterMaterializedViewRewriteOperation extends DDLOperation<AlterMaterializedViewRewriteDesc> {
  public AlterMaterializedViewRewriteOperation(DDLOperationContext context, AlterMaterializedViewRewriteDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table mv = context.getDb().getTable(desc.getMaterializedViewName());
    if (mv.isRewriteEnabled() == desc.isRewriteEnable()) {
      // This is a noop, return successfully
      return 0;
    }
    Table newMV = mv.copy(); // Do not mess with Table instance

    if (desc.isRewriteEnable()) {
      try {
        QueryState qs = new QueryState.Builder().withHiveConf(context.getConf()).build();
        CalcitePlanner planner = new CalcitePlanner(qs);
        Context ctx = new Context(context.getConf());
        ctx.setIsLoadingMaterializedView(true);
        planner.initCtx(ctx);
        planner.init(false);

        RelNode plan = planner.genLogicalPlan(ParseUtils.parse(newMV.getViewExpandedText()));
        if (plan == null) {
          String msg = "Cannot enable automatic rewriting for materialized view.";
          if (ctx.getCboInfo() != null) {
            msg += " " + ctx.getCboInfo();
          }
          throw new HiveException(msg);
        }
        if (!planner.isValidAutomaticRewritingMaterialization()) {
          throw new HiveException("Cannot enable rewriting for materialized view. " +
              planner.getInvalidAutomaticRewritingMaterializationReason());
        }
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }

    newMV.setRewriteEnabled(desc.isRewriteEnable());
    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    context.getDb().alterTable(newMV, false, environmentContext, true);

    return 0;
  }
}
