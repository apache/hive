/**
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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates the materialized view registry after changes.
 */
public class MaterializedViewRegistryUpdateHook implements QueryLifeTimeHook {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRegistryUpdateHook.class);

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
      return;
    }
    HiveConf hiveConf = ctx.getHiveConf();
    try {
      List<TaskRunner> completedTasks = ctx.getHookContext().getCompleteTaskList();
      for (TaskRunner taskRunner : completedTasks) {
        Task<? extends Serializable> task = taskRunner.getTask();
        if (task instanceof DDLTask) {
          DDLTask ddlTask = (DDLTask) task;
          DDLWork work = ddlTask.getWork();
          String tableName = null;
          boolean isRewriteEnabled = false;
          if (work.getCreateViewDesc() != null && work.getCreateViewDesc().isMaterialized()) {
            tableName = work.getCreateViewDesc().toTable(hiveConf).getFullyQualifiedName();
            isRewriteEnabled = work.getCreateViewDesc().isRewriteEnabled();
          } else if (work.getAlterMaterializedViewDesc() != null) {
            tableName = work.getAlterMaterializedViewDesc().getMaterializedViewName();
            isRewriteEnabled = work.getAlterMaterializedViewDesc().isRewriteEnable();
          } else {
            continue;
          }

          if (isRewriteEnabled) {
            Hive db = Hive.get();
            Table mvTable = db.getTable(tableName);
            HiveMaterializedViewsRegistry.get().createMaterializedView(db.getConf(), mvTable);
          } else if (work.getAlterMaterializedViewDesc() != null) {
            // Disabling rewriting, removing from cache
            String[] names =  tableName.split("\\.");
            HiveMaterializedViewsRegistry.get().dropMaterializedView(names[0], names[1]);
          }
        }
      }
    } catch (HiveException e) {
      if (HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_MATERIALIZED_VIEW_ENABLE_AUTO_REWRITING)) {
        String message = "Error updating materialized view cache; consider disabling: " + ConfVars.HIVE_MATERIALIZED_VIEW_ENABLE_AUTO_REWRITING.varname;
        LOG.error(message, e);
        throw new RuntimeException(message, e);
      } else {
        LOG.debug("Exception during materialized view cache update", e);
      }
    }
  }

}
