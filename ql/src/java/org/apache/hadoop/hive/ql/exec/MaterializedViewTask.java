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

package org.apache.hadoop.hive.ql.exec;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;

/**
 * This task does some work related to materialized views. In particular, it adds
 * or removes the materialized view from the registry if needed, or registers new
 * creation metadata.
 */
public class MaterializedViewTask extends Task<MaterializedViewDesc> implements Serializable {

  private static final long serialVersionUID = 1L;

  public MaterializedViewTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    try {
      if (getWork().isRetrieveAndInclude()) {
        Hive db = Hive.get(conf);
        Table mvTable = db.getTable(getWork().getViewName());
        HiveMaterializedViewsRegistry.get().createMaterializedView(db.getConf(), mvTable);
      } else if (getWork().isDisableRewrite()) {
        // Disabling rewriting, removing from cache
        String[] names = getWork().getViewName().split("\\.");
        HiveMaterializedViewsRegistry.get().dropMaterializedView(names[0], names[1]);
      } else if (getWork().isUpdateCreationMetadata()) {
        // We need to update the status of the creation signature
        Hive db = Hive.get(conf);
        Table mvTable = db.getTable(getWork().getViewName());
        CreationMetadata cm =
            new CreationMetadata(mvTable.getDbName(), mvTable.getTableName(),
                ImmutableSet.copyOf(mvTable.getCreationMetadata().getTablesUsed()));
        cm.setValidTxnList(conf.get(ValidTxnList.VALID_TXNS_KEY));
        db.updateCreationMetadata(mvTable.getDbName(), mvTable.getTableName(), cm);
      }
    } catch (HiveException e) {
      LOG.debug("Exception during materialized view cache update", e);
    }
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return MaterializedViewTask.class.getSimpleName();
  }
}
