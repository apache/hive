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

package org.apache.hadoop.hive.ql.ddl.view;

import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;

import com.google.common.collect.ImmutableSet;

/**
 * Operation process of updating a materialized view.
 */
public class MaterializedViewUpdateOperation extends DDLOperation<MaterializedViewUpdateDesc> {
  public MaterializedViewUpdateOperation(DDLOperationContext context, MaterializedViewUpdateDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (context.getDriverContext().getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }

    try {
      if (desc.isRetrieveAndInclude()) {
        Table mvTable = context.getDb().getTable(desc.getName());
        HiveMaterializedViewsRegistry.get().createMaterializedView(context.getDb().getConf(), mvTable);
      } else if (desc.isDisableRewrite()) {
        // Disabling rewriting, removing from cache
        String[] names = desc.getName().split("\\.");
        HiveMaterializedViewsRegistry.get().dropMaterializedView(names[0], names[1]);
      } else if (desc.isUpdateCreationMetadata()) {
        // We need to update the status of the creation signature
        Table mvTable = context.getDb().getTable(desc.getName());
        CreationMetadata cm = new CreationMetadata(MetaStoreUtils.getDefaultCatalog(context.getConf()),
            mvTable.getDbName(), mvTable.getTableName(),
            ImmutableSet.copyOf(mvTable.getCreationMetadata().getTablesUsed()));
        cm.setValidTxnList(context.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
        context.getDb().updateCreationMetadata(mvTable.getDbName(), mvTable.getTableName(), cm);
        mvTable.setCreationMetadata(cm);
        HiveMaterializedViewsRegistry.get().createMaterializedView(context.getDb().getConf(), mvTable);
      }
    } catch (HiveException e) {
      LOG.debug("Exception during materialized view cache update", e);
      context.getTask().setException(e);
    }

    return 0;
  }
}
