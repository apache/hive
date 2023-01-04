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

package org.apache.hadoop.hive.ql.ddl.view.create;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.metastore.Warehouse;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.io.AcidUtils;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils.getSnapshotOf;

/**
 * Operation process of creating a view.
 */
public class CreateMaterializedViewOperation extends DDLOperation<CreateMaterializedViewDesc> {
  public CreateMaterializedViewOperation(DDLOperationContext context, CreateMaterializedViewDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table oldview = context.getDb().getTable(desc.getViewName(), false);
    if (oldview != null) {

      if (desc.getIfNotExists()) {
        return 0;
      }

      // Materialized View already exists, thus we should be replacing
      throw new HiveException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(desc.getViewName()));
    } else {
      // We create new view
      Table tbl = desc.toTable(context.getConf());
      // We set the signature for the view if it is a materialized view
      if (tbl.isMaterializedView()) {
        Set<SourceTable> sourceTables = new HashSet<>(desc.getTablesUsed().size());
        for (TableName tableName : desc.getTablesUsed()) {
          sourceTables.add(context.getDb().getTable(tableName).createSourceTable());
        }
        MaterializedViewMetadata metadata = new MaterializedViewMetadata(
                MetaStoreUtils.getDefaultCatalog(context.getConf()),
                tbl.getDbName(),
                tbl.getTableName(),
                sourceTables,
                getSnapshotOf(context, desc.getTablesUsed()));
        tbl.setMaterializedViewMetadata(metadata);
      }
      context.getDb().createTable(tbl, desc.getIfNotExists());
      DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());

      //set lineage info
      DataContainer dc = new DataContainer(tbl.getTTable());
      Map<String, String> tblProps = tbl.getTTable().getParameters();
      Path tlocation = null;
      try {
        Warehouse wh = new Warehouse(context.getConf());
        tlocation = wh.getDefaultTablePath(context.getDb().getDatabase(tbl.getDbName()), tbl.getTableName(),
                tblProps == null || !AcidUtils.isTablePropertyTransactional(tblProps));
      } catch (MetaException e) {
        throw new HiveException(e);
      }

      context.getQueryState().getLineageState().setLineage(tlocation, dc, tbl.getCols());
    }
    return 0;
  }
}
