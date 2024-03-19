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

package org.apache.hadoop.hive.ql.ddl.table.create.like;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableOperation;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

/**
 * Operation process of creating a table like an existing one.
 */
public class CreateTableLikeOperation extends DDLOperation<CreateTableLikeDesc> {
  public CreateTableLikeOperation(DDLOperationContext context, CreateTableLikeDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // Get the existing table
    Table oldTable = context.getDb().getTable(desc.getLikeTableName());
    Table tbl;
    if (oldTable.getTableType() == TableType.VIRTUAL_VIEW || oldTable.getTableType() == TableType.MATERIALIZED_VIEW) {
      tbl = createViewLikeTable(oldTable);
    } else {
      Map<String, String> originalProperties = new HashMap<>();
      // Get the storage handler without caching, since the storage handler can get changed when copying the
      // properties of the target table and
      if (oldTable.getStorageHandlerWithoutCaching() != null) {
        originalProperties = new HashMap<>(oldTable.getStorageHandlerWithoutCaching().getNativeProperties(oldTable));
      }
      tbl = createTableLikeTable(oldTable, originalProperties);
    }

    // If location is specified - ensure that it is a full qualified name
    if (CreateTableOperation.doesTableNeedLocation(tbl)) {
      CreateTableOperation.makeLocationQualified(tbl, context.getConf());
    }

    if (desc.getLocation() == null && !tbl.isPartitioned() &&
        context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER)) {
      StatsSetupConst.setStatsStateForCreateTable(tbl.getTTable().getParameters(),
          MetaStoreUtils.getColumnNames(tbl.getCols()), StatsSetupConst.TRUE);
    }

    // create the table
    context.getDb().createTable(tbl, desc.getIfNotExists());
    DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
    return 0;
  }

  private Table createViewLikeTable(Table oldTable) throws HiveException {
    Table table = context.getDb().newTable(desc.getTableName());

    if (desc.getTblProps() != null) {
      table.getTTable().getParameters().putAll(desc.getTblProps());
    }

    table.setTableType(TableType.MANAGED_TABLE);

    if (desc.isExternal()) {
      setExternalProperties(table);
    }

    setUserSpecifiedLocation(table);

    table.setFields(oldTable.getCols());
    table.setPartCols(oldTable.getPartCols());

    if (desc.getDefaultSerdeProps() != null) {
      for (Map.Entry<String, String> e : desc.getDefaultSerdeProps().entrySet()) {
        table.setSerdeParam(e.getKey(), e.getValue());
      }
    }

    setStorage(table);

    return table;
  }

  private Table createTableLikeTable(Table table, Map<String, String> originalProperties)
      throws SemanticException, HiveException {
    String[] names = Utilities.getDbTableName(desc.getTableName());
    table.setDbName(names[0]);
    table.setTableName(names[1]);
    table.setOwner(SessionState.getUserFromAuthenticator());

    setUserSpecifiedLocation(table);

    setTableParameters(table, originalProperties);

    if (desc.isUserStorageFormat() || (table.getInputFormatClass() == null) || (table.getOutputFormatClass() == null)) {
      setStorage(table);
    }

    table.getTTable().setTemporary(desc.isTemporary());
    table.getTTable().unsetId();

    if (desc.isExternal()) {
      setExternalProperties(table);
    } else {
      table.getParameters().remove("EXTERNAL");
      table.setTableType(TableType.MANAGED_TABLE);
    }

    return table;
  }

  private void setUserSpecifiedLocation(Table table) {
    if (desc.getLocation() != null) {
      table.setDataLocation(new Path(desc.getLocation()));
    } else {
      table.unsetDataLocation();
    }
  }

  private void setTableParameters(Table tbl, Map<String, String> originalProperties) throws HiveException {
    // With Hive-25813, we'll not copy over table properties from the source.
    // CTLT should should copy column schema but not table properties. It is also consistent
    // with other query engines like mysql, redshift.
    originalProperties.putAll(tbl.getParameters());
    tbl.getParameters().clear();
    if (desc.getTblProps() != null) {
      tbl.setParameters(desc.getTblProps());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();
    if (storageHandler != null) {
      storageHandler.setTableParametersForCTLT(tbl, desc, originalProperties);
    }
  }

  private void setStorage(Table table) throws HiveException {
    table.setInputFormatClass(desc.getDefaultInputFormat());
    table.setOutputFormatClass(desc.getDefaultOutputFormat());
    table.getTTable().getSd().setInputFormat(table.getInputFormatClass().getName());
    table.getTTable().getSd().setOutputFormat(table.getOutputFormatClass().getName());
    if (table.getTTable().getSd().getSerdeInfo() != null &&
            table.getTTable().getSd().getSerdeInfo().getParameters() != null) {
      table.getTTable().getSd().getSerdeInfo().getParameters().clear();
    }
    if (desc.getDefaultSerName() == null) {
      LOG.info("Default to LazySimpleSerDe for table {}", desc.getTableName());
      table.setSerializationLib(LazySimpleSerDe.class.getName());
    } else {
      // let's validate that the serde exists
      DDLUtils.validateSerDe(desc.getDefaultSerName(), context);
      table.setSerializationLib(desc.getDefaultSerName());
    }
  }

  private void setExternalProperties(Table tbl) {
    tbl.setProperty("EXTERNAL", "TRUE");
    tbl.setTableType(TableType.EXTERNAL_TABLE);
  }
}
