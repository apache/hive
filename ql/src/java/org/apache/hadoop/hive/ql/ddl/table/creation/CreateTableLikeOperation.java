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

package org.apache.hadoop.hive.ql.ddl.table.creation;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.PartitionManagementTask;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * Operation process of creating a table like an existing one.
 */
public class CreateTableLikeOperation extends DDLOperation {
  private final CreateTableLikeDesc desc;

  public CreateTableLikeOperation(DDLOperationContext context, CreateTableLikeDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    // Get the existing table
    Table oldtbl = context.getDb().getTable(desc.getLikeTableName());
    Table tbl;
    if (oldtbl.getTableType() == TableType.VIRTUAL_VIEW || oldtbl.getTableType() == TableType.MATERIALIZED_VIEW) {
      tbl = createViewLikeTable(oldtbl);
    } else {
      tbl = createTableLikeTable(oldtbl);
    }

    // If location is specified - ensure that it is a full qualified name
    if (CreateTableOperation.doesTableNeedLocation(tbl)) {
      CreateTableOperation.makeLocationQualified(tbl, context.getConf());
    }

    if (desc.getLocation() == null && !tbl.isPartitioned() &&
        context.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      StatsSetupConst.setStatsStateForCreateTable(tbl.getTTable().getParameters(),
          MetaStoreUtils.getColumnNames(tbl.getCols()), StatsSetupConst.TRUE);
    }

    // create the table
    context.getDb().createTable(tbl, desc.getIfNotExists());
    DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
    return 0;
  }

  private Table createViewLikeTable(Table oldtbl) throws HiveException {
    Table tbl;
    String targetTableName = desc.getTableName();
    tbl = context.getDb().newTable(targetTableName);

    if (desc.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(desc.getTblProps());
    }

    tbl.setTableType(TableType.MANAGED_TABLE);

    if (desc.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
      // if the partition discovery tablproperty is already defined don't change it
      if (tbl.isPartitioned() && tbl.getProperty(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY) == null) {
        // partition discovery is on by default if it already doesn't exist
        tbl.setProperty(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
      }
    }

    tbl.setFields(oldtbl.getCols());
    tbl.setPartCols(oldtbl.getPartCols());

    if (desc.getDefaultSerName() == null) {
      LOG.info("Default to LazySimpleSerDe for table {}", targetTableName);
      tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    } else {
      // let's validate that the serde exists
      DDLUtils.validateSerDe(desc.getDefaultSerName(), context);
      tbl.setSerializationLib(desc.getDefaultSerName());
    }

    if (desc.getDefaultSerdeProps() != null) {
      for (Map.Entry<String, String> e : desc.getDefaultSerdeProps().entrySet()) {
        tbl.setSerdeParam(e.getKey(), e.getValue());
      }
    }

    tbl.setInputFormatClass(desc.getDefaultInputFormat());
    tbl.setOutputFormatClass(desc.getDefaultOutputFormat());
    tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
    tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());

    return tbl;
  }

  private Table createTableLikeTable(Table oldtbl) throws SemanticException, HiveException {
    Table tbl = oldtbl;

    // find out database name and table name of target table
    String targetTableName = desc.getTableName();
    String[] names = Utilities.getDbTableName(targetTableName);

    tbl.setDbName(names[0]);
    tbl.setTableName(names[1]);

    // using old table object, hence reset the owner to current user for new table.
    tbl.setOwner(SessionState.getUserFromAuthenticator());

    if (desc.getLocation() != null) {
      tbl.setDataLocation(new Path(desc.getLocation()));
    } else {
      tbl.unsetDataLocation();
    }

    Class<? extends Deserializer> serdeClass;
    try {
      serdeClass = oldtbl.getDeserializerClass();
    } catch (Exception e) {
      throw new HiveException(e);
    }
    // We should copy only those table parameters that are specified in the config.
    SerDeSpec spec = AnnotationUtils.getAnnotation(serdeClass, SerDeSpec.class);

    Set<String> retainer = new HashSet<String>();
    // for non-native table, property storage_handler should be retained
    retainer.add(META_TABLE_STORAGE);
    if (spec != null && spec.schemaProps() != null) {
      retainer.addAll(Arrays.asList(spec.schemaProps()));
    }

    String paramsStr = HiveConf.getVar(context.getConf(), HiveConf.ConfVars.DDL_CTL_PARAMETERS_WHITELIST);
    if (paramsStr != null) {
      retainer.addAll(Arrays.asList(paramsStr.split(",")));
    }

    Map<String, String> params = tbl.getParameters();
    if (!retainer.isEmpty()) {
      params.keySet().retainAll(retainer);
    } else {
      params.clear();
    }

    if (desc.getTblProps() != null) {
      params.putAll(desc.getTblProps());
    }

    if (desc.isUserStorageFormat()) {
      tbl.setInputFormatClass(desc.getDefaultInputFormat());
      tbl.setOutputFormatClass(desc.getDefaultOutputFormat());
      tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
      tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());
      if (desc.getDefaultSerName() == null) {
        LOG.info("Default to LazySimpleSerDe for like table {}", targetTableName);
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      } else {
        // let's validate that the serde exists
        DDLUtils.validateSerDe(desc.getDefaultSerName(), context);
        tbl.setSerializationLib(desc.getDefaultSerName());
      }
    }

    tbl.getTTable().setTemporary(desc.isTemporary());
    tbl.getTTable().unsetId();

    if (desc.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
      // if the partition discovery tablproperty is already defined don't change it
      if (tbl.isPartitioned() && tbl.getProperty(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY) == null) {
        // partition discovery is on by default if it already doesn't exist
        tbl.setProperty(PartitionManagementTask.DISCOVER_PARTITIONS_TBLPROPERTY, "true");
      }
    } else {
      tbl.getParameters().remove("EXTERNAL");
    }

    return tbl;
  }
}
