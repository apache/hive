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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.StorageFormat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Operation process of creating a view.
 */
public class CreateViewOperation extends DDLOperation<CreateViewDesc> {
  public CreateViewOperation(DDLOperationContext context, CreateViewDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (desc.isIcebergNativeView()) {
      return executeIcebergNativeView();
    }

    Table oldview = context.getDb().getTable(desc.getViewName(), false);
    if (oldview != null) {
      boolean isReplace = desc.isReplace();

      // Check whether we are replicating
      if (desc.getReplicationSpec().isInReplicationScope()) {
        Map<String, String> dbParams = context.getDb().getDatabase(oldview.getDbName()).getParameters();
        // if this is a replication spec, then replace-mode semantics might apply.
        if (desc.getReplicationSpec().allowEventReplacementInto(dbParams)) {
          isReplace = true; // we replace existing view.
        } else {
          LOG.debug("DDLTask: Create View is skipped as view {} is newer than update", desc.getViewName());
          return 0;
        }
      }

      if (!isReplace) {
        if (desc.getIfNotExists()) {
          return 0;
        }

        // View already exists, thus we should be replacing
        throw new HiveException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(desc.getViewName()));
      }

      // replace existing view
      // remove the existing partition columns from the field schema
      oldview.setViewOriginalText(desc.getOriginalText());
      oldview.setViewExpandedText(desc.getExpandedText());
      oldview.setFields(desc.getSchema());
      if (desc.getComment() != null) {
        oldview.setProperty("comment", desc.getComment());
      }
      if (desc.getProperties() != null) {
        oldview.getTTable().getParameters().putAll(desc.getProperties());
      }
      if (desc.isIcebergNativeView()) {
        oldview.setProperty(CreateViewDesc.ICEBERG_NATIVE_VIEW_PROPERTY, "true");
      } else {
        oldview.getParameters().remove(CreateViewDesc.ICEBERG_NATIVE_VIEW_PROPERTY);
      }
      oldview.setPartCols(desc.getPartitionColumns());

      oldview.checkValidity(null);
      if (desc.getOwnerName() != null) {
        oldview.setOwner(desc.getOwnerName());
      }
      context.getDb().alterTable(desc.getViewName(), oldview, false, null, true);
      DDLUtils.addIfAbsentByName(new WriteEntity(oldview, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());
    } else {
      // We create new view
      Table view = createViewObject();
      context.getDb().createTable(view, desc.getIfNotExists());
      DDLUtils.addIfAbsentByName(new WriteEntity(view, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());

      //set lineage info
      DataContainer dc = new DataContainer(view.getTTable());
      context.getQueryState().getLineageState().setLineage(new Path(desc.getViewName()), dc, view.getCols());
    }
    return 0;
  }

  /**
   * Creates a native Iceberg view via the configured default Iceberg catalog (HiveCatalog, REST
   * catalog with {@link org.apache.iceberg.view.ViewCatalog}, etc.).
   */
  private int executeIcebergNativeView() throws HiveException {
    Table oldview = context.getDb().getTable(desc.getViewName(), false);

    if (oldview != null) {
      boolean isReplace = desc.isReplace();

      if (desc.getReplicationSpec().isInReplicationScope()) {
        Map<String, String> dbParams = context.getDb().getDatabase(oldview.getDbName()).getParameters();
        if (desc.getReplicationSpec().allowEventReplacementInto(dbParams)) {
          isReplace = true;
        } else {
          LOG.debug("DDLTask: Create Iceberg native view is skipped as view {} is newer than update",
              desc.getViewName());
          return 0;
        }
      }

      if (!isReplace) {
        if (desc.getIfNotExists()) {
          return 0;
        }
        throw new HiveException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(desc.getViewName()));
      }
    }

    TableName name = HiveTableName.of(desc.getViewName());
    boolean replace = oldview != null || desc.isReplace();

    try {
      boolean created = invokeNativeIcebergViewSupport(
          name.getDb(),
          name.getTable(),
          replace,
          desc.getIfNotExists());
      if (!created) {
        return 0;
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    Table viewTbl = context.getDb().getTable(desc.getViewName(), false);
    if (viewTbl != null) {
      DDLUtils.addIfAbsentByName(new WriteEntity(viewTbl, WriteEntity.WriteType.DDL_NO_LOCK),
          context.getWork().getOutputs());
      DataContainer dc = new DataContainer(viewTbl.getTTable());
      context.getQueryState().getLineageState().setLineage(new Path(desc.getViewName()), dc, viewTbl.getCols());
    }
    return 0;
  }

  /**
   * Delegates to {@code org.apache.iceberg.hive.NativeIcebergViewSupport} when the Iceberg catalog
   * module is on the classpath (avoids a Maven reactor cycle between {@code hive-exec} and
   * {@code hive-iceberg-catalog}).
   */
  private boolean invokeNativeIcebergViewSupport(
      String databaseName, String viewName, boolean replace, boolean ifNotExists)
      throws Exception {
    Class<?> supportClass;
    Method method;
    try {
      supportClass = Class.forName("org.apache.iceberg.hive.NativeIcebergViewSupport");
      method =
          supportClass.getMethod(
              "createOrReplaceNativeView",
              org.apache.hadoop.conf.Configuration.class,
              String.class,
              String.class,
              java.util.List.class,
              String.class,
              Map.class,
              String.class,
              boolean.class,
              boolean.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new HiveException(
          "Native Iceberg views require hive-iceberg-catalog on the classpath "
              + "(org.apache.iceberg.hive.NativeIcebergViewSupport is missing).",
          e);
    }
    try {
      return (Boolean)
          method.invoke(
              null,
              context.getConf(),
              databaseName,
              viewName,
              desc.getSchema(),
              desc.getExpandedText(),
              desc.getProperties(),
              desc.getComment(),
              replace,
              ifNotExists);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      if (cause.getClass().getName().endsWith("AlreadyExistsException")) {
        throw new HiveException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(desc.getViewName()), cause);
      }
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new HiveException(cause);
    }
  }

  private Table createViewObject() throws HiveException {
    TableName name = HiveTableName.of(desc.getViewName());
    Table view = new Table(name.getDb(), name.getTable());
    view.setViewOriginalText(desc.getOriginalText());
    view.setViewExpandedText(desc.getExpandedText());
    view.setTableType(TableType.VIRTUAL_VIEW);
    view.setSerializationLib(null);
    view.clearSerDeInfo();
    view.setFields(desc.getSchema());
    if (desc.getComment() != null) {
      view.setProperty("comment", desc.getComment());
    }

    if (desc.getProperties() != null) {
      view.getParameters().putAll(desc.getProperties());
    }
    if (desc.isIcebergNativeView()) {
      view.setProperty(CreateViewDesc.ICEBERG_NATIVE_VIEW_PROPERTY, "true");
    }

    if (!CollectionUtils.isEmpty(desc.getPartitionColumns())) {
      view.setPartCols(desc.getPartitionColumns());
    }

    StorageFormat storageFormat = new StorageFormat(context.getConf());
    storageFormat.fillDefaultStorageFormat(false, false);

    view.setInputFormatClass(storageFormat.getInputFormat());
    view.setOutputFormatClass(storageFormat.getOutputFormat());

    if (desc.getOwnerName() != null) {
      view.setOwner(desc.getOwnerName());
    }

    // Sets the column state for the create view statement (false since it is a creation).
    // Similar to logic in CreateTableDesc.
    StatsSetupConst.setStatsStateForCreateTable(view.getTTable().getParameters(), null,
        StatsSetupConst.FALSE);

    return view;
  }
}
