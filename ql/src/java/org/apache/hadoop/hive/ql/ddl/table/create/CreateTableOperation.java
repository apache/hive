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

package org.apache.hadoop.hive.ql.ddl.table.create;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.SchemaInferenceUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Operation process of creating a table.
 */
public class CreateTableOperation extends DDLOperation<CreateTableDesc> {
  public CreateTableOperation(DDLOperationContext context, CreateTableDesc desc) {
    super(context, desc);
  }

  // Sets the tables columns using the FieldSchema inferred from the SerDe's SchemaInference
  // implementation. This is used by CREATE TABLE LIKE FILE.
  private void readSchemaFromFile() throws HiveException {
    String fileFormat = desc.getLikeFileFormat();
    String filePath = desc.getLikeFile();
    List<FieldSchema> fieldSchema = SchemaInferenceUtils.readSchemaFromFile(context.getConf(), fileFormat, filePath);
    LOG.debug("Inferred field schema for {} file {} was {}", fileFormat, filePath, fieldSchema);
    desc.setCols(fieldSchema);
  }

  @Override
  public int execute() throws HiveException {
    // check if schema is being inferred via LIKE FILE
    if (desc.getLikeFile() != null) {
      readSchemaFromFile();
    }

    // create the table
    Table tbl = desc.toTable(context.getConf());
    LOG.debug("creating table {} on {}", tbl.getFullyQualifiedName(), tbl.getDataLocation());

    boolean replDataLocationChanged = false;
    if (desc.getReplicationSpec().isInReplicationScope()) {
      // If in replication scope, we should check if the object we're looking at exists, and if so,
      // trigger replace-mode semantics.
      Table existingTable = context.getDb().getTable(tbl.getDbName(), tbl.getTableName(), false);
      if (existingTable != null) {
        Map<String, String> dbParams = context.getDb().getDatabase(existingTable.getDbName()).getParameters();
        if (desc.getReplicationSpec().allowEventReplacementInto(dbParams) || desc.getReplicationSpec()
            .isForceOverwrite()) {
          desc.setReplaceMode(true); // we replace existing table.
          // If location of an existing managed table is changed, then need to delete the old location if exists.
          // This scenario occurs when a managed table is converted into external table at source. In this case,
          // at target, the table data would be moved to different location under base directory for external tables.
          if (existingTable.getTableType().equals(TableType.MANAGED_TABLE)
                  && tbl.getTableType().equals(TableType.EXTERNAL_TABLE)
                  && (!existingTable.getDataLocation().equals(tbl.getDataLocation()))) {
            replDataLocationChanged = true;
          }
        } else {
          LOG.debug("DDLTask: Create Table is skipped as table {} is newer than update", desc.getDbTableName());
          return 0; // no replacement, the existing table state is newer than our update.
        }
      }
    }

    // create the table
    if (desc.getReplaceMode()) {
      createTableReplaceMode(tbl, replDataLocationChanged);
    } else {
      // Some HMS background tasks skip processing tables being replicated into. Set the
      // replication property while creating the table so that they can identify such tables right
      // from the beginning. Set it to 0, which is lesser than any eventId ever created. This will
      // soon be overwritten by an actual value.
      if (desc.getReplicationSpec().isInReplicationScope() &&
              !tbl.getParameters().containsKey(ReplConst.REPL_TARGET_TABLE_PROPERTY)) {
        tbl.getParameters().put(ReplConst.REPL_TARGET_TABLE_PROPERTY, "0");
      }
      createTableNonReplaceMode(tbl);
    }

    DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
    return 0;
  }

  private void createTableReplaceMode(Table tbl, boolean replDataLocationChanged) throws HiveException {
    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    Long writeId = 0L;
    EnvironmentContext environmentContext = null;
    if (replicationSpec != null && replicationSpec.isInReplicationScope()) {
      writeId = desc.getReplWriteId();

      // In case of replication statistics is obtained from the source, so do not update those
      // on replica.
      environmentContext = new EnvironmentContext();
      environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }

    // In replication flow, if table's data location is changed, then set the corresponding flag in
    // environment context to notify Metastore to update location of all partitions and delete old directory.
    if (replDataLocationChanged) {
      environmentContext = ReplUtils.setReplDataLocationChangedFlag(environmentContext);
    }

    // replace-mode creates are really alters using CreateTableDesc.
    context.getDb().alterTable(tbl.getCatName(), tbl.getDbName(), tbl.getTableName(), tbl, false, environmentContext,
        true, writeId);
  }

  private void createTableNonReplaceMode(Table tbl) throws HiveException {
    if (CollectionUtils.isNotEmpty(desc.getPrimaryKeys()) ||
        CollectionUtils.isNotEmpty(desc.getForeignKeys()) ||
        CollectionUtils.isNotEmpty(desc.getUniqueConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getNotNullConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getDefaultConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getCheckConstraints())) {
      context.getDb().createTable(tbl, desc.getIfNotExists(), desc.getPrimaryKeys(), desc.getForeignKeys(),
          desc.getUniqueConstraints(), desc.getNotNullConstraints(), desc.getDefaultConstraints(),
          desc.getCheckConstraints());
    } else {
      context.getDb().createTable(tbl, desc.getIfNotExists());
    }

    if (desc.isCTAS()) {
      Table createdTable = context.getDb().getTable(tbl.getDbName(), tbl.getTableName());
      DataContainer dc = new DataContainer(createdTable.getTTable());
      context.getQueryState().getLineageState().setLineage(createdTable.getPath(), dc, createdTable.getCols());

      // We did not create the table before moving the data files for a non-partitioned table i.e
      // we used load file instead of load table (see SemanticAnalyzer#getFileSinkPlan() for
      // more details). Thus could not add a write notification required for a transactional
      // table. Do that here, after we have created the table. Since this is a newly created
      // table, listing all the files in the directory and listing only the ones corresponding to
      // the given id doesn't have much difference.
      if (!createdTable.isPartitioned() && AcidUtils.isTransactionalTable(createdTable)) {
        org.apache.hadoop.hive.metastore.api.Table tTable = createdTable.getTTable();
        Path tabLocation = new Path(tTable.getSd().getLocation());
        List<FileStatus> newFilesList;
        try {
          newFilesList = HdfsUtils.listLocatedFileStatus(tabLocation.getFileSystem(context.getConf()), tabLocation, null, true);
        } catch (IOException e) {
          LOG.error("Error listing files", e);
          throw new HiveException(e);
        }
        context.getDb().addWriteNotificationLog(createdTable, null,
                newFilesList, tTable.getWriteId(), null);
      }
    }
  }

  public static boolean doesTableNeedLocation(Table tbl) {
    // TODO: If we are ok with breaking compatibility of existing 3rd party StorageHandlers,
    // this method could be moved to the HiveStorageHandler interface.
    boolean retval = true;
    if (tbl.getStorageHandler() != null) {
      // TODO: why doesn't this check class name rather than toString?
      String sh = tbl.getStorageHandler().toString();
      retval = !"org.apache.hadoop.hive.hbase.HBaseStorageHandler".equals(sh) &&
          !Constants.DRUID_HIVE_STORAGE_HANDLER_ID.equals(sh) &&
          !Constants.JDBC_HIVE_STORAGE_HANDLER_ID.equals(sh) &&
          !"org.apache.hadoop.hive.accumulo.AccumuloStorageHandler".equals(sh);
    }
    return retval;
  }

  public static void makeLocationQualified(Table table, HiveConf conf) throws HiveException {
    StorageDescriptor sd = table.getTTable().getSd();
    // If the table's location is currently unset, it is left unset, allowing the metastore to
    // fill in the table's location.
    // Note that the previous logic for some reason would make a special case if the DB was the
    // default database, and actually attempt to generate a  location.
    // This seems incorrect and uncessary, since the metastore is just as able to fill in the
    // default table location in the case of the default DB, as it is for non-default DBs.
    Path path = null;
    if (sd.isSetLocation()) {
      path = new Path(sd.getLocation());
    }
    if (path != null) {
      sd.setLocation(Utilities.getQualifiedPath(conf, path));
    }
  }
}
