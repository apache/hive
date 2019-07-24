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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.util.HiveStrictManagedMigration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.util.HiveStrictManagedMigration.getHiveUpdater;

public class FSTableEvent implements TableEvent {
  private final Path fromPath;
  private final MetaData metadata;
  private final HiveConf hiveConf;

  FSTableEvent(HiveConf hiveConf, String metadataDir) {
    try {
      URI fromURI = EximUtil.getValidatedURI(hiveConf, PlanUtils.stripQuotes(metadataDir));
      fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());
      FileSystem fs = FileSystem.get(fromURI, hiveConf);
      metadata = EximUtil.readMetaData(fs, new Path(fromPath, EximUtil.METADATA_NAME));
      this.hiveConf = hiveConf;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getDbName() {
    return metadata.getTable().getDbName();
  }
  public String getTableName() {
    return metadata.getTable().getTableName();
  }

  public boolean shouldNotReplicate() {
    ReplicationSpec spec = replicationSpec();
    return spec.isNoop() || !spec.isInReplicationScope();
  }

  @Override
  public Path metadataPath() {
    return fromPath;
  }

  /**
   * To determine if the tableDesc is for an external table,
   * use {@link ImportTableDesc#isExternal()}
   * and not {@link ImportTableDesc#tableType()} method.
   */
  @Override
  public ImportTableDesc tableDesc(String dbName) throws SemanticException {
    try {
      Table table = new Table(metadata.getTable());
      boolean externalTableOnSource = TableType.EXTERNAL_TABLE.equals(table.getTableType());
      // The table can be non acid in case of replication from 2.6 cluster.
      if (!AcidUtils.isTransactionalTable(table)
              && hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES)
              && (table.getTableType() == TableType.MANAGED_TABLE)) {
        Hive hiveDb = Hive.get(hiveConf);
        //TODO : dump metadata should be read to make sure that migration is required.
        HiveStrictManagedMigration.TableMigrationOption migrationOption =
            HiveStrictManagedMigration.determineMigrationTypeAutomatically(table.getTTable(),
                table.getTableType(), null, hiveConf,
                hiveDb.getMSC(), true);
        HiveStrictManagedMigration.migrateTable(table.getTTable(), table.getTableType(),
                migrationOption, false,
                getHiveUpdater(hiveConf), hiveDb.getMSC(), hiveConf);
        // If the conversion is from non transactional to transactional table
        if (AcidUtils.isTransactionalTable(table)) {
          replicationSpec().setMigratingToTxnTable();
          // For migrated tables associate bootstrap writeId when replicating stats.
          if (table.getTTable().isSetColStats()) {
            table.getTTable().setWriteId(ReplUtils.REPL_BOOTSTRAP_MIGRATION_BASE_WRITE_ID);
          }
        }
        if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
          // since we have converted to an external table now after applying the migration rules the
          // table location has to be set to null so that the location on the target is picked up
          // based on default configuration
          table.setDataLocation(null);
          if(!externalTableOnSource) {
            replicationSpec().setMigratingToExternalTable();
          }
        }
      }
      ImportTableDesc tableDesc
              = new ImportTableDesc(StringUtils.isBlank(dbName) ? table.getDbName() : dbName, table);
      if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
        tableDesc.setLocation(
            table.getDataLocation() == null ? null : table.getDataLocation().toString());
        tableDesc.setExternal(true);
      }
      tableDesc.setReplicationSpec(replicationSpec());
      if (table.getOwner() != null) {
        tableDesc.setOwnerName(table.getOwner());
      }
      return tableDesc;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public List<AlterTableAddPartitionDesc> partitionDescriptions(ImportTableDesc tblDesc)
      throws SemanticException {
    List<AlterTableAddPartitionDesc> descs = new ArrayList<>();
    //TODO: if partitions are loaded lazily via the iterator then we will have to avoid conversion of everything here as it defeats the purpose.
    for (Partition partition : metadata.getPartitions()) {
      // TODO: this should ideally not create AddPartitionDesc per partition
      AlterTableAddPartitionDesc partsDesc = partitionDesc(fromPath, tblDesc, partition);
      descs.add(partsDesc);
    }
    return descs;
  }

  @Override
  public List<String> partitions(ImportTableDesc tblDesc)
          throws SemanticException {
    List<String> partitions = new ArrayList<>();
    try {
      for (Partition partition : metadata.getPartitions()) {
        String partName = Warehouse.makePartName(tblDesc.getPartCols(), partition.getValues());
        partitions.add(partName);
      }
    } catch (MetaException e) {
      throw new SemanticException(e);
    }
    return partitions;
  }

  private AlterTableAddPartitionDesc partitionDesc(Path fromPath,
      ImportTableDesc tblDesc, Partition partition) throws SemanticException {
    try {
      AlterTableAddPartitionDesc partsDesc =
          new AlterTableAddPartitionDesc(tblDesc.getDatabaseName(), tblDesc.getTableName(),
              EximUtil.makePartSpec(tblDesc.getPartCols(), partition.getValues()),
              partition.getSd().getLocation(), partition.getParameters());
      AlterTableAddPartitionDesc.PartitionDesc partDesc = partsDesc.getPartition(0);
      partDesc.setInputFormat(partition.getSd().getInputFormat());
      partDesc.setOutputFormat(partition.getSd().getOutputFormat());
      partDesc.setNumBuckets(partition.getSd().getNumBuckets());
      partDesc.setCols(partition.getSd().getCols());
      partDesc.setSerializationLib(partition.getSd().getSerdeInfo().getSerializationLib());
      partDesc.setSerdeParams(partition.getSd().getSerdeInfo().getParameters());
      partDesc.setBucketCols(partition.getSd().getBucketCols());
      partDesc.setSortCols(partition.getSd().getSortCols());
      if (tblDesc.isExternal() && !replicationSpec().isMigratingToExternalTable()) {
        // we have to provide the source location so target location can be derived.
        partDesc.setLocation(partition.getSd().getLocation());
      } else {
        /**
         * this is required for file listing of all files in a partition for managed table as described in
         * {@link org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator}
         */
        partDesc.setLocation(new Path(fromPath,
            Warehouse.makePartName(tblDesc.getPartCols(), partition.getValues())).toString());
      }
      partsDesc.setReplicationSpec(replicationSpec());

      if (partition.isSetColStats()) {
        ColumnStatistics colStats = partition.getColStats();
        ColumnStatisticsDesc colStatsDesc = new ColumnStatisticsDesc(colStats.getStatsDesc());
        colStatsDesc.setTableName(tblDesc.getTableName());
        colStatsDesc.setDbName(tblDesc.getDatabaseName());
        partDesc.setColStats(new ColumnStatistics(colStatsDesc, colStats.getStatsObj(), colStats.getEngine()));
        long writeId = replicationSpec().isMigratingToTxnTable() ?
                ReplUtils.REPL_BOOTSTRAP_MIGRATION_BASE_WRITE_ID : partition.getWriteId();
        partDesc.setWriteId(writeId);
      }
      return partsDesc;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public ReplicationSpec replicationSpec() {
    return metadata.getReplicationSpec();
  }

  @Override
  public EventType eventType() {
    return EventType.Table;
  }
}
