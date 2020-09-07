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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.table.partition.add.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FSTableEvent implements TableEvent {
  private final Path fromPathMetadata;
  private final Path fromPathData;
  private final MetaData metadata;
  private final HiveConf hiveConf;

  FSTableEvent(HiveConf hiveConf, String metadataDir, String dataDir) {
    try {
      URI fromURI = EximUtil.getValidatedURI(hiveConf, PlanUtils.stripQuotes(metadataDir));
      fromPathMetadata = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());
      URI fromURIData = EximUtil.getValidatedURI(hiveConf, PlanUtils.stripQuotes(dataDir));
      fromPathData = new Path(fromURIData.getScheme(), fromURIData.getAuthority(), fromURIData.getPath());
      FileSystem fs = FileSystem.get(fromURI, hiveConf);
      metadata = EximUtil.readMetaData(fs, new Path(fromPathMetadata, EximUtil.METADATA_NAME));
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
    return fromPathMetadata;
  }

  @Override
  public Path dataPath() {
    return fromPathData;
  }

  public MetaData getMetaData() {
    return metadata;
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
      AlterTableAddPartitionDesc partsDesc = addPartitionDesc(fromPathMetadata, tblDesc, partition);
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

  private AlterTableAddPartitionDesc addPartitionDesc(Path fromPath, ImportTableDesc tblDesc, Partition partition)
      throws SemanticException {
    try {
      Map<String, String> partitionSpec = EximUtil.makePartSpec(tblDesc.getPartCols(), partition.getValues());

      StorageDescriptor sd = partition.getSd();
      String location = sd.getLocation();
      if (!tblDesc.isExternal()) {
        /**
         * this is required for file listing of all files in a partition for managed table as described in
         * {@link org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator}
         */
        location = new Path(fromPath, Warehouse.makePartName(tblDesc.getPartCols(), partition.getValues())).toString();
      }

      ColumnStatistics columnStatistics = null;
      long writeId = -1;
      if (partition.isSetColStats()) {
        ColumnStatistics colStats = partition.getColStats();
        ColumnStatisticsDesc colStatsDesc = new ColumnStatisticsDesc(colStats.getStatsDesc());
        colStatsDesc.setTableName(tblDesc.getTableName());
        colStatsDesc.setDbName(tblDesc.getDatabaseName());
        columnStatistics = new ColumnStatistics(colStatsDesc, colStats.getStatsObj());
        columnStatistics.setEngine(colStats.getEngine());
        writeId = partition.getWriteId();
      }

      AlterTableAddPartitionDesc.PartitionDesc partitionDesc = new AlterTableAddPartitionDesc.PartitionDesc(
          partitionSpec, location, partition.getParameters(), sd.getInputFormat(), sd.getOutputFormat(),
          sd.getNumBuckets(), sd.getCols(), sd.getSerdeInfo().getSerializationLib(), sd.getSerdeInfo().getParameters(),
          sd.getBucketCols(), sd.getSortCols(), columnStatistics, writeId);

      AlterTableAddPartitionDesc addPartitionDesc = new AlterTableAddPartitionDesc(tblDesc.getDatabaseName(),
          tblDesc.getTableName(), true, ImmutableList.of(partitionDesc));
      addPartitionDesc.setReplicationSpec(replicationSpec());
      return addPartitionDesc;
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
