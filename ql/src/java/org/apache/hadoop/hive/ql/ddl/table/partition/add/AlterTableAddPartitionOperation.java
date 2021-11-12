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

package org.apache.hadoop.hive.ql.ddl.table.partition.add;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Operation process of adding a partition to a table.
 */
public class AlterTableAddPartitionOperation extends DDLOperation<AlterTableAddPartitionDesc> {
  public AlterTableAddPartitionOperation(DDLOperationContext context, AlterTableAddPartitionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // TODO: catalog name everywhere in this method
    Table table = context.getDb().getTable(desc.getDbName(), desc.getTableName());
    long writeId = getWriteId(table);

    List<Partition> partitions = getPartitions(table, writeId);
    addPartitions(table, partitions, writeId);
    return 0;
  }

  private long getWriteId(Table table) throws LockException {
    // In case of replication, get the writeId from the source and use valid write Id list for replication.
    if (desc.getReplicationSpec().isInReplicationScope() && desc.getPartitions().get(0).getWriteId() > 0) {
      return desc.getPartitions().get(0).getWriteId();
    } else {
      AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(context.getConf(), table, true);
      if (tableSnapshot != null && tableSnapshot.getWriteId() > 0) {
        return tableSnapshot.getWriteId();
      } else {
        return -1;
      }
    }
  }

  private List<Partition> getPartitions(Table table, long writeId) throws HiveException {
    List<Partition> partitions = new ArrayList<>(desc.getPartitions().size());
    for (AlterTableAddPartitionDesc.PartitionDesc partitionDesc : desc.getPartitions()) {
      Partition partition = convertPartitionSpecToMetaPartition(table, partitionDesc);
      if (partition != null && writeId > 0) {
        partition.setWriteId(writeId);
      }
      partitions.add(partition);
    }

    return partitions;
  }

  private Partition convertPartitionSpecToMetaPartition(Table table,
      AlterTableAddPartitionDesc.PartitionDesc partitionSpec) throws HiveException {
    Path location = partitionSpec.getLocation() != null ? new Path(table.getPath(), partitionSpec.getLocation()) : null;
    if (location != null) {
      // Ensure that it is a full qualified path (in most cases it will be since tbl.getPath() is full qualified)
      location = new Path(Utilities.getQualifiedPath(context.getConf(), location));
    }

    Partition partition = org.apache.hadoop.hive.ql.metadata.Partition.createMetaPartitionObject(
        table, partitionSpec.getPartSpec(), location);

    if (partitionSpec.getPartParams() != null) {
      partition.setParameters(partitionSpec.getPartParams());
    }
    if (partitionSpec.getInputFormat() != null) {
      partition.getSd().setInputFormat(partitionSpec.getInputFormat());
    }
    if (partitionSpec.getOutputFormat() != null) {
      partition.getSd().setOutputFormat(partitionSpec.getOutputFormat());
    }
    if (partitionSpec.getNumBuckets() != -1) {
      partition.getSd().setNumBuckets(partitionSpec.getNumBuckets());
    }
    if (partitionSpec.getCols() != null) {
      partition.getSd().setCols(partitionSpec.getCols());
    }
    if (partitionSpec.getSerializationLib() != null) {
      partition.getSd().getSerdeInfo().setSerializationLib(partitionSpec.getSerializationLib());
    }
    if (partitionSpec.getSerdeParams() != null) {
      partition.getSd().getSerdeInfo().setParameters(partitionSpec.getSerdeParams());
    }
    if (partitionSpec.getBucketCols() != null) {
      partition.getSd().setBucketCols(partitionSpec.getBucketCols());
    }
    if (partitionSpec.getSortCols() != null) {
      partition.getSd().setSortCols(partitionSpec.getSortCols());
    }
    if (partitionSpec.getColStats() != null) {
      partition.setColStats(partitionSpec.getColStats());

      ColumnStatistics statistics = partition.getColStats();
      if (statistics != null && statistics.getEngine() == null) {
        statistics.setEngine(org.apache.hadoop.hive.conf.Constants.HIVE_ENGINE);
      }
      // Statistics will have an associated write Id for a transactional table. We need it to update column statistics.
      partition.setWriteId(partitionSpec.getWriteId());
    }
    return partition;
  }

  private void addPartitions(Table table, List<Partition> partitions, long writeId) throws HiveException {
    List<org.apache.hadoop.hive.ql.metadata.Partition> outPartitions = null;
    if (!desc.getReplicationSpec().isInReplicationScope()) {
      outPartitions = addPartitionsNoReplication(table, partitions);
    } else {
      outPartitions = addPartitionsWithReplication(table, partitions, writeId);
    }

    for (org.apache.hadoop.hive.ql.metadata.Partition outPartition : outPartitions) {
      DDLUtils.addIfAbsentByName(new WriteEntity(outPartition, WriteEntity.WriteType.INSERT), context);
    }
  }

  private List<org.apache.hadoop.hive.ql.metadata.Partition> addPartitionsNoReplication(Table table,
      List<Partition> partitions) throws HiveException {
    // TODO: normally, the result is not necessary; might make sense to pass false
    List<org.apache.hadoop.hive.ql.metadata.Partition> outPartitions = new ArrayList<>();
    for (Partition outPart : context.getDb().addPartitions(partitions, desc.isIfNotExists(), true)) {
      outPartitions.add(new org.apache.hadoop.hive.ql.metadata.Partition(table, outPart));
    }
    return outPartitions;
  }

  private List<org.apache.hadoop.hive.ql.metadata.Partition> addPartitionsWithReplication(Table table,
      List<Partition> partitions, long writeId) throws HiveException {
    // For replication add-ptns, we need to follow a insert-if-not-exist, alter-if-exists scenario.
    // TODO : ideally, we should push this mechanism to the metastore, because, otherwise, we have
    // no choice but to iterate over the partitions here.

    List<Partition> partitionsToAdd = new ArrayList<>();
    List<Partition> partitionsToAlter = new ArrayList<>();
    List<String> partitionNames = new ArrayList<>();
    Map<String, String> dbParams = context.getDb().getDatabase(desc.getDbName()).getParameters();
    for (Partition partition : partitions) {
      partitionNames.add(getPartitionName(table, partition));
      try {
        Partition p = context.getDb().getPartition(table, desc.getDbName(), desc.getTableName(), partition.getValues());
        if (desc.getReplicationSpec().allowReplacementInto(dbParams)) {
          partitionsToAlter.add(partition);
        } // else ptn already exists, but we do nothing with it.
      } catch (HiveException e){
        if (e.getCause() instanceof NoSuchObjectException) {
          // if the object does not exist, we want to add it.
          partitionsToAdd.add(partition);
        } else {
          throw e;
        }
      }
    }

    List<org.apache.hadoop.hive.ql.metadata.Partition> outPartitions = new ArrayList<>();
    if (!partitionsToAdd.isEmpty()) {
      LOG.debug("Calling AddPartition for {}", partitionsToAdd);
      for (Partition outPartition : context.getDb()
          .addPartitions(partitionsToAdd, desc.isIfNotExists(), true)) {
        outPartitions.add(
            new org.apache.hadoop.hive.ql.metadata.Partition(table,
                outPartition));
      }
    }
    // In case of replication, statistics is obtained from the source, so do not update those on replica.
    if (!partitionsToAlter.isEmpty()) {
      LOG.debug("Calling AlterPartition for {}", partitionsToAlter);
      EnvironmentContext ec = new EnvironmentContext();
      ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS,
          StatsSetupConst.TRUE);
      String validWriteIdList = getValidWriteIdList(table, writeId);
      context.getDb().alterPartitions(desc.getDbName(), desc.getTableName(),
          partitionsToAlter, ec, validWriteIdList, writeId);

      for (Partition outPartition : context.getDb()
          .getPartitionsByNames(desc.getDbName(), desc.getTableName(),
              partitionNames, table)) {
        outPartitions.add(
            new org.apache.hadoop.hive.ql.metadata.Partition(table,
                outPartition));
      }
    }
    return outPartitions;
  }

  private String getPartitionName(Table table, Partition partition) throws HiveException {
    try {
      return Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  private String getValidWriteIdList(Table table, long writeId) throws LockException {
    if (desc.getReplicationSpec().isInReplicationScope() && desc.getPartitions().get(0).getWriteId() > 0) {
      // We need a valid writeId list for a transactional change. During replication we do not
      // have a valid writeId list which was used for this on the source. But we know for sure
      // that the writeId associated with it was valid then (otherwise the change would have
      // failed on the source). So use a valid transaction list with only that writeId.
      return new ValidReaderWriteIdList(TableName.getDbTable(table.getDbName(), table.getTableName()),
          new long[0], new BitSet(), writeId).writeToString();
    } else {
      AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(context.getConf(), table, true);
      if (tableSnapshot != null && tableSnapshot.getWriteId() > 0) {
        return tableSnapshot.getValidWriteIdList();
      } else {
        return null;
      }
    }
  }
}
