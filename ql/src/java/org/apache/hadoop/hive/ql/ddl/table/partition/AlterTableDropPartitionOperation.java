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

package org.apache.hadoop.hive.ql.ddl.table.partition;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

import com.google.common.collect.Iterables;

/**
 * Operation process of dropping some partitions of a table.
 */
public class AlterTableDropPartitionOperation extends DDLOperation<AlterTableDropPartitionDesc> {
  public AlterTableDropPartitionOperation(DDLOperationContext context, AlterTableDropPartitionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // We need to fetch the table before it is dropped so that it can be passed to post-execution hook
    Table tbl = null;
    try {
      tbl = context.getDb().getTable(desc.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    if (replicationSpec.isInReplicationScope()) {
      dropPartitionForReplication(tbl, replicationSpec);
    } else {
      dropPartitions();
    }

    return 0;
  }

  private void dropPartitionForReplication(Table tbl, ReplicationSpec replicationSpec) throws HiveException {
    /**
     * ALTER TABLE DROP PARTITION ... FOR REPLICATION(x) behaves as a DROP PARTITION IF OLDER THAN x
     *
     * So, we check each partition that matches our DropTableDesc.getPartSpecs(), and drop it only
     * if it's older than the event that spawned this replicated request to drop partition
     */
    // TODO: Current implementation of replication will result in DROP_PARTITION under replication
    // scope being called per-partition instead of multiple partitions. However, to be robust, we
    // must still handle the case of multiple partitions in case this assumption changes in the
    // future. However, if this assumption changes, we will not be very performant if we fetch
    // each partition one-by-one, and then decide on inspection whether or not this is a candidate
    // for dropping. Thus, we need a way to push this filter (replicationSpec.allowEventReplacementInto)
    // to the  metastore to allow it to do drop a partition or not, depending on a Predicate on the
    // parameter key values.

    if (tbl == null) {
      // If table is missing, then partitions are also would've been dropped. Just no-op.
      return;
    }

    for (AlterTableDropPartitionDesc.PartitionDesc partSpec : desc.getPartSpecs()){
      List<Partition> partitions = new ArrayList<>();
      try {
        context.getDb().getPartitionsByExpr(tbl, partSpec.getPartSpec(), context.getConf(), partitions);
        for (Partition p : Iterables.filter(partitions, replicationSpec.allowEventReplacementInto())) {
          context.getDb().dropPartition(tbl.getDbName(), tbl.getTableName(), p.getValues(), true);
        }
      } catch (NoSuchObjectException e){
        // ignore NSOE because that means there's nothing to drop.
      } catch (Exception e) {
        throw new HiveException(e.getMessage(), e);
      }
    }
  }

  private void dropPartitions() throws HiveException {
    // ifExists is currently verified in DDLSemanticAnalyzer
    List<Partition> droppedParts = context.getDb().dropPartitions(desc.getTableName(), desc.getPartSpecs(),
        PartitionDropOptions.instance().deleteData(true).ifExists(true).purgeData(desc.getIfPurge()));
    for (Partition partition : droppedParts) {
      context.getConsole().printInfo("Dropped the partition " + partition.getName());
      // We have already locked the table, don't lock the partitions.
      DDLUtils.addIfAbsentByName(new WriteEntity(partition, WriteEntity.WriteType.DDL_NO_LOCK), context);
    }
  }
}
