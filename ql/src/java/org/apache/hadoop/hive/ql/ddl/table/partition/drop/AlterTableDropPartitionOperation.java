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

package org.apache.hadoop.hive.ql.ddl.table.partition.drop;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.ProactiveEviction;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

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
    Table table = null;
    try {
      table = context.getDb().getTable(desc.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    if (replicationSpec.isInReplicationScope()) {
      dropPartitionForReplication(table, replicationSpec);
    } else {
      dropPartitions(false);
    }

    return 0;
  }

  private void dropPartitionForReplication(Table table, ReplicationSpec replicationSpec) throws HiveException {
    /**
     * ALTER TABLE DROP PARTITION ... FOR REPLICATION(x) behaves as a DROP PARTITION IF OLDER THAN x
     *
     * So, we check each partition that matches our DropTableDesc.getPartSpecs(), and drop it only
     * if it's older than the event that spawned this replicated request to drop partition
     */
    if (table == null) {
      // If table is missing, then partitions are also would've been dropped. Just no-op.
      return;
    }

    Map<String, String> dbParams = context.getDb().getDatabase(table.getDbName()).getParameters();
    for (AlterTableDropPartitionDesc.PartitionDesc partSpec : desc.getPartSpecs()) {
      List<Partition> partitions = new ArrayList<>();
      try {
        context.getDb().getPartitionsByExpr(table, partSpec.getPartSpec(), context.getConf(), partitions);
        // Check if that is a comeback from a checkpoint, if not call normal drop partition.
        boolean modifySinglePartition = false;
        for (Partition p : partitions) {
          if (p != null && !replicationSpec.allowEventReplacementInto(dbParams)) {
            modifySinglePartition = true;
            break;
          }
        }
        // If there is no partition which has an older state. we are in normal flow, we can go ahead with calling the
        // regular drop partition with all the partitions specified in one go.
        if (!modifySinglePartition) {
          LOG.info("Replication calling normal drop partitions for regular partition drops {}", partitions);
          dropPartitions(true);
        } else {
          for (Partition p : partitions) {
            if (replicationSpec.allowEventReplacementInto(dbParams)) {
              PartitionDropOptions options =
                  PartitionDropOptions.instance().deleteData(desc.getDeleteData())
                    .setWriteId(desc.getWriteId());
              context.getDb().dropPartition(table.getDbName(), table.getTableName(), p.getValues(), options);
            }
          }
        }
      } catch (NoSuchObjectException e) {
        // ignore NSOE because that means there's nothing to drop.
      } catch (Exception e) {
        throw new HiveException(e.getMessage(), e);
      }
    }
  }

  private void dropPartitions(boolean isRepl) throws HiveException {
    // ifExists is currently verified in AlterTableDropPartitionAnalyzer
    TableName tableName = HiveTableName.of(desc.getTableName());

    List<Pair<Integer, byte[]>> partitionExpressions = new ArrayList<>(desc.getPartSpecs().size());
    for (AlterTableDropPartitionDesc.PartitionDesc partSpec : desc.getPartSpecs()) {
      partitionExpressions.add(Pair.of(partSpec.getPrefixLength(),
          SerializationUtilities.serializeObjectWithTypeInformation(partSpec.getPartSpec())));
    }

    PartitionDropOptions options =
        PartitionDropOptions.instance().deleteData(desc.getDeleteData())
          .ifExists(true).purgeData(desc.getIfPurge());
    List<Partition> droppedPartitions = context.getDb().dropPartitions(tableName.getDb(), tableName.getTable(),
        partitionExpressions, options);

    if (isRepl) {
      LOG.info("Dropped {} partitions for replication.", droppedPartitions.size());
      // If replaying an event, we need not to bother about the further steps, we can return from here itself.
      return;
    }

    ProactiveEviction.Request.Builder llapEvictRequestBuilder = LlapHiveUtils.isLlapMode(context.getConf()) ?
        ProactiveEviction.Request.Builder.create() : null;

    for (Partition partition : droppedPartitions) {
      context.getConsole().printInfo("Dropped the partition " + partition.getName());
      // We have already locked the table, don't lock the partitions.
      DDLUtils.addIfAbsentByName(new WriteEntity(partition, WriteEntity.WriteType.DDL_NO_LOCK), context);

      if (llapEvictRequestBuilder != null) {
        llapEvictRequestBuilder.addPartitionOfATable(tableName.getDb(), tableName.getTable(), partition.getSpec());
      }
    }

    if (llapEvictRequestBuilder != null) {
      ProactiveEviction.evict(context.getConf(), llapEvictRequestBuilder.build());
    }
  }
}
