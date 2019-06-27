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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

import com.google.common.collect.Iterables;

/**
 * Operation process of dropping a table.
 */
public class DropTableOperation extends DDLOperation<DropTableDesc> {
  public DropTableOperation(DDLOperationContext context, DropTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table tbl = null;
    try {
      tbl = context.getDb().getTable(desc.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    // This is a true DROP TABLE
    if (tbl != null && desc.getValidationRequired()) {
      if (tbl.isView()) {
        if (!desc.getExpectView()) {
          if (desc.getIfExists()) {
            return 0;
          }
          if (desc.getExpectMaterializedView()) {
            throw new HiveException("Cannot drop a view with DROP MATERIALIZED VIEW");
          } else {
            throw new HiveException("Cannot drop a view with DROP TABLE");
          }
        }
      } else if (tbl.isMaterializedView()) {
        if (!desc.getExpectMaterializedView()) {
          if (desc.getIfExists()) {
            return 0;
          }
          if (desc.getExpectView()) {
            throw new HiveException("Cannot drop a materialized view with DROP VIEW");
          } else {
            throw new HiveException("Cannot drop a materialized view with DROP TABLE");
          }
        }
      } else {
        if (desc.getExpectView()) {
          if (desc.getIfExists()) {
            return 0;
          }
          throw new HiveException("Cannot drop a base table with DROP VIEW");
        } else if (desc.getExpectMaterializedView()) {
          if (desc.getIfExists()) {
            return 0;
          }
          throw new HiveException("Cannot drop a base table with DROP MATERIALIZED VIEW");
        }
      }
    }

    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    if (tbl != null && replicationSpec.isInReplicationScope()) {
      /**
       * DROP TABLE FOR REPLICATION behaves differently from DROP TABLE IF EXISTS - it more closely
       * matches a DROP TABLE IF OLDER THAN(x) semantic.
       *
       * Ideally, commands executed under the scope of replication need to be idempotent and resilient
       * to repeats. What can happen, sometimes, is that a drone processing a replication task can
       * have been abandoned for not returning in time, but still execute its task after a while,
       * which should not result in it mucking up data that has been impressed later on. So, for eg.,
       * if we create partition P1, followed by droppping it, followed by creating it yet again,
       * the replication of that drop should not drop the newer partition if it runs after the destination
       * object is already in the newer state.
       *
       * Thus, we check the replicationSpec.allowEventReplacementInto to determine whether or not we can
       * drop the object in question(will return false if object is newer than the event, true if not)
       *
       * In addition, since DROP TABLE FOR REPLICATION can result in a table not being dropped, while DROP
       * TABLE will always drop the table, and the included partitions, DROP TABLE FOR REPLICATION must
       * do one more thing - if it does not drop the table because the table is in a newer state, it must
       * drop the partitions inside it that are older than this event. To wit, DROP TABLE FOR REPL
       * acts like a recursive DROP TABLE IF OLDER.
       */
      if (!replicationSpec.allowEventReplacementInto(tbl.getParameters())) {
        // Drop occured as part of replicating a drop, but the destination
        // table was newer than the event being replicated. Ignore, but drop
        // any partitions inside that are older.
        if (tbl.isPartitioned()) {
          PartitionIterable partitions = new PartitionIterable(context.getDb(), tbl, null,
              context.getConf().getIntVar(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
          for (Partition p : Iterables.filter(partitions, replicationSpec.allowEventReplacementInto())){
            context.getDb().dropPartition(tbl.getDbName(), tbl.getTableName(), p.getValues(), true);
          }
        }
        LOG.debug("DDLTask: Drop Table is skipped as table {} is newer than update", desc.getTableName());
        return 0; // table is newer, leave it be.
      }
    }

    // drop the table
    // TODO: API w/catalog name
    context.getDb().dropTable(desc.getTableName(), desc.getIfPurge());
    if (tbl != null) {
      // Remove from cache if it is a materialized view
      if (tbl.isMaterializedView()) {
        HiveMaterializedViewsRegistry.get().dropMaterializedView(tbl);
      }
      // We have already locked the table in DDLSemanticAnalyzer, don't do it again here
      DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
    }

    return 0;
  }
}
