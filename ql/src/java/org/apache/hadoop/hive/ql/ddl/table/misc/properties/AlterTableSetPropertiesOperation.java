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

package org.apache.hadoop.hive.ql.ddl.table.misc.properties;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;

import com.google.common.collect.Lists;

import static org.apache.hadoop.hive.metastore.TransactionalValidationListener.DEFAULT_TRANSACTIONAL_PROPERTY;

/**
 * Operation process of setting properties of a table.
 */
public class AlterTableSetPropertiesOperation extends AbstractAlterTableOperation<AlterTableSetPropertiesDesc> {
  public AlterTableSetPropertiesOperation(DDLOperationContext context, AlterTableSetPropertiesDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    if (StatsSetupConst.USER.equals(environmentContext.getProperties().get(StatsSetupConst.STATS_GENERATED))) {
      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
    }

    if (partition != null) {
      partition.getTPartition().getParameters().putAll(desc.getProps());
    } else {
      boolean isFromMmTable = AcidUtils.isInsertOnlyTable(table.getParameters());
      Boolean isToMmTable = AcidUtils.isToInsertOnlyTable(table, desc.getProps());
      boolean isToFullAcid = AcidUtils.isToFullAcid(table, desc.getProps());
      if (!isFromMmTable && BooleanUtils.isTrue(isToMmTable)) {
        if (!HiveConf.getBoolVar(context.getConf(), ConfVars.HIVE_MM_ALLOW_ORIGINALS)) {
          List<Task<?>> mmTasks = generateAddMmTasks(table, desc.getWriteId());
          for (Task<?> mmTask : mmTasks) {
            context.getTask().addDependentTask(mmTask);
          }
        } else {
          if (!table.getPartitionKeys().isEmpty()) {
            PartitionIterable parts = new PartitionIterable(context.getDb(), table, null,
                MetastoreConf.getIntVar(context.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX));
            for (Partition part : parts) {
              checkMmLb(part);
            }
          } else {
            checkMmLb(table);
          }
        }
      } else if (isFromMmTable) {
        if (isToFullAcid) {
          table.getParameters().put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
                  DEFAULT_TRANSACTIONAL_PROPERTY);
        } else if (BooleanUtils.isFalse(isToMmTable)) {
          throw new HiveException("Cannot convert an ACID table to non-ACID");
        }
      }

      // Converting to/from external table
      String externalProp = desc.getProps().get("EXTERNAL");
      if (externalProp != null) {
        if (Boolean.parseBoolean(externalProp) && table.getTableType() == TableType.MANAGED_TABLE) {
          table.setTableType(TableType.EXTERNAL_TABLE);
        } else if (!Boolean.parseBoolean(externalProp) && table.getTableType() == TableType.EXTERNAL_TABLE) {
          table.setTableType(TableType.MANAGED_TABLE);
        }
      }

      table.getTTable().getParameters().putAll(desc.getProps());
    }
  }


  private List<Task<?>> generateAddMmTasks(Table table, Long writeId) throws HiveException {
    // We will move all the files in the table/partition directories into the first MM
    // directory, then commit the first write ID.
    if (writeId == null) {
      throw new HiveException("Internal error - write ID not set for MM conversion");
    }

    List<Path> sources = new ArrayList<>();
    List<Path> targets = new ArrayList<>();

    int stmtId = 0;
    String mmDir = AcidUtils.deltaSubdir(writeId, writeId, stmtId);

    if (!table.getPartitionKeys().isEmpty()) {
      PartitionIterable parts = new PartitionIterable(context.getDb(), table, null,
          MetastoreConf.getIntVar(context.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX));
      for (Partition part : parts) {
        checkMmLb(part);
        Path source = part.getDataLocation();
        Path target = new Path(source, mmDir);
        sources.add(source);
        targets.add(target);
        Utilities.FILE_OP_LOGGER.trace("Will move " + source + " to " + target);
      }
    } else {
      checkMmLb(table);
      Path source = table.getDataLocation();
      Path target = new Path(source, mmDir);
      sources.add(source);
      targets.add(target);
      Utilities.FILE_OP_LOGGER.trace("Will move " + source + " to " + target);
    }

    // Don't set inputs and outputs - the locks have already been taken so it's pointless.
    MoveWork mw = new MoveWork(null, null, null, null, false);
    mw.setMultiFilesDesc(new LoadMultiFilesDesc(sources, targets, true, null, null));
    return Lists.<Task<?>>newArrayList(TaskFactory.get(mw));
  }

  private void checkMmLb(Table table) throws HiveException {
    if (!table.isStoredAsSubDirectories()) {
      return;
    }
    // TODO [MM gap?]: by design; no-one seems to use LB tables. They will work, but not convert.
    //                 It's possible to work around this by re-creating and re-inserting the table.
    throw new HiveException("Converting list bucketed tables stored as subdirectories "
        + " to MM is not supported. Please re-create a table in the desired format.");
  }

  private void checkMmLb(Partition partition) throws HiveException {
    if (!partition.isStoredAsSubDirectories()) {
      return;
    }
    throw new HiveException("Converting list bucketed tables stored as subdirectories "
        + " to MM is not supported. Please re-create a table in the desired format.");
  }
}
