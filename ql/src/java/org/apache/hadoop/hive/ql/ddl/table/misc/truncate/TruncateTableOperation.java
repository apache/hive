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

package org.apache.hadoop.hive.ql.ddl.table.misc.truncate;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateTask;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Operation process of truncating a table.
 */
public class TruncateTableOperation extends DDLOperation<TruncateTableDesc> {
  public TruncateTableOperation(DDLOperationContext context, TruncateTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (desc.getColumnIndexes() != null) {
      ColumnTruncateWork truncateWork = new ColumnTruncateWork(desc.getColumnIndexes(), desc.getInputDir(),
          desc.getOutputDir());
      truncateWork.setListBucketingCtx(desc.getLbCtx());
      truncateWork.setMapperCannotSpanPartns(true);

      TaskQueue taskQueue = new TaskQueue();
      ColumnTruncateTask taskExec = new ColumnTruncateTask();
      taskExec.initialize(context.getQueryState(), null, taskQueue, null);
      taskExec.setWork(truncateWork);
      taskExec.setQueryPlan(context.getQueryPlan());

      int ret = taskExec.execute();
      if (taskExec.getException() != null) {
        context.getTask().setException(taskExec.getException());
      }
      return ret;
    }

    String tableName = desc.getTableName();
    Map<String, String> partSpec = desc.getPartSpec();

    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    if (!DDLUtils.allowOperationInReplicationScope(context.getDb(), tableName, partSpec, replicationSpec)) {
      // no truncate, the table is missing either due to drop/rename which follows the truncate.
      // or the existing table is newer than our update.
      LOG.debug("DDLTask: Truncate Table/Partition is skipped as table {} / partition {} is newer than update",
          tableName, (partSpec == null) ?
              "null" : FileUtils.makePartName(new ArrayList<>(partSpec.keySet()), new ArrayList<>(partSpec.values())));
      return 0;
    }

    try {
      context.getDb().truncateTable(tableName, partSpec,
              replicationSpec != null && replicationSpec.isInReplicationScope() ? desc.getWriteId() : 0L);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }
    return 0;
  }
}
