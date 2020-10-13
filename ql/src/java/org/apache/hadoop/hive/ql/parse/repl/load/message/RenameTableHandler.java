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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.misc.rename.AlterTableRenameDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;

public class RenameTableHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    AlterTableMessage msg = deserializer.getAlterTableMessage(context.dmd.getPayload());
    try {
      Table tableObjBefore = msg.getTableObjBefore();
      Table tableObjAfter = msg.getTableObjAfter();
      String oldDbName = tableObjBefore.getDbName();
      String newDbName = tableObjAfter.getDbName();

      if (!context.isDbNameEmpty()) {
        // If we're loading into a db, instead of into the warehouse, then the oldDbName and
        // newDbName must be the same
        if (!oldDbName.equalsIgnoreCase(newDbName)) {
          throw new SemanticException("Cannot replicate an event renaming a table across"
              + " databases into a db level load " + oldDbName + "->" + newDbName);
        } else {
          // both were the same, and can be replaced by the new db we're loading into.
          oldDbName = context.dbName;
          newDbName = context.dbName;
        }
      }

      TableName oldName = TableName.fromString(tableObjBefore.getTableName(), null, oldDbName);
      TableName newName = TableName.fromString(tableObjAfter.getTableName(), null, newDbName);
      ReplicationSpec replicationSpec = context.eventOnlyReplicationSpec();

      AlterTableRenameDesc renameTableDesc =
          new AlterTableRenameDesc(oldName, replicationSpec, false, newName.getNotEmptyDbTable());
      renameTableDesc.setWriteId(msg.getWriteId());
      Task<DDLWork> renameTableTask = TaskFactory.get(new DDLWork(readEntitySet, writeEntitySet,
              renameTableDesc, true, context.getDumpDirectory(),
              context.getMetricCollector()), context.hiveConf);
      context.log.debug("Added rename table task : {}:{}->{}",
                        renameTableTask.getId(), oldName.getNotEmptyDbTable(), newName.getNotEmptyDbTable());

      // oldDbName and newDbName *will* be the same if we're here
      updatedMetadata.set(context.dmd.getEventTo().toString(), newDbName,
              tableObjAfter.getTableName(), null);

      // Note : edge-case here in interaction with table-level REPL LOAD, where that nukes out
      // tablesUpdated. However, we explicitly don't support repl of that sort, and error out above
      // if so. If that should ever change, this will need reworking.
      return ReplUtils.addChildTask(renameTableTask);
    } catch (Exception e) {
      throw (e instanceof SemanticException)
          ? (SemanticException) e
          : new SemanticException("Error reading message members", e);
    }
  }
}
