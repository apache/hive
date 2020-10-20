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
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.constraint.drop.AlterTableDropConstraintDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class DropConstraintHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    DropConstraintMessage msg = deserializer.getDropConstraintMessage(context.dmd.getPayload());
    final String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
    final String actualTblName = msg.getTable();
    final TableName tName = HiveTableName.ofNullable(actualTblName, actualDbName);
    String constraintName = msg.getConstraint();

    AlterTableDropConstraintDesc dropConstraintsDesc =
        new AlterTableDropConstraintDesc(tName, context.eventOnlyReplicationSpec(), constraintName);
    Task<DDLWork> dropConstraintsTask = TaskFactory.get(
            new DDLWork(readEntitySet, writeEntitySet, dropConstraintsDesc, true,
                    context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
    context.log.debug("Added drop constrain task : {}:{}", dropConstraintsTask.getId(), actualTblName);
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, null);
    return Collections.singletonList(dropConstraintsTask);
  }
}
