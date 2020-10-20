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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddCheckConstraintMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.ddl.table.constraint.add.AlterTableAddConstraintDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * AddCheckConstraintHandler
 * Target(Load) side handler for add check constraint event.
 */
public class AddCheckConstraintHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
    throws SemanticException {
    AddCheckConstraintMessage msg = deserializer.getAddCheckConstraintMessage(context.dmd.getPayload());

    List<SQLCheckConstraint> ccs;
    try {
      ccs = msg.getCheckConstraints();
    } catch (Exception e) {
      if (!(e instanceof SemanticException)){
        throw new SemanticException("Error reading message members", e);
      } else {
        throw (SemanticException)e;
      }
    }

    List<Task<?>> tasks = new ArrayList<Task<?>>();
    if (ccs.isEmpty()) {
      return tasks;
    }

    final String actualDbName = context.isDbNameEmpty() ? ccs.get(0).getTable_db() : context.dbName;
    final String actualTblName = ccs.get(0).getTable_name();
    final TableName tName = TableName.fromString(actualTblName, null, actualDbName);

    for (SQLCheckConstraint ck : ccs) {
      ck.setTable_db(actualDbName);
      ck.setTable_name(actualTblName);
    }

    Constraints constraints = new Constraints(null, null, null, null, null, ccs);
    AlterTableAddConstraintDesc addConstraintsDesc = new AlterTableAddConstraintDesc(tName,
      context.eventOnlyReplicationSpec(), constraints);
    Task<DDLWork> addConstraintsTask = TaskFactory.get(
      new DDLWork(readEntitySet, writeEntitySet, addConstraintsDesc, true,
              context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
    tasks.add(addConstraintsTask);
    context.log.debug("Added add constrains task : {}:{}", addConstraintsTask.getId(), actualTblName);
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, null);
    return Collections.singletonList(addConstraintsTask);
  }
}
