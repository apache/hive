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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.ddl.table.constraint.add.AlterTableAddConstraintDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AddForeignKeyHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    AddForeignKeyMessage msg = deserializer.getAddForeignKeyMessage(context.dmd.getPayload());

    List<SQLForeignKey> fks;
    try {
      fks = msg.getForeignKeys();
    } catch (Exception e) {
      if (!(e instanceof SemanticException)){
        throw new SemanticException("Error reading message members", e);
      } else {
        throw (SemanticException)e;
      }
    }

    List<Task<?>> tasks = new ArrayList<Task<?>>();
    if (fks.isEmpty()) {
      return tasks;
    }

    final String actualDbName = context.isDbNameEmpty() ? fks.get(0).getFktable_db() : context.dbName;
    final String actualTblName = fks.get(0).getFktable_name();
    final TableName tName = TableName.fromString(actualTblName, null, actualDbName);

    for (SQLForeignKey fk : fks) {
      // If parent table is in the same database, change it to the actual db on destination
      // Otherwise, keep db name
      if (fk.getPktable_db().equals(fk.getFktable_db())) {
        fk.setPktable_db(actualDbName);
      }
      fk.setFktable_db(actualDbName);
      fk.setFktable_name(actualTblName);
    }

    Constraints constraints = new Constraints(null, fks, null, null, null, null);
    AlterTableAddConstraintDesc addConstraintsDesc = new AlterTableAddConstraintDesc(tName,
        context.eventOnlyReplicationSpec(), constraints);
    Task<DDLWork> addConstraintsTask = TaskFactory.get(
            new DDLWork(readEntitySet, writeEntitySet, addConstraintsDesc,
                    true, context.getDumpDirectory(), context.getMetricCollector()),
            context.hiveConf
            );
    tasks.add(addConstraintsTask);
    context.log.debug("Added add constrains task : {}:{}", addConstraintsTask.getId(), actualTblName);
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, null);
    return Collections.singletonList(addConstraintsTask);
  }
}
