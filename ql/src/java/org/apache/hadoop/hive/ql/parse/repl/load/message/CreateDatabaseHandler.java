
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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.owner.AlterDatabaseSetOwnerDesc;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.database.create.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class CreateDatabaseHandler extends AbstractMessageHandler {

  @Override
  public List<Task<?>> handle(Context context)
          throws SemanticException {
    MetaData metaData;
    try {
      FileSystem fs = FileSystem.get(new Path(context.location).toUri(), context.hiveConf);

      metaData = EximUtil.readMetaData(fs, new Path(context.location, EximUtil.METADATA_NAME));
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
    Database db = metaData.getDatabase();
    String destinationDBName =
        context.dbName == null ? db.getName() : context.dbName;

    CreateDatabaseDesc createDatabaseDesc =
        new CreateDatabaseDesc(destinationDBName, db.getDescription(), null, null, true, db.getParameters());
    Task<DDLWork> createDBTask = TaskFactory.get(
        new DDLWork(new HashSet<>(), new HashSet<>(), createDatabaseDesc, true,
                context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
    if (!db.getParameters().isEmpty()) {
      AlterDatabaseSetPropertiesDesc alterDbDesc = new AlterDatabaseSetPropertiesDesc(destinationDBName,
          db.getParameters(), context.eventOnlyReplicationSpec());
      Task<DDLWork> alterDbProperties = TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(),
                                        alterDbDesc, true, context.getDumpDirectory(),
                                        context.getMetricCollector()), context.hiveConf);
      createDBTask.addDependentTask(alterDbProperties);
    }
    if (StringUtils.isNotEmpty(db.getOwnerName())) {
      AlterDatabaseSetOwnerDesc alterDbOwner = new AlterDatabaseSetOwnerDesc(destinationDBName,
          new PrincipalDesc(db.getOwnerName(), db.getOwnerType()),
          context.eventOnlyReplicationSpec());
      Task<DDLWork> alterDbTask = TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(),
              alterDbOwner, true, context.getDumpDirectory(), context.getMetricCollector()),
              context.hiveConf);
      createDBTask.addDependentTask(alterDbTask);
    }
    updatedMetadata
        .set(context.dmd.getEventTo().toString(), destinationDBName, null, null);
    return Collections.singletonList(createDBTask);
  }
}
