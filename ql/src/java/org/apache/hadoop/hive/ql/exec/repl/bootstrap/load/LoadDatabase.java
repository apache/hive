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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LoadDatabase {

  final Context context;
  final TaskTracker tracker;

  private final DatabaseEvent event;
  private final String dbNameToLoadIn;

  public LoadDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn,
      TaskTracker loadTaskTracker) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(loadTaskTracker);
  }

  public TaskTracker tasks() throws SemanticException {
    try {
      Database dbInMetadata = readDbMetadata();
      Task<? extends Serializable> dbRootTask = existEmptyDb(dbInMetadata.getName())
          ? alterDbTask(dbInMetadata, context.hiveConf)
          : createDbTask(dbInMetadata);
      dbRootTask.addDependentTask(setOwnerInfoTask(dbInMetadata));
      tracker.addTask(dbRootTask);
      return tracker;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  Database readDbMetadata() throws SemanticException {
    return event.dbInMetadata(dbNameToLoadIn);
  }

  private Task<? extends Serializable> createDbTask(Database dbObj) {
    CreateDatabaseDesc createDbDesc = new CreateDatabaseDesc();
    createDbDesc.setName(dbObj.getName());
    createDbDesc.setComment(dbObj.getDescription());

    /*
    explicitly remove the setting of last.repl.id from the db object parameters as loadTask is going
    to run multiple times and explicit logic is in place which prevents updates to tables when db level
    last repl id is set and we create a AlterDatabaseTask at the end of processing a database.
     */
    Map<String, String> parameters = new HashMap<>(dbObj.getParameters());
    parameters.remove(ReplicationSpec.KEY.CURR_STATE_ID.toString());
    createDbDesc.setDatabaseProperties(parameters);
    // note that we do not set location - for repl load, we want that auto-created.
    createDbDesc.setIfNotExists(false);
    // If it exists, we want this to be an error condition. Repl Load is not intended to replace a
    // db.
    // TODO: we might revisit this in create-drop-recreate cases, needs some thinking on.
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), createDbDesc);
    return TaskFactory.get(work, context.hiveConf, true);
  }

  private static Task<? extends Serializable> alterDbTask(Database dbObj, HiveConf hiveConf) {
    AlterDatabaseDesc alterDbDesc =
        new AlterDatabaseDesc(dbObj.getName(), dbObj.getParameters(), null);
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc);
    return TaskFactory.get(work, hiveConf, true);
  }

  private Task<? extends Serializable> setOwnerInfoTask(Database dbObj) {
    AlterDatabaseDesc alterDbDesc = new AlterDatabaseDesc(dbObj.getName(),
            new PrincipalDesc(dbObj.getOwnerName(), dbObj.getOwnerType()),
            null);
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc);
    return TaskFactory.get(work, context.hiveConf, true);
  }

  private boolean existEmptyDb(String dbName) throws InvalidOperationException, HiveException {
    Database db = context.hiveDb.getDatabase(dbName);
    if (db == null) {
      return false;
    }
    List<String> allTables = context.hiveDb.getAllTables(dbName);
    List<String> allFunctions = context.hiveDb.getFunctions(dbName, "*");
    if (allTables.isEmpty() && allFunctions.isEmpty()) {
      return true;
    }
    throw new InvalidOperationException(
        "Database " + db.getName() + " is not empty. One or more tables/functions exist.");
  }

  public static class AlterDatabase extends LoadDatabase {

    public AlterDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn,
        TaskTracker loadTaskTracker) {
      super(context, event, dbNameToLoadIn, loadTaskTracker);
    }

    @Override
    public TaskTracker tasks() throws SemanticException {
      tracker.addTask(alterDbTask(readDbMetadata(), context.hiveConf));
      return tracker;
    }
  }
}
