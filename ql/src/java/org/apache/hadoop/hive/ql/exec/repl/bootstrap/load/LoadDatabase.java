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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.owner.AlterDatabaseSetOwnerDesc;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.database.create.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.ReplLoadOpType;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class LoadDatabase {

  final Context context;
  final TaskTracker tracker;

  private final DatabaseEvent event;
  private final String dbNameToLoadIn;
  transient ReplicationMetricCollector metricCollector;
  protected static transient Logger LOG = LoggerFactory.getLogger(LoadDatabase.class);

  public LoadDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn, TaskTracker loadTaskTracker) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(loadTaskTracker);
  }

  public LoadDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn,
                      TaskTracker loadTaskTracker, ReplicationMetricCollector metricCollector) {
    this.context = context;
    this.event = event;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.tracker = new TaskTracker(loadTaskTracker);
    this.metricCollector = metricCollector;
  }

  public TaskTracker tasks() throws Exception {
    Database dbInMetadata = readDbMetadata();
    String dbName = dbInMetadata.getName();
    Task<?> dbRootTask = null;
    ReplLoadOpType loadDbType = getLoadDbType(dbName);
    switch (loadDbType) {
      case LOAD_NEW:
        dbRootTask = createDbTask(dbInMetadata);
        break;
      case LOAD_REPLACE:
        dbRootTask = alterDbTask(dbInMetadata);
        break;
      default:
        break;
    }
    if (dbRootTask != null) {
      dbRootTask.addDependentTask(setOwnerInfoTask(dbInMetadata));
      tracker.addTask(dbRootTask);
    }
    return tracker;
  }

  Database readDbMetadata() throws SemanticException {
    return event.dbInMetadata(dbNameToLoadIn);
  }

  String getDbLocation(Database dbInMetadata) {
    if (context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET)
            && Boolean.parseBoolean(dbInMetadata.getParameters().get(ReplConst.REPL_IS_CUSTOM_DB_LOC))) {
      String locOnTarget = new Path(dbInMetadata.getLocationUri()).toUri().getPath().toString();
      LOG.info("Using the custom location {} on the target", locOnTarget);
      return locOnTarget;
    }
    return null;
  }

  String getDbManagedLocation(Database dbInMetadata) {
    if (context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET)
            && Boolean.parseBoolean(dbInMetadata.getParameters().get(ReplConst.REPL_IS_CUSTOM_DB_MANAGEDLOC))) {
      String locOnTarget = new Path(dbInMetadata.getManagedLocationUri()).toUri().getPath().toString();
      LOG.info("Using the custom managed location {} on the target", locOnTarget);
      return locOnTarget;
    }
    return null;
  }

  private ReplLoadOpType getLoadDbType(String dbName) throws InvalidOperationException, HiveException {
    Database db = context.hiveDb.getDatabase(dbName);
    if (db == null) {
      return ReplLoadOpType.LOAD_NEW;
    }
    if (isDbAlreadyBootstrapped(db)) {
      throw new InvalidOperationException("Bootstrap REPL LOAD is not allowed on Database: " + dbName
              + " as it was already done.");
    }
    if (ReplUtils.replCkptStatus(dbName, db.getParameters(), context.dumpDirectory)) {
      return ReplLoadOpType.LOAD_SKIP;
    }
    if (isDbEmpty(dbName)) {
      return ReplLoadOpType.LOAD_REPLACE;
    }
    throw new InvalidOperationException("Bootstrap REPL LOAD is not allowed on Database: " + dbName
                    + " as it is not empty. One or more tables/functions exist.");
  }

  private boolean isDbAlreadyBootstrapped(Database db) {
    Map<String, String> props = db.getParameters();
    return ((props != null)
            && props.containsKey(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString())
            && !props.get(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString()).isEmpty());
  }

  private boolean isDbEmpty(String dbName) throws HiveException {
    return context.hiveDb.getAllTables(dbName).isEmpty() && context.hiveDb.getFunctions(dbName, "*").isEmpty();
  }

  private Task<?> createDbTask(Database dbObj) throws MetaException {
    // note that we do not set location - for repl load, we want that auto-created.
    CreateDatabaseDesc createDbDesc = new CreateDatabaseDesc(dbObj.getName(), dbObj.getDescription(),
            getDbLocation(dbObj), getDbManagedLocation(dbObj), false, updateDbProps(dbObj, context.dumpDirectory));
    // If it exists, we want this to be an error condition. Repl Load is not intended to replace a
    // db.
    // TODO: we might revisit this in create-drop-recreate cases, needs some thinking on.
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), createDbDesc, true,
            (new Path(context.dumpDirectory)).getParent().toString(), this.metricCollector);
    return TaskFactory.get(work, context.hiveConf);
  }

  private Task<?> alterDbTask(Database dbObj) {
    return alterDbTask(dbObj.getName(), updateDbProps(dbObj, context.dumpDirectory),
            context.hiveConf, context.dumpDirectory, this.metricCollector);
  }

  private Task<?> setOwnerInfoTask(Database dbObj) {
    AlterDatabaseSetOwnerDesc alterDbDesc = new AlterDatabaseSetOwnerDesc(dbObj.getName(),
        new PrincipalDesc(dbObj.getOwnerName(), dbObj.getOwnerType()), null);
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc, true,
            (new Path(context.dumpDirectory)).getParent().toString(), this.metricCollector);
    return TaskFactory.get(work, context.hiveConf);
  }

  private static Map<String, String> updateDbProps(Database dbObj, String dumpDirectory) {
    /*
    explicitly remove the setting of last.repl.id from the db object parameters as loadTask is going
    to run multiple times and explicit logic is in place which prevents updates to tables when db level
    last repl id is set and we create a AlterDatabaseTask at the end of processing a database.
     */
    Map<String, String> parameters = new HashMap<>(dbObj.getParameters());
    parameters.remove(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());

    parameters.remove(ReplicationSpec.KEY.CURR_STATE_ID_TARGET.toString());

    parameters.remove(ReplConst.REPL_IS_CUSTOM_DB_LOC);

    parameters.remove(ReplConst.REPL_IS_CUSTOM_DB_MANAGEDLOC);

    // Add the checkpoint key to the Database binding it to current dump directory.
    // So, if retry using same dump, we shall skip Database object update.
    parameters.put(ReplConst.REPL_TARGET_DB_PROPERTY, dumpDirectory);

    // This flag will be set to false after first incremental load is done. This flag is used by repl copy task to
    // check if duplicate file check is required or not. This flag is used by compaction to check if compaction can be
    // done for this database or not. If compaction is done before first incremental then duplicate check will fail as
    // compaction may change the directory structure.
    parameters.put(ReplConst.REPL_FIRST_INC_PENDING_FLAG, "true");
    //This flag will be set to identify its a target of replication. Repl dump won't be allowed on a database
    //which is a target of replication.
    parameters.put(ReplConst.TARGET_OF_REPLICATION, "true");

    return parameters;
  }

  private static Task<?> alterDbTask(String dbName, Map<String, String> props,
                                     HiveConf hiveConf, String dumpDirectory,
                                     ReplicationMetricCollector metricCollector) {
    AlterDatabaseSetPropertiesDesc alterDbDesc = new AlterDatabaseSetPropertiesDesc(dbName, props, null);
    DDLWork work = new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc, true,
            (new Path(dumpDirectory)).getParent().toString(), metricCollector);
    return TaskFactory.get(work, hiveConf);
  }

  public static class AlterDatabase extends LoadDatabase {

    public AlterDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn,
        TaskTracker loadTaskTracker) {
      super(context, event, dbNameToLoadIn, loadTaskTracker);
    }

    public AlterDatabase(Context context, DatabaseEvent event, String dbNameToLoadIn,
                         TaskTracker loadTaskTracker, ReplicationMetricCollector metricCollector) {
      super(context, event, dbNameToLoadIn, loadTaskTracker, metricCollector);
    }

    @Override
    public TaskTracker tasks() throws SemanticException {
      Database dbObj = readDbMetadata();
      tracker.addTask(alterDbTask(dbObj.getName(), dbObj.getParameters(), context.hiveConf,
              context.dumpDirectory, this.metricCollector ));
      return tracker;
    }
  }
}
