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
package org.apache.hadoop.hive.ql.exec.repl.incremental;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.misc.flags.ReplRemoveFirstIncLoadPendFlagDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.hive.ql.parse.repl.load.log.IncrementalLoadLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

/**
 * IncrementalLoad
 * Iterate through the dump directory and create tasks to load the events.
 */
public class IncrementalLoadTasksBuilder {
  private final String dbName;
  private final IncrementalLoadEventsIterator iterator;
  private final HashSet<ReadEntity> inputs;
  private final HashSet<WriteEntity> outputs;
  private Logger log;
  private final HiveConf conf;

  public ReplLogger getReplLogger() {
    return replLogger;
  }

  private final ReplLogger replLogger;
  private static long numIteration;
  private final Long eventTo;
  private String dumpDirectory;
  private final ReplicationMetricCollector metricCollector;
  private boolean shouldFailover;

  public IncrementalLoadTasksBuilder(String dbName, String loadPath, IncrementalLoadEventsIterator iterator,
      HiveConf conf, Long eventTo, ReplicationMetricCollector metricCollector, ReplStatsTracker replStatsTracker,
                                     boolean shouldFailover, int bootstrapTableSize) throws SemanticException {
    this.dbName = dbName;
    dumpDirectory = (new Path(loadPath).getParent()).toString();
    this.iterator = iterator;
    inputs = new HashSet<>();
    outputs = new HashSet<>();
    log = null;
    this.conf = conf;
    replLogger = new IncrementalLoadLogger(dbName, loadPath, iterator.getTotalEventsCount(), replStatsTracker);
    replLogger.startLog();
    this.eventTo = eventTo;
    setNumIteration(0);
    this.metricCollector = metricCollector;
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.EVENTS.name(), (long) iterator.getTotalEventsCount());
    this.shouldFailover = shouldFailover;
    if (shouldFailover) {
      Database db = null;
      try {
        db = Hive.get().getDatabase(dbName);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      String dbFailoverEndPoint = "";
      if (db != null) {
        Map<String, String> params = db.getParameters();
        if (params != null) {
          dbFailoverEndPoint = params.get(ReplConst.REPL_FAILOVER_ENDPOINT);
        }
      }
      this.metricCollector.reportFailoverStart("REPL_LOAD", metricMap,
          new FailoverMetaData(new Path(dumpDirectory, ReplUtils.REPL_HIVE_BASE_DIR), conf),
          dbFailoverEndPoint, ReplConst.FailoverType.PLANNED.toString());
    } else {
      //Registering table metric as we do boostrap of selective tables
      // in second load cycle of optimized bootstrap
      if(bootstrapTableSize > 0) {
        metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) bootstrapTableSize);
      }
      this.metricCollector.reportStageStart("REPL_LOAD", metricMap);
    }
  }

  public Task<?> build(Context context, Hive hive, Logger log,
                                            TaskTracker tracker) throws Exception {
    long builderStartTime = System.currentTimeMillis();
    Task<?> evTaskRoot = TaskFactory.get(new DependencyCollectionWork());
    Task<?> taskChainTail = evTaskRoot;
    Long lastReplayedEvent = null;
    this.log = log;
    numIteration++;
    this.log.debug("Iteration num " + numIteration);
    while (iterator.hasNext() && tracker.canAddMoreTasks()) {
      FileStatus dir = iterator.next();
      String location = dir.getPath().toUri().toString();
      DumpMetaData eventDmd = new DumpMetaData(new Path(location), conf);

      if (!shouldReplayEvent(dir, eventDmd.getDumpType(), dbName)) {
        this.log.debug("Skipping event {} from {} for DB {} maxTasks: {}",
                eventDmd.getDumpType(), dir.getPath().toUri(), dbName, tracker.numberOfTasks());
        continue;
      }

      this.log.debug("Loading event {} from {} for DB {} maxTasks: {}",
              eventDmd.getDumpType(), dir.getPath().toUri(), dbName, tracker.numberOfTasks());

      // event loads will behave similar to table loads, with one crucial difference
      // precursor order is strict, and each event must be processed after the previous one.
      // The way we handle this strict order is as follows:
      // First, we start with a taskChainTail which is a dummy noop task (a DependecyCollectionTask)
      // at the head of our event chain. For each event we process, we tell analyzeTableLoad to
      // create tasks that use the taskChainTail as a dependency. Then, we collect all those tasks
      // and introduce a new barrier task(also a DependencyCollectionTask) which depends on all
      // these tasks. Then, this barrier task becomes our new taskChainTail. Thus, we get a set of
      // tasks as follows:
      //
      //                 --->ev1.task1--                          --->ev2.task1--
      //                /               \                        /               \
      //  evTaskRoot-->*---->ev1.task2---*--> ev1.barrierTask-->*---->ev2.task2---*->evTaskChainTail
      //                \               /
      //                 --->ev1.task3--
      //
      // Once this entire chain is generated, we add evTaskRoot to rootTasks, so as to execute the
      // entire chain

      MessageHandler.Context mhContext = new MessageHandler.Context(dbName, location,
              taskChainTail, eventDmd, conf, hive, context, this.log,
              dumpDirectory, metricCollector);
      List<Task<?>> evTasks = analyzeEventLoad(mhContext);

      if ((evTasks != null) && (!evTasks.isEmpty())) {
        ReplStateLogWork replStateLogWork = new ReplStateLogWork(replLogger, metricCollector,
                dir.getPath().getName(),
                eventDmd.getDumpType().toString(), dumpDirectory);
        Task<?> barrierTask = TaskFactory.get(replStateLogWork, conf);
        AddDependencyToLeaves function = new AddDependencyToLeaves(barrierTask);
        DAGTraversal.traverse(evTasks, function);
        this.log.debug("Updated taskChainTail from {}:{} to {}:{}",
                taskChainTail.getClass(), taskChainTail.getId(), barrierTask.getClass(), barrierTask.getId());
        tracker.addTaskList(taskChainTail.getChildTasks());
        taskChainTail = barrierTask;
      }
      lastReplayedEvent = eventDmd.getEventTo();
    }

    if (!hasMoreWork()) {
      ReplRemoveFirstIncLoadPendFlagDesc desc = new ReplRemoveFirstIncLoadPendFlagDesc(dbName);
      Task<?> updateIncPendTask = TaskFactory.get(new DDLWork(inputs, outputs, desc,
              true, dumpDirectory, this.metricCollector), conf);
      taskChainTail.addDependentTask(updateIncPendTask);
      taskChainTail = updateIncPendTask;

      Map<String, String> dbProps = new HashMap<>();
      dbProps.put(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(),
              String.valueOf((lastReplayedEvent == null) ? eventTo : lastReplayedEvent));
      ReplStateLogWork replStateLogWork = new ReplStateLogWork(replLogger, dbProps, dumpDirectory,
              metricCollector, shouldFailover);
      Task<?> barrierTask = TaskFactory.get(replStateLogWork, conf);
      taskChainTail.addDependentTask(barrierTask);
      this.log.debug("Added {}:{} as a precursor of barrier task {}:{}",
              taskChainTail.getClass(), taskChainTail.getId(),
              barrierTask.getClass(), barrierTask.getId());
    }
    this.log.info("REPL_INCREMENTAL_LOAD task-builder iteration #{}, duration : {} ms",
            numIteration, System.currentTimeMillis() - builderStartTime);
    return evTaskRoot;
  }

  public boolean hasMoreWork() {
    return iterator.hasNext();
  }

  private boolean isEventNotReplayed(Map<String, String> params, FileStatus dir, DumpType dumpType) {
    if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString()))) {
      String replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());
      if (Long.parseLong(replLastId) >= Long.parseLong(dir.getPath().getName())) {
        log.debug("Event " + dumpType + " with replId " + Long.parseLong(dir.getPath().getName())
                + " is already replayed. LastReplId - " +  Long.parseLong(replLastId));
        return false;
      }
    }
    return true;
  }

  private boolean shouldReplayEvent(FileStatus dir, DumpType dumpType, String dbName) {
    // If database itself is null then we can not filter out anything.
    if (StringUtils.isBlank(dbName)) {
      return true;
    }

    try {
      Database database = Hive.get().getDatabase(dbName);
      return (database == null) || isEventNotReplayed(database.getParameters(), dir, dumpType);
    } catch (HiveException e) {
      // May be the db is getting created in this load
      log.debug("Failed to get the database " + dbName);
      return true;
    }
  }

  private List<Task<?>> analyzeEventLoad(MessageHandler.Context context) throws SemanticException {
    MessageHandler messageHandler = context.dmd.getDumpType().handler();
    List<Task<?>> tasks = messageHandler.handle(context);

    if (context.precursor != null) {
      for (Task<?> t : tasks) {
        context.precursor.addDependentTask(t);
        log.debug("Added {}:{} as a precursor of {}:{}",
                context.precursor.getClass(), context.precursor.getId(), t.getClass(), t.getId());
      }
    }

    inputs.addAll(messageHandler.readEntities());
    outputs.addAll(messageHandler.writeEntities());
    return addUpdateReplStateTasks(messageHandler.getUpdatedMetadata(), tasks);
  }

  private Task<?> dbUpdateReplStateTask(String dbName, String replState, Task<?> preCursor) {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(), replState);

    AlterDatabaseSetPropertiesDesc alterDbDesc = new AlterDatabaseSetPropertiesDesc(dbName, mapProp,
        new ReplicationSpec(replState, replState));
    Task<?> updateReplIdTask = TaskFactory.get(new DDLWork(inputs, outputs, alterDbDesc,
                                               true, dumpDirectory,
                                               metricCollector), conf);

    // Link the update repl state task with dependency collection task
    if (preCursor != null) {
      preCursor.addDependentTask(updateReplIdTask);
      log.debug("Added {}:{} as a precursor of {}:{}", preCursor.getClass(), preCursor.getId(),
              updateReplIdTask.getClass(), updateReplIdTask.getId());
    }
    return updateReplIdTask;
  }

  private List<Task<?>> addUpdateReplStateTasks(
          UpdatedMetaDataTracker updatedMetaDataTracker,
          List<Task<?>> importTasks) throws SemanticException {
    // If no import tasks generated by the event then no need to update the repl state to any object.
    if (importTasks.isEmpty()) {
      log.debug("No objects need update of repl state: 0 import tasks");
      return importTasks;
    }

    // Create a barrier task for dependency collection of import tasks
    Task<?> barrierTask = TaskFactory.get(new DependencyCollectionWork(), conf);

    List<Task<?>> tasks = new ArrayList<>();
    Task<?> updateReplIdTask;

    HashMap<String, Integer> dbS = new HashMap<>();
    for (UpdatedMetaDataTracker.UpdateMetaData updateMetaData : updatedMetaDataTracker.getUpdateMetaDataList()) {
      String replState = updateMetaData.getReplState();
      String dbName = updateMetaData.getDbName();

      if (dbS.get(dbName) != null) {
        if (Integer.parseInt(replState) > dbS.get(dbName)) {
          dbS.put(dbName, Integer.parseInt(replState));
        }
      } else {
        dbS.put(dbName, Integer.parseInt(replState));
      }
    }

    for (Map.Entry<String, Integer> entry : dbS.entrySet()) {
      updateReplIdTask = dbUpdateReplStateTask(dbName, String.valueOf(entry.getValue()), barrierTask);
      tasks.add(updateReplIdTask);
    }

    if (tasks.isEmpty()) {
      log.debug("No objects need update of repl state: 0 update tracker tasks");
      return importTasks;
    }

    // Link import tasks to the barrier task which will in-turn linked with repl state update tasks
    DAGTraversal.traverse(importTasks, new AddDependencyToLeaves(barrierTask));

    // At least one task would have been added to update the repl state
    return tasks;
  }

  private static void setNumIteration(int count) {
    numIteration = count;
  }

  public Long eventTo() {
    return eventTo;
  }

  public static long getNumIteration() {
    return numIteration;
  }
}
