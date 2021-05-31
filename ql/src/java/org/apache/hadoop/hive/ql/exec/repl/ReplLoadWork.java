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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.ConstraintEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadTasksBuilder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_STATS_TOP_EVENTS_COUNTS;

@Explain(displayName = "Replication Load Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ReplLoadWork implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ReplLoadWork.class);
  final String dbNameToLoadIn;
  final ReplScope currentReplScope;
  final String dumpDirectory;
  private boolean lastReplIDUpdated;
  private String sourceDbName;
  private Long dumpExecutionId;
  private final transient ReplicationMetricCollector metricCollector;
  final boolean replScopeModified;

  private final ConstraintEventsIterator constraintsIterator;
  private int loadTaskRunCount = 0;
  private DatabaseEvent.State state = null;
  private final transient BootstrapEventsIterator bootstrapIterator;
  private transient IncrementalLoadTasksBuilder incrementalLoadTasksBuilder;
  private transient Task<?> rootTask;
  private Iterator<String> externalTableDataCopyItr;
  private ReplStatsTracker replStatsTracker;

  /*
  these are sessionState objects that are copied over to work to allow for parallel execution.
  based on the current use case the methods are selectively synchronized, which might need to be
  taken care when using other methods.
  */
  final LineageState sessionStateLineageState;

  public ReplLoadWork(HiveConf hiveConf, String dumpDirectory,
                      String sourceDbName, String dbNameToLoadIn, ReplScope currentReplScope,
                      LineageState lineageState, boolean isIncrementalDump, Long eventTo,
                      Long dumpExecutionId,
                      ReplicationMetricCollector metricCollector,
                      boolean replScopeModified) throws IOException, SemanticException {
    sessionStateLineageState = lineageState;
    this.dumpDirectory = dumpDirectory;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.currentReplScope = currentReplScope;
    this.sourceDbName = sourceDbName;
    this.dumpExecutionId = dumpExecutionId;
    this.metricCollector = metricCollector;
    this.replScopeModified = replScopeModified;


    // If DB name is changed during REPL LOAD, then set it instead of referring to source DB name.
    if ((currentReplScope != null) && StringUtils.isNotBlank(dbNameToLoadIn)) {
      currentReplScope.setDbName(dbNameToLoadIn);
    }

    rootTask = null;
    if (isIncrementalDump) {
      if (replStatsTracker == null) {
        int numEvents = hiveConf.getIntVar(REPL_STATS_TOP_EVENTS_COUNTS);
        if (numEvents < 0) {
          LOG.warn("Invalid value configured for {}, Using default of {}", REPL_STATS_TOP_EVENTS_COUNTS,
              REPL_STATS_TOP_EVENTS_COUNTS.defaultIntVal);
          numEvents = REPL_STATS_TOP_EVENTS_COUNTS.defaultIntVal;
        }
        replStatsTracker = new ReplStatsTracker(numEvents);
      }
      incrementalLoadTasksBuilder = new IncrementalLoadTasksBuilder(dbNameToLoadIn, dumpDirectory,
          new IncrementalLoadEventsIterator(dumpDirectory, hiveConf), hiveConf, eventTo, metricCollector,
          replStatsTracker);

      /*
       * If the current incremental dump also includes bootstrap for some tables, then create iterator
       * for the same.
       */
      Path incBootstrapDir = new Path(dumpDirectory, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
      FileSystem fs = incBootstrapDir.getFileSystem(hiveConf);
      if (fs.exists(incBootstrapDir)) {
        this.bootstrapIterator = new BootstrapEventsIterator(
                new Path(incBootstrapDir, EximUtil.METADATA_PATH_NAME).toString(), dbNameToLoadIn, true,
          hiveConf, metricCollector);
        this.constraintsIterator = new ConstraintEventsIterator(dumpDirectory, hiveConf);
      } else {
        this.bootstrapIterator = null;
        this.constraintsIterator = null;
      }
    } else {
      this.bootstrapIterator = new BootstrapEventsIterator(new Path(dumpDirectory, EximUtil.METADATA_PATH_NAME)
              .toString(), dbNameToLoadIn, true, hiveConf, metricCollector);
      this.constraintsIterator = new ConstraintEventsIterator(
              new Path(dumpDirectory, EximUtil.METADATA_PATH_NAME).toString(), hiveConf);
      incrementalLoadTasksBuilder = null;
    }
  }

  BootstrapEventsIterator bootstrapIterator() {
    return bootstrapIterator;
  }

  ConstraintEventsIterator constraintsIterator() {
    return constraintsIterator;
  }

  int executedLoadTask() {
    return ++loadTaskRunCount;
  }

  void updateDbEventState(DatabaseEvent.State state) {
    this.state = state;
  }

  DatabaseEvent databaseEvent(HiveConf hiveConf) {
    return state.toEvent(hiveConf);
  }

  boolean hasDbState() {
    return state != null;
  }

  boolean isIncrementalLoad() {
    return incrementalLoadTasksBuilder != null;
  }

  boolean hasBootstrapLoadTasks() {
    return (((bootstrapIterator != null) && bootstrapIterator.hasNext())
            || ((constraintsIterator != null) && constraintsIterator.hasNext()));
  }

  IncrementalLoadTasksBuilder incrementalLoadTasksBuilder() {
    return incrementalLoadTasksBuilder;
  }

  public Task<?> getRootTask() {
    return rootTask;
  }

  public String getDumpDirectory() {return dumpDirectory;}
  
  public void setRootTask(Task<?> rootTask) {
    this.rootTask = rootTask;
  }

  public boolean isLastReplIDUpdated() {
    return lastReplIDUpdated;
  }

  public void setLastReplIDUpdated(boolean lastReplIDUpdated) {
    this.lastReplIDUpdated = lastReplIDUpdated;
  }

  public String getSourceDbName() {
    return sourceDbName;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }

  public Long getDumpExecutionId() {
    return dumpExecutionId;
  }

  public List<Task<?>> externalTableCopyTasks(TaskTracker tracker, HiveConf conf) throws IOException {
    if (conf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY)) {
      return Collections.emptyList();
    }
    List<Task<?>> tasks = new ArrayList<>();
    Retryable retryable = Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(UncheckedIOException.class).build();
    try {
      retryable.executeCallable((Callable<Void>) ()-> {
        try{
          int numEntriesToSkip = tasks == null ? 0 : tasks.size();
          while (externalTableDataCopyItr.hasNext() && tracker.canAddMoreTasks()) {
            if(numEntriesToSkip > 0) {
              //skip entries added in the previous attempts of this retryable block
              externalTableDataCopyItr.next();
              numEntriesToSkip--;
              continue;
            }
            DirCopyWork dirCopyWork = new DirCopyWork(metricCollector, (new Path(dumpDirectory).getParent()).toString());
            dirCopyWork.loadFromString(externalTableDataCopyItr.next());
            Task<DirCopyWork> task = TaskFactory.get(dirCopyWork, conf);
            tasks.add(task);
            tracker.addTask(task);
            LOG.debug("Added task for {}", dirCopyWork);
          }
        } catch (UncheckedIOException e) {
          LOG.error("Reading entry for data copy failed for external tables, attempting retry.", e);
          throw e;
        }
        return null;
      });
    } catch (Exception e) {
      throw new IOException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()));
    }
    LOG.info("Added total {} tasks for external table locations copy.", tasks.size());
    return tasks;
  }

  public Iterator<String> getExternalTableDataCopyItr() {
    return externalTableDataCopyItr;
  }

  public void setExternalTableDataCopyItr(Iterator<String> externalTableDataCopyItr) {
    this.externalTableDataCopyItr = externalTableDataCopyItr;
  }
}
