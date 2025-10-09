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

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;
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

@Explain(displayName = "Replication Dump Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ReplDumpWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReplDumpWork.class);
  final ReplScope replScope;
  ReplScope oldReplScope;
  final String dbNameOrPattern, astRepresentationForErrorMsg, resultTempPath;
  Long eventTo;
  Long eventFrom;
  private boolean isBootstrap;
  private static String testInjectDumpDir = null;
  private static boolean testInjectDumpDirAutoIncrement = false;
  static boolean testDeletePreviousDumpMetaPath = false;
  private Integer maxEventLimit;
  private transient Iterator<String> externalTblCopyPathIterator;
  private transient Iterator<String> managedTblCopyPathIterator;
  private transient Iterator<EximUtil.DataCopyPath>  functionCopyPathIterator;
  private Path currentDumpPath;
  private List<String> resultValues;
  private boolean shouldOverwrite;
  private transient ReplicationMetricCollector metricCollector;
  private ReplicationSpec replicationSpec;
  private ReplLogger replLogger;
  private FailoverMetaData fmd;
  private boolean firstDumpAfterFailover;
  private boolean secondDumpAfterFailover;

  public static void injectNextDumpDirForTest(String dumpDir) {
    injectNextDumpDirForTest(dumpDir, false);
  }
  public static void injectNextDumpDirForTest(String dumpDir, boolean autoIncr) {
    testInjectDumpDir = dumpDir;
    testInjectDumpDirAutoIncrement = autoIncr;
  }

  public static synchronized String getTestInjectDumpDir() {
    return testInjectDumpDir;
  }

  public static synchronized String getInjectNextDumpDirForTest() {
    if (testInjectDumpDirAutoIncrement) {
      testInjectDumpDir = String.valueOf(Integer.parseInt(testInjectDumpDir) + 1);
    }
    return testInjectDumpDir;
  }

  public static void testDeletePreviousDumpMetaPath(boolean failDeleteDumpMeta) {
    testDeletePreviousDumpMetaPath = failDeleteDumpMeta;
  }

  public ReplDumpWork(ReplScope replScope,
                      String astRepresentationForErrorMsg,
                      String resultTempPath) {
    this.replScope = replScope;
    this.dbNameOrPattern = replScope.getDbName();
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
    this.resultTempPath = resultTempPath;
  }

  void setOldReplScope(ReplScope replScope) {
    oldReplScope = replScope;
  }

  int maxEventLimit() throws Exception {
    if (eventTo < eventFrom) {
      throw new Exception("Invalid event ID input received in TO clause");
    }
    Integer maxRange = Ints.checkedCast(this.eventTo - eventFrom + 1);
    if ((maxEventLimit == null) || (maxEventLimit > maxRange)) {
      maxEventLimit = maxRange;
    }
    return maxEventLimit;
  }

  public boolean isFirstDumpAfterFailover() {
    return firstDumpAfterFailover;
  }

  public void setFirstDumpAfterFailover(boolean firstDumpAfterFailover) {
    this.firstDumpAfterFailover = firstDumpAfterFailover;
  }

  FailoverMetaData getFailoverMetadata() {
    return fmd;
  }

  void setFailoverMetadata(FailoverMetaData fmd) {
    this.fmd = fmd;
  }

  void setEventFrom(long eventId) {
    eventFrom = eventId;
  }

  // Override any user specification that changes the last event to be dumped.
  void overrideLastEventToDump(Hive fromDb, long bootstrapLastId, long failoverEventId) throws Exception {
    // If we are bootstrapping ACID tables, we need to dump all the events upto the event id at
    // the beginning of the bootstrap dump and also not dump any event after that. So we override
    // both, the last event and any user specified limit on the number of events. See
    // bootstrapDump() for more details.
    if (failoverEventId > 0) {
      LOG.info("eventTo : {} marked as failover eventId.", eventTo);
      eventTo = failoverEventId;
      return;
    }
    if (bootstrapLastId > 0) {
      eventTo = bootstrapLastId;
      LoggerFactory.getLogger(this.getClass())
              .debug("eventTo restricted to event id : {} because of bootstrap of ACID tables",
                      eventTo);
      return;
    }

    // If no last event is specified get the current last from the metastore.
    if (eventTo == null) {
      eventTo = fromDb.getMSC().getCurrentNotificationEventId().getEventId();
      LoggerFactory.getLogger(this.getClass())
          .debug("eventTo not specified, using current event id : {}", eventTo);
    }
  }

  public boolean isBootstrap() {
    return isBootstrap || firstDumpAfterFailover;
  }

  void setBootstrap(boolean bootstrap) {
    isBootstrap = bootstrap;
  }

  public void setExternalTblCopyPathIterator(Iterator<String> externalTblCopyPathIterator) {
    if (this.externalTblCopyPathIterator != null) {
      throw new IllegalStateException("External table copy path iterator has already been initialized");
    }
    this.externalTblCopyPathIterator = externalTblCopyPathIterator;
  }

  public void setManagedTableCopyPathIterator(Iterator<String> managedTblCopyPathIterator) {
    if (this.managedTblCopyPathIterator != null) {
      throw new IllegalStateException("Managed table copy path iterator has already been initialized");
    }
    this.managedTblCopyPathIterator = managedTblCopyPathIterator;
  }

  public void setFunctionCopyPathIterator(Iterator<EximUtil.DataCopyPath> functionCopyPathIterator) {
    if (this.functionCopyPathIterator != null) {
      throw new IllegalStateException("Function copy path iterator has already been initialized");
    }
    this.functionCopyPathIterator = functionCopyPathIterator;
  }

  public boolean dataCopyIteratorsInitialized() {
    return externalTblCopyPathIterator != null
            || managedTblCopyPathIterator != null
            || functionCopyPathIterator != null;
  }

  public Path getCurrentDumpPath() {
    return currentDumpPath;
  }

  public void setCurrentDumpPath(Path currentDumpPath) {
    this.currentDumpPath = currentDumpPath;
  }

  public List<String> getResultValues() {
    return resultValues;
  }

  public void setResultValues(List<String> resultValues) {
    this.resultValues = resultValues;
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
          while (externalTblCopyPathIterator.hasNext() &&  tracker.canAddMoreTasks()) {
            if(numEntriesToSkip > 0) {
              //skip tasks added in previous attempts of this retryable block
              externalTblCopyPathIterator.next();
              numEntriesToSkip--;
              continue;
            }
            DirCopyWork dirCopyWork = new DirCopyWork(metricCollector, currentDumpPath.toString());
            dirCopyWork.loadFromString(externalTblCopyPathIterator.next());
            Task<DirCopyWork> task = TaskFactory.get(dirCopyWork, conf);
            tasks.add(task);
            tracker.addTask(task);
            LOG.debug("added task for {}", dirCopyWork);
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
    return tasks;
  }

  public List<Task<?>> managedTableCopyTasks(TaskTracker tracker, HiveConf conf) throws IOException {
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
          while (managedTblCopyPathIterator.hasNext() && tracker.canAddMoreTasks()) {
            if(numEntriesToSkip > 0) {
              //skip tasks added in previous attempts of this retryable block
              managedTblCopyPathIterator.next();
              numEntriesToSkip--;
              continue;
            }
            ReplicationSpec replSpec = new ReplicationSpec();
            replSpec.setIsReplace(true);
            replSpec.setInReplicationScope(true);
            EximUtil.DataCopyPath managedTableCopyPath = new EximUtil.DataCopyPath(replSpec);
            managedTableCopyPath.loadFromString(managedTblCopyPathIterator.next());
            //If its incremental, in checkpointing case, dump dir may exist. We will delete the event dir.
            //In case of bootstrap checkpointing we will not delete the entire dir and just do a sync
            Task<?> copyTask = ReplCopyTask.getDumpCopyTask(
                    managedTableCopyPath.getReplicationSpec(), managedTableCopyPath.getSrcPath(),
                    managedTableCopyPath.getTargetPath(), conf, false, shouldOverwrite, !isBootstrap(),
                    getCurrentDumpPath().toString(), getMetricCollector());
            tasks.add(copyTask);
            tracker.addTask(copyTask);
            LOG.debug("added task for {}", managedTableCopyPath);
          }
        } catch (UncheckedIOException e) {
          LOG.error("Reading entry for data copy failed for managed tables, attempting retry.", e);
          throw e;
        }
        return null;
      });
    } catch (Exception e) {
      throw new IOException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()));
    }
    return tasks;
  }

  public List<Task<?>> functionsBinariesCopyTasks(TaskTracker tracker, HiveConf conf) {
    List<Task<?>> tasks = new ArrayList<>();
    if (functionCopyPathIterator != null) {
      while (functionCopyPathIterator.hasNext() && tracker.canAddMoreTasks()) {
        EximUtil.DataCopyPath binaryCopyPath = functionCopyPathIterator.next();
        Task<?> copyTask = ReplCopyTask.getDumpCopyTask(
                binaryCopyPath.getReplicationSpec(), binaryCopyPath.getSrcPath(), binaryCopyPath.getTargetPath(), conf,
          getCurrentDumpPath().toString(), getMetricCollector()
        );
        tasks.add(copyTask);
        tracker.addTask(copyTask);
        LOG.debug("added task for {}", binaryCopyPath);
      }
    }
    return tasks;
  }

  public boolean shouldOverWrite() {
    return shouldOverwrite;
  }

  public void setShouldOverwrite(boolean shouldOverwrite) {
    this.shouldOverwrite = shouldOverwrite;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }

  public void setMetricCollector(ReplicationMetricCollector metricCollector) {
    this.metricCollector = metricCollector;
  }

  public ReplicationSpec getReplicationSpec() {
    return replicationSpec;
  }

  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  public Iterator<String> getExternalTblCopyPathIterator() {
    return externalTblCopyPathIterator;
  }

  public ReplLogger getReplLogger() {
    return replLogger;
  }

  public void setReplLogger(ReplLogger replLogger) {
    this.replLogger = replLogger;
  }

  public boolean isSecondDumpAfterFailover() {
    return secondDumpAfterFailover;
  }

  public void setSecondDumpAfterFailover(boolean secondDumpAfterFailover) {
    this.secondDumpAfterFailover = secondDumpAfterFailover;
  }
}
