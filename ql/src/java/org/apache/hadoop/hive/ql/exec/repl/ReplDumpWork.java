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
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Explain(displayName = "Replication Dump Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ReplDumpWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReplDumpWork.class);
  final ReplScope replScope;
  final ReplScope oldReplScope;
  final String dbNameOrPattern, astRepresentationForErrorMsg, resultTempPath;
  Long eventTo;
  Long eventFrom;
  static String testInjectDumpDir = null;
  static boolean testDeletePreviousDumpMetaPath = false;
  private Integer maxEventLimit;
  private transient Iterator<DirCopyWork> dirCopyIterator;
  private transient Iterator<EximUtil.ManagedTableCopyPath> managedTableCopyPathIterator;
  private Path currentDumpPath;
  private List<String> resultValues;

  public static void injectNextDumpDirForTest(String dumpDir) {
    testInjectDumpDir = dumpDir;
  }

  public static void testDeletePreviousDumpMetaPath(boolean failDeleteDumpMeta) {
    testDeletePreviousDumpMetaPath = failDeleteDumpMeta;
  }

  public ReplDumpWork(ReplScope replScope, ReplScope oldReplScope,
                      String astRepresentationForErrorMsg,
                      String resultTempPath) {
    this.replScope = replScope;
    this.oldReplScope = oldReplScope;
    this.dbNameOrPattern = replScope.getDbName();
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
    this.resultTempPath = resultTempPath;
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

  void setEventFrom(long eventId) {
    eventFrom = eventId;
  }

  // Override any user specification that changes the last event to be dumped.
  void overrideLastEventToDump(Hive fromDb, long bootstrapLastId) throws Exception {
    // If we are bootstrapping ACID tables, we need to dump all the events upto the event id at
    // the beginning of the bootstrap dump and also not dump any event after that. So we override
    // both, the last event as well as any user specified limit on the number of events. See
    // bootstrampDump() for more details.
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

  public void setDirCopyIterator(Iterator<DirCopyWork> dirCopyIterator) {
    if (this.dirCopyIterator != null) {
      throw new IllegalStateException("Dir Copy iterator has already been initialized");
    }
    this.dirCopyIterator = dirCopyIterator;
  }

  public void setManagedTableCopyPathIterator(Iterator<EximUtil.ManagedTableCopyPath> managedTableCopyPathIterator) {
    if (this.managedTableCopyPathIterator != null) {
      throw new IllegalStateException("Managed table copy path iterator has already been initialized");
    }
    this.managedTableCopyPathIterator = managedTableCopyPathIterator;
  }

  public boolean tableDataCopyIteratorsInitialized() {
    return dirCopyIterator != null || managedTableCopyPathIterator != null;
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

  public List<Task<?>> externalTableCopyTasks(TaskTracker tracker, HiveConf conf) {
    List<Task<?>> tasks = new ArrayList<>();
    while (dirCopyIterator.hasNext() && tracker.canAddMoreTasks()) {
      DirCopyWork dirCopyWork = dirCopyIterator.next();
      Task<DirCopyWork> task = TaskFactory.get(dirCopyWork, conf);
      tasks.add(task);
      tracker.addTask(task);
      LOG.debug("added task for {}", dirCopyWork);
    }
    return tasks;
  }

  public List<Task<?>> managedTableCopyTasks(TaskTracker tracker, HiveConf conf) {
    List<Task<?>> tasks = new ArrayList<>();
    while (managedTableCopyPathIterator.hasNext() && tracker.canAddMoreTasks()) {
      EximUtil.ManagedTableCopyPath managedTableCopyPath = managedTableCopyPathIterator.next();
      Task<?> copyTask = ReplCopyTask.getLoadCopyTask(
              managedTableCopyPath.getReplicationSpec(), managedTableCopyPath.getSrcPath(),
              managedTableCopyPath.getTargetPath(), conf, false);
      tasks.add(copyTask);
      tracker.addTask(copyTask);
      LOG.debug("added task for {}", managedTableCopyPath);
    }
    return tasks;
  }
}
