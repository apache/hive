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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.ConstraintEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadTasksBuilder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.exec.Task;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import static org.apache.hadoop.hive.ql.exec.repl.ExternalTableCopyTaskBuilder.DirCopyWork;

@Explain(displayName = "Replication Load Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ReplLoadWork implements Serializable {
  final String dbNameToLoadIn;
  final ReplScope currentReplScope;
  final String dumpDirectory;
  final String bootstrapDumpToCleanTables;
  boolean needCleanTablesFromBootstrap;

  private final ConstraintEventsIterator constraintsIterator;
  private int loadTaskRunCount = 0;
  private DatabaseEvent.State state = null;
  private final transient BootstrapEventsIterator bootstrapIterator;
  private transient IncrementalLoadTasksBuilder incrementalLoadTasksBuilder;
  private transient Task<? extends Serializable> rootTask;
  private final transient Iterator<DirCopyWork> pathsToCopyIterator;

  /*
  these are sessionState objects that are copied over to work to allow for parallel execution.
  based on the current use case the methods are selectively synchronized, which might need to be
  taken care when using other methods.
  */
  final LineageState sessionStateLineageState;

  public ReplLoadWork(HiveConf hiveConf, String dumpDirectory,
                      String dbNameToLoadIn, ReplScope currentReplScope,
                      LineageState lineageState, boolean isIncrementalDump, Long eventTo,
                      List<DirCopyWork> pathsToCopyIterator) throws IOException {
    sessionStateLineageState = lineageState;
    this.dumpDirectory = dumpDirectory;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.currentReplScope = currentReplScope;

    // If DB name is changed during REPL LOAD, then set it instead of referring to source DB name.
    if ((currentReplScope != null) && StringUtils.isNotBlank(dbNameToLoadIn)) {
      currentReplScope.setDbName(dbNameToLoadIn);
    }
    this.bootstrapDumpToCleanTables = hiveConf.get(ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG);
    this.needCleanTablesFromBootstrap = StringUtils.isNotBlank(this.bootstrapDumpToCleanTables);

    rootTask = null;
    if (isIncrementalDump) {
      incrementalLoadTasksBuilder = new IncrementalLoadTasksBuilder(dbNameToLoadIn, dumpDirectory,
                  new IncrementalLoadEventsIterator(dumpDirectory, hiveConf), hiveConf, eventTo);

      /*
       * If the current incremental dump also includes bootstrap for some tables, then create iterator
       * for the same.
       */
      Path incBootstrapDir = new Path(dumpDirectory, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
      FileSystem fs = incBootstrapDir.getFileSystem(hiveConf);
      if (fs.exists(incBootstrapDir)) {
        this.bootstrapIterator = new BootstrapEventsIterator(incBootstrapDir.toString(), dbNameToLoadIn,
                true, hiveConf);
        this.constraintsIterator = new ConstraintEventsIterator(dumpDirectory, hiveConf);
      } else {
        this.bootstrapIterator = null;
        this.constraintsIterator = null;
      }
    } else {
      this.bootstrapIterator = new BootstrapEventsIterator(dumpDirectory, dbNameToLoadIn, true, hiveConf);
      this.constraintsIterator = new ConstraintEventsIterator(dumpDirectory, hiveConf);
      incrementalLoadTasksBuilder = null;
    }
    this.pathsToCopyIterator = pathsToCopyIterator.iterator();
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

  public Task<? extends Serializable> getRootTask() {
    return rootTask;
  }

  public void setRootTask(Task<? extends Serializable> rootTask) {
    this.rootTask = rootTask;
  }

  public Iterator<DirCopyWork> getPathsToCopyIterator() {
    return pathsToCopyIterator;
  }
}
