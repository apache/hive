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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.load.log.BootstrapLoadLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Replication layout is from the root directory of replication Dump is
 * db
 *    _external_tables_info
 *    table1
 *        _metadata
 *        data
 *          _files
 *    table2
 *        _metadata
 *        data
 *          _files
 *    _functions
 *        functionName1
 *          _metadata
 *        functionName2
 *          _metadata
 * this class understands this layout and hence will help in identifying for subsequent bootstrap tasks
 * as to where the last set of tasks left execution and from where this task should pick up replication.
 * Since for replication we have the need for hierarchy of tasks we need to make sure that db level are
 * processed first before table, table level are processed first before partitions etc.
 *
 * Based on how the metadata is being exported on the file we have to currently take care of the following:
 * 1. Make sure db level are processed first as this will be required before table / functions processing.
 * 2. Table before partition is not explicitly required as table and partition metadata are in the same file.
 *
 *
 * For future integrations other sources of events like kafka, would require to implement an Iterator&lt;BootstrapEvent&gt;
 *
 */
public class BootstrapEventsIterator implements Iterator<BootstrapEvent> {
  private DatabaseEventsIterator currentDatabaseIterator = null;
  /*
      This denotes listing of any directories where during replication we want to take care of
      db level operations first, namely in our case its only during db creation on the replica
      warehouse.
   */
  private Iterator<DatabaseEventsIterator> dbEventsIterator;
  private final String dumpDirectory;
  private final String dbNameToLoadIn;
  private final HiveConf hiveConf;
  private final boolean needLogger;
  private ReplLogger replLogger;
  private final transient ReplicationMetricCollector metricCollector;

  public BootstrapEventsIterator(String dumpDirectory, String dbNameToLoadIn, boolean needLogger, HiveConf hiveConf,
                                 ReplicationMetricCollector metricCollector)
          throws IOException {
    this.metricCollector = metricCollector;
    Path path = new Path(dumpDirectory);
    FileSystem fileSystem = path.getFileSystem(hiveConf);
    if (!fileSystem.exists(path)) {
      throw new IllegalArgumentException("No data to load in path " + dumpDirectory);
    }
    FileStatus[] fileStatuses =
        fileSystem.listStatus(path, ReplUtils.getBootstrapDirectoryFilter(fileSystem));
    if ((fileStatuses == null) || (fileStatuses.length == 0)) {
      throw new IllegalArgumentException("No data to load in path " + dumpDirectory);
    }
    if ((dbNameToLoadIn != null) && (fileStatuses.length > 1)) {
      throw new IllegalArgumentException(
              "Multiple dirs in "
                      + dumpDirectory
                      + " does not correspond to REPL LOAD expecting to load to a singular destination point.");
    }

    List<FileStatus> dbsToCreate = Arrays.stream(fileStatuses).filter(
        f -> !f.getPath().getName().equals(ReplUtils.CONSTRAINTS_ROOT_DIR_NAME)
    ).collect(Collectors.toList());
    dbEventsIterator = dbsToCreate.stream().map(f -> {
      try {
        return new DatabaseEventsIterator(f.getPath(), hiveConf);
      } catch (IOException e) {
        throw new RuntimeException(
            "Error while creating event iterator for db at path" + f.getPath().toString(), e);
      }
    }).collect(Collectors.toList()).iterator();

    this.dumpDirectory = dumpDirectory;
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.needLogger = needLogger;
    this.hiveConf = hiveConf;
    if (needLogger) {
      String dbName = StringUtils.isBlank(dbNameToLoadIn) ? "" : dbNameToLoadIn;
      replLogger = new BootstrapLoadLogger(dbName, dumpDirectory, 0, 0);
    }
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (currentDatabaseIterator == null) {
        if (dbEventsIterator.hasNext()) {
          currentDatabaseIterator = dbEventsIterator.next();
          if (needLogger) {
            initReplLogger();
          }
          initMetricCollector();
        } else {
          return false;
        }
      } else if (currentDatabaseIterator.hasNext()) {
        return true;
      } else {
        currentDatabaseIterator = null;
      }
    }
  }

  @Override
  public BootstrapEvent next() {
    return currentDatabaseIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  @Override
  public void forEachRemaining(Consumer<? super BootstrapEvent> action) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public boolean currentDbHasNext() {
    return ((currentDatabaseIterator != null) && (currentDatabaseIterator.hasNext()));
  }

  public void setReplicationState(ReplicationState replicationState) {
    this.currentDatabaseIterator.replicationState = replicationState;
  }

  public ReplLogger replLogger() {
    return replLogger;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }

  private void initReplLogger() {
    try {
      Path dbDumpPath = currentDatabaseIterator.dbLevelPath();
      FileSystem fs = dbDumpPath.getFileSystem(hiveConf);
      long numTables = getNumTables(dbDumpPath, fs);
      long numFunctions = getNumFunctions(dbDumpPath, fs);
      String dbName = StringUtils.isBlank(dbNameToLoadIn) ? dbDumpPath.getName() : dbNameToLoadIn;
      if (replLogger != null) {
        replLogger.setParams(dbName, dumpDirectory, numTables, numFunctions);
      } else {
        replLogger = new BootstrapLoadLogger(dbName, dumpDirectory, numTables, numFunctions);
      }
      replLogger.startLog();
    } catch (IOException e) {
      // Ignore the exception
    }
  }

  private long getNumFunctions(Path dbDumpPath, FileSystem fs) throws IOException {
    Path funcPath = new Path(dbDumpPath, ReplUtils.FUNCTIONS_ROOT_DIR_NAME);
    if (fs.exists(funcPath)) {
      return getSubDirs(fs, funcPath).length;
    }
    return 0;
  }

  private long getNumTables(Path dbDumpPath, FileSystem fs) throws IOException {
    return getSubDirs(fs, dbDumpPath).length;
  }

  private void initMetricCollector() {
    try {
      Path dbDumpPath = currentDatabaseIterator.dbLevelPath();
      FileSystem fs = dbDumpPath.getFileSystem(hiveConf);
      long numTables = getNumTables(dbDumpPath, fs);
      long numFunctions = getNumFunctions(dbDumpPath, fs);
      Map<String, Long> metricMap = new HashMap<>();
      metricMap.put(ReplUtils.MetricName.TABLES.name(), numTables);
      metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), numFunctions);
      metricCollector.reportStageStart("REPL_LOAD", metricMap);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect Metrics ", e);
    }
  }

  FileStatus[] getSubDirs(FileSystem fs, Path dirPath) throws IOException {
    return fs.listStatus(dirPath, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
  }
}
