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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;

import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.FUNCTIONS_ROOT_DIR_NAME;

class DatabaseEventsIterator implements Iterator<BootstrapEvent> {
  private static Logger LOG = LoggerFactory.getLogger(DatabaseEventsIterator.class);
  private RemoteIterator<LocatedFileStatus> remoteIterator;

  private final Path dbLevelPath;
  private HiveConf hiveConf;
  ReplicationState replicationState;
  private Path next = null, previous = null;
  private boolean databaseEventProcessed = false;

  DatabaseEventsIterator(Path dbLevelPath, HiveConf hiveConf) throws IOException {
    this.dbLevelPath = dbLevelPath;
    this.hiveConf = hiveConf;
    FileSystem fileSystem = dbLevelPath.getFileSystem(hiveConf);
    // this is only there for the use case where we are doing table only replication and not database level
    if (!fileSystem.exists(new Path(dbLevelPath, EximUtil.METADATA_NAME))) {
      databaseEventProcessed = true;
    }

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_REPL_TEST_FILES_SORTED)) {
      LOG.info(" file sorting is enabled in DatabaseEventsIterator");
      List<LocatedFileStatus> fileStatuses = new ArrayList<>();

      // Sort the directories in tha path and then add the files recursively .
      getSortedFileList(dbLevelPath, fileStatuses, fileSystem);
      remoteIterator =  new RemoteIterator<LocatedFileStatus>() {
        private int idx = 0;
        private final int numEntry = fileStatuses.size();
        private final List<LocatedFileStatus> fileStatusesLocal = fileStatuses;
        public boolean hasNext() throws IOException {
          return idx < numEntry;
        }
        public LocatedFileStatus next() throws IOException {
          LOG.info(" file in next is " + fileStatusesLocal.get(idx));
          return fileStatusesLocal.get(idx++);
        }
      };
    } else {
      remoteIterator = fileSystem.listFiles(dbLevelPath, true);
    }
  }

  private void getSortedFileList(Path eventPath, List<LocatedFileStatus> fileStatuses,
                                 FileSystem fileSystem) throws IOException {
    //Add all the files in this directory. No need to sort.
    RemoteIterator<LocatedFileStatus> iteratorNext = fileSystem.listFiles(eventPath, false);
    while (iteratorNext.hasNext()) {
      LocatedFileStatus status = iteratorNext.next();
      LOG.info(" files added at getSortedFileList" + status.getPath());
      fileStatuses.add(status);
    }

    // get all the directories in this path and sort them
    FileStatus[] eventDirs = fileSystem.listStatus(eventPath, EximUtil.getDirectoryFilter(fileSystem));
    if (eventDirs.length == 0) {
      return;
    }
    Arrays.sort(eventDirs, new EventDumpDirComparator());

    // add files recursively for each directory
    for (FileStatus fs : eventDirs) {
      getSortedFileList(fs.getPath(), fileStatuses, fileSystem);
    }
  }

  Path dbLevelPath() {
    return this.dbLevelPath;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!databaseEventProcessed) {
        next = dbLevelPath;
        return true;
      }

      if (replicationState == null && next == null) {
        while (remoteIterator.hasNext()) {
          LocatedFileStatus next = remoteIterator.next();
          // we want to skip this file, this also means there cant be a table with name represented
          // by constant ReplExternalTables.FILE_NAME or ReplUtils.REPL_TABLE_LIST_DIR_NAME (_tables)
          if(next.getPath().toString().endsWith(EximUtil.FILE_LIST_EXTERNAL) ||
                  next.getPath().toString().endsWith(ReplUtils.REPL_TABLE_LIST_DIR_NAME)) {
            continue;
          }
          if (next.getPath().toString().endsWith(EximUtil.METADATA_NAME)) {
            String replacedString = next.getPath().toString()
                    .replace(dbLevelPath.toString(), "");
            List<String> filteredNames = Arrays.stream(replacedString.split(Path.SEPARATOR))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
            if (filteredNames.size() == 1) {
              // this relates to db level event tracked via databaseEventProcessed
            } else {
              this.next = next.getPath().getParent();
              return true;
            }
          }
        }
        return false;
      }
      return true;
    } catch (Exception e) {
      // may be do some retry logic here.
      LOG.error("could not traverse the file via remote iterator " + dbLevelPath, e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /*
  we handle three types of scenarios with special case.
  1. handling of db Level _metadata
  2. handling of subsequent loadTask which will start running from the previous replicationState
  3. other events : these can only be either table / function _metadata.
   */
  @Override
  public BootstrapEvent next() {
    if (!databaseEventProcessed) {
      FSDatabaseEvent event = new FSDatabaseEvent(hiveConf, next.toString());
      databaseEventProcessed = true;
      return postProcessing(event);
    }

    if (replicationState != null) {
      return eventForReplicationState();
    }

    String currentPath = next.toString();
    if (currentPath.contains(Path.SEPARATOR + FUNCTIONS_ROOT_DIR_NAME + Path.SEPARATOR)) {
      LOG.debug("functions directory: {}", next.toString());
      return postProcessing(new FSFunctionEvent(next));
    }
    return postProcessing(new FSTableEvent(hiveConf, next.toString(),
            new Path(getDbLevelDataPath(), next.getName()).toString()));
  }

  private Path getDbLevelDataPath() {
    return new Path(new Path(dbLevelPath.getParent().getParent(), EximUtil.DATA_PATH_NAME), dbLevelPath.getName());
  }

  private BootstrapEvent postProcessing(BootstrapEvent bootstrapEvent) {
    previous = next;
    next = null;
    LOG.debug("processing " + previous);
    return bootstrapEvent;
  }

  private BootstrapEvent eventForReplicationState() {
    if (replicationState.partitionState != null) {
      BootstrapEvent
          bootstrapEvent = new FSPartitionEvent(hiveConf, previous.toString(),
              new Path(getDbLevelDataPath(), previous.getName()).toString(),
              replicationState);
      replicationState = null;
      return bootstrapEvent;
    } else if (replicationState.lastTableReplicated != null) {
      FSTableEvent event = new FSTableEvent(hiveConf, previous.toString(),
              new Path(new Path(dbLevelPath, EximUtil.DATA_PATH_NAME), previous.getName()).toString());
      replicationState = null;
      return event;
    }
    throw new IllegalStateException("for replicationState " + replicationState.toString());
  }
}
