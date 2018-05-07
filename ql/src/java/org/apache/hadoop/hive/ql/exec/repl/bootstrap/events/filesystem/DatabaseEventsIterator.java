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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.FUNCTIONS_ROOT_DIR_NAME;

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
    if (!fileSystem.exists(new Path(dbLevelPath + Path.SEPARATOR + EximUtil.METADATA_NAME))) {
      databaseEventProcessed = true;
    }
    remoteIterator = fileSystem.listFiles(dbLevelPath, true);
  }

  public Path dbLevelPath() {
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
          if (next.getPath().toString().endsWith(EximUtil.METADATA_NAME)) {
            String replacedString = next.getPath().toString().replace(dbLevelPath.toString(), "");
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
      throw new RuntimeException("could not traverse the file via remote iterator " + dbLevelPath,
          e);
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
    if (currentPath.contains(FUNCTIONS_ROOT_DIR_NAME)) {
      LOG.debug("functions directory: {}", next.toString());
      return postProcessing(new FSFunctionEvent(next));
    }
    return postProcessing(new FSTableEvent(hiveConf, next.toString()));
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
          bootstrapEvent = new FSPartitionEvent(hiveConf, previous.toString(), replicationState);
      replicationState = null;
      return bootstrapEvent;
    } else if (replicationState.lastTableReplicated != null) {
      FSTableEvent event = new FSTableEvent(hiveConf, previous.toString());
      replicationState = null;
      return event;
    }
    throw new IllegalStateException("for replicationState " + replicationState.toString());
  }
}
