/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Replication layout is from the root directory of replication Dump is
 * db
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
 * For future integrations other sources of events like kafka, would require to implement an Iterator<BootstrapEvent>
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

  public BootstrapEventsIterator(String dumpDirectory, HiveConf hiveConf) throws IOException {
    Path path = new Path(dumpDirectory);
    FileSystem fileSystem = path.getFileSystem(hiveConf);
    FileStatus[] fileStatuses =
        fileSystem.listStatus(new Path(dumpDirectory), EximUtil.getDirectoryFilter(fileSystem));

    List<FileStatus> dbsToCreate = Arrays.stream(fileStatuses).filter(f -> {
      Path metadataPath = new Path(f.getPath() + Path.SEPARATOR + EximUtil.METADATA_NAME);
      try {
        return fileSystem.exists(metadataPath);
      } catch (IOException e) {
        throw new RuntimeException("could not determine if exists : " + metadataPath.toString(), e);
      }
    }).collect(Collectors.toList());
    dbEventsIterator = dbsToCreate.stream().map(f -> {
      try {
        return new DatabaseEventsIterator(f.getPath(), hiveConf);
      } catch (IOException e) {
        throw new RuntimeException(
            "Error while creating event iterator for db at path" + f.getPath().toString(), e);
      }
    }).collect(Collectors.toList()).iterator();

  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (currentDatabaseIterator == null) {
        if (dbEventsIterator.hasNext()) {
          currentDatabaseIterator = dbEventsIterator.next();
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

  public void setReplicationState(ReplicationState replicationState) {
    this.currentDatabaseIterator.replicationState = replicationState;
  }
}
