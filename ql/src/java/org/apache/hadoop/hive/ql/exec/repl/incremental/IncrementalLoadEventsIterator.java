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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * IncrementalLoadEventsIterator
 * Helper class to iterate through event dump directory.
 */
public class IncrementalLoadEventsIterator implements Iterator<FileStatus> {
  private FileStatus[] eventDirs;
  private int currentIndex;
  private int numEvents;

  public IncrementalLoadEventsIterator(String loadPath, HiveConf conf) throws IOException {
    Path eventPath = new Path(loadPath);
    FileSystem fs = eventPath.getFileSystem(conf);
    eventDirs = fs.listStatus(eventPath, EximUtil.getDirectoryFilter(fs));
    if ((eventDirs == null) || (eventDirs.length == 0)) {
      throw new IllegalArgumentException("No data to load in path " + loadPath);
    }
    // For event dump, each sub-dir is an individual event dump.
    // We need to guarantee that the directory listing we got is in order of event id.
    Arrays.sort(eventDirs, new EventDumpDirComparator());
    currentIndex = 0;
    numEvents = eventDirs.length;
  }

  @Override
  public boolean hasNext() {
    return (eventDirs != null && currentIndex < numEvents);
  }

  @Override
  public FileStatus next() {
    if (hasNext()) {
      return eventDirs[currentIndex++];
    } else {
      throw new NoSuchElementException("no more events");
    }
  }

  public int getNumEvents() {
    return numEvents;
  }
}
