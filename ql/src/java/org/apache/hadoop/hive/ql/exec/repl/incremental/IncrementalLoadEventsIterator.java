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
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.repl.dump.EventsDumpMetadata;
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
  // when events are batched numEvents denotes number of events in a given batch else numEvents = totalEventsCount
  private int numEvents;
  private int totalEventsCount;
  private boolean eventsBatched;
  private Iterator<FileStatus> eventBatchDirsIterator;
  private FileSystem fs;

  public IncrementalLoadEventsIterator(String loadPath, HiveConf conf) throws IOException, HiveException {
    Path eventPath = new Path(loadPath);
    fs = eventPath.getFileSystem(conf);
    Path eventsDumpAckFile = new Path(eventPath, ReplAck.EVENTS_DUMP.toString());

    eventDirs = fs.listStatus(eventPath, ReplUtils.getEventsDirectoryFilter(fs));

    if ((eventDirs == null) || (eventDirs.length == 0)) {
      currentIndex = numEvents = totalEventsCount = 0;
      return;
    }
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(eventsDumpAckFile, conf);
    eventsBatched = eventsDumpMetadata.isEventsBatched();
    totalEventsCount = eventDirs.length;

    if (eventsBatched) {
      //eventDirs will now have batches eg. events_batch_2, events_batch_1 etc.
      Arrays.sort(eventDirs, new EventDumpDirComparator());
      eventBatchDirsIterator = Arrays.stream(eventDirs).iterator();
      // get all events from first batch.
      eventDirs = fs.listStatus(eventBatchDirsIterator.next().getPath(), ReplUtils.getEventsDirectoryFilter(fs));
      totalEventsCount = eventsDumpMetadata.getEventsDumpedCount();
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

  private boolean hasNextBatch() {
    return eventBatchDirsIterator.hasNext();
  }

  @Override
  public FileStatus next() {
    if (eventsBatched) {
      FileStatus event = eventDirs[currentIndex++];
      if (hasNextBatch() && currentIndex == numEvents) {
        try {
          eventDirs = fs.listStatus(eventBatchDirsIterator.next().getPath(), ReplUtils.getEventsDirectoryFilter(fs));
          Arrays.sort(eventDirs, new EventDumpDirComparator());
          currentIndex = 0;
          numEvents = eventDirs.length;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return event;
    }
    else if (hasNext()) {
      return eventDirs[currentIndex++];
    } else {
      throw new NoSuchElementException("no more events");
    }
  }

  public int getTotalEventsCount() {
    return totalEventsCount;
  }
}
