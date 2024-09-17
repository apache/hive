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
package org.apache.hadoop.hive.ql.parse.repl.load.log;

import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadBegin;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadEnd;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadEvent;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplState.LogTag;

/**
 * IncrementalLoadLogger.
 *
 * ReplLogger for Incremental Load.
 **/
public class IncrementalLoadLogger extends ReplLogger<String> {
  private final ReplStatsTracker replStatsTracker;
  private String dbName;
  private String dumpDir;
  private long numEvents;
  private long eventSeqNo;
  private long currentEventTimestamp;

  public IncrementalLoadLogger(String dbName, String dumpDir, int numEvents, ReplStatsTracker replStatsTracker) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.numEvents = numEvents;
    this.eventSeqNo = 0;
    this.currentEventTimestamp = 0;
    this.replStatsTracker = replStatsTracker;
  }

  public void initiateEventTimestamp(long timestamp) {
    if (this.currentEventTimestamp == 0) {
      this.currentEventTimestamp = timestamp;
    }
  }

  @Override
  public void startLog() {
    (new IncrementalLoadBegin(dbName, dumpDir, numEvents)).log(LogTag.START);
  }

  @Override
  public void eventLog(String eventId, String eventType) {
    eventSeqNo++;
    long previousEventTimestamp = this.currentEventTimestamp;
    IncrementalLoadEvent incEvent = new IncrementalLoadEvent(dbName,
            eventId, eventType, eventSeqNo, numEvents, previousEventTimestamp, replStatsTracker);
    incEvent.log(LogTag.EVENT_LOAD);
    this.currentEventTimestamp = incEvent.getLoadTimeMillis();
  }

  @Override
  public void endLog(String lastReplId) {
    (new IncrementalLoadEnd(dbName, numEvents, dumpDir, lastReplId)).log(LogTag.END);
  }

  public ReplStatsTracker getReplStatsTracker() {
    return replStatsTracker;
  }
}
