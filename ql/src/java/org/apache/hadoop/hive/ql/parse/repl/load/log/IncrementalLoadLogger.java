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

import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadBegin;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadEnd;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.IncrementalLoadEvent;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplState.LogTag;

public class IncrementalLoadLogger extends ReplLogger {
  private String dbName;
  private String dumpDir;
  private long numEvents;
  private long eventSeqNo;

  public IncrementalLoadLogger(String dbName, String dumpDir, int numEvents) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.numEvents = numEvents;
    this.eventSeqNo = 0;
  }

  @Override
  public void startLog() {
    (new IncrementalLoadBegin(dbName, dumpDir, numEvents)).log(LogTag.START);
  }

  @Override
  public void eventLog(String eventId, String eventType) {
    eventSeqNo++;
    (new IncrementalLoadEvent(dbName, eventId, eventType, eventSeqNo, numEvents))
            .log(LogTag.EVENT_LOAD);
  }

  @Override
  public void endLog(String lastReplId) {
    (new IncrementalLoadEnd(dbName, numEvents, dumpDir, lastReplId)).log(LogTag.END);
  }
}
