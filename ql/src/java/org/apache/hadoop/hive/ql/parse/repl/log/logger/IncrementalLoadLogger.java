/**
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
package org.apache.hadoop.hive.ql.parse.repl.log.logger;

import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalLoadBeginLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalLoadEndLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalLoadEventLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.LogTag;

public class IncrementalLoadLogger extends ReplLogger {
  private String dbName;
  private String dumpDir;
  private Long numEvents;
  private Long eventSeqNo;

  public IncrementalLoadLogger(String dbName, String dumpDir, int numEvents) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.numEvents = new Long(numEvents);
    this.eventSeqNo = new Long(0);
  }

  @Override
  public void startLog() {
    (new IncrementalLoadBeginLog(dbName, dumpDir, numEvents)).log(LogTag.START);
  }

  @Override
  public void eventLog(String eventId, String eventType) {
    eventSeqNo++;
    (new IncrementalLoadEventLog(dbName, eventId, eventType, eventSeqNo, numEvents))
            .log(LogTag.EVENT_LOAD);
  }

  @Override
  public void endLog(String dumpDir, String lastReplId) {
    (new IncrementalLoadEndLog(dbName, numEvents, dumpDir, lastReplId)).log(LogTag.END);
  }
}
