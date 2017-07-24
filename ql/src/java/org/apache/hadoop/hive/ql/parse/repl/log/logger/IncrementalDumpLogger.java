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

import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalDumpBeginLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalDumpEndLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.IncrementalDumpEventLog;
import org.apache.hadoop.hive.ql.parse.repl.log.message.LogTag;

public class IncrementalDumpLogger extends ReplLogger {
  private String dbName;
  private Long estimatedNumEvents;
  private Long eventSeqNo;

  public IncrementalDumpLogger(String dbName, long estimatedNumEvents) {
    this.dbName = dbName;
    this.estimatedNumEvents = new Long(estimatedNumEvents);
    this.eventSeqNo = new Long(0);
  }

  @Override
  public void startLog() {
    (new IncrementalDumpBeginLog(dbName, estimatedNumEvents)).log(LogTag.START);
  }

  @Override
  public void eventLog(String eventId, String eventType) {
    eventSeqNo++;
    (new IncrementalDumpEventLog(dbName, eventId, eventType, eventSeqNo, estimatedNumEvents))
            .log(LogTag.EVENT_DUMP);
  }

  @Override
  public void endLog(String dumpDir, String lastReplId) {
    (new IncrementalDumpEndLog(dbName, eventSeqNo, dumpDir, lastReplId)).log(LogTag.END);
  }
}
