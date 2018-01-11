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
package org.apache.hadoop.hive.ql.exec.repl;

import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@Explain(displayName = "Replication Dump Operator", explainLevels = { Explain.Level.USER,
    Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class ReplDumpWork implements Serializable {
  final String dbNameOrPattern, tableNameOrPattern, astRepresentationForErrorMsg, resultTempPath;
  final Long eventFrom;
  Long eventTo;
  private Integer maxEventLimit;
  static String testInjectDumpDir = null;

  public static void injectNextDumpDirForTest(String dumpDir) {
    testInjectDumpDir = dumpDir;
  }

  public ReplDumpWork(String dbNameOrPattern, String tableNameOrPattern,
      Long eventFrom, Long eventTo, String astRepresentationForErrorMsg, Integer maxEventLimit,
      String resultTempPath) {
    this.dbNameOrPattern = dbNameOrPattern;
    this.tableNameOrPattern = tableNameOrPattern;
    this.eventFrom = eventFrom;
    this.eventTo = eventTo;
    this.astRepresentationForErrorMsg = astRepresentationForErrorMsg;
    this.maxEventLimit = maxEventLimit;
    this.resultTempPath = resultTempPath;
  }

  boolean isBootStrapDump() {
    return eventFrom == null;
  }

  int maxEventLimit() throws Exception {
    if (eventTo < eventFrom) {
      throw new Exception("Invalid event ID input received in TO clause");
    }
    Integer maxRange = Ints.checkedCast(this.eventTo - eventFrom + 1);
    if ((maxEventLimit == null) || (maxEventLimit > maxRange)) {
      maxEventLimit = maxRange;
    }
    return maxEventLimit;
  }

  void overrideEventTo(Hive fromDb) throws Exception {
    if (eventTo == null) {
      eventTo = fromDb.getMSC().getCurrentNotificationEventId().getEventId();
      LoggerFactory.getLogger(this.getClass())
          .debug("eventTo not specified, using current event id : {}", eventTo);
    }
  }
}
