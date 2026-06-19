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
package org.apache.hadoop.hive.ql.parse.repl.dump.log;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.state.BootstrapDumpBegin;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.state.BootstrapDumpEnd;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.state.BootstrapDumpFunction;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.state.BootstrapDumpTable;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplState.LogTag;

/**
 * BootstrapDumpLogger.
 *
 * ReplLogger for bootstrap dump.
 **/
public class BootstrapDumpLogger extends ReplLogger<String> {
  private String dbName;
  private String dumpDir;
  private long estimatedNumTables;
  private long estimatedNumFunctions;
  private long tableSeqNo;
  private long functionSeqNo;

  public BootstrapDumpLogger(String dbName, String dumpDir,
                             int estimatedNumTables, int estimatedNumFunctions) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.estimatedNumTables = estimatedNumTables;
    this.estimatedNumFunctions = estimatedNumFunctions;
    this.tableSeqNo = 0;
    this.functionSeqNo = 0;
  }

  @Override
  public void startLog() {
    (new BootstrapDumpBegin(dbName, estimatedNumTables, estimatedNumFunctions))
            .log(LogTag.START);
  }

  @Override
  public void tableLog(String tableName, TableType tableType) {
    tableSeqNo++;
    (new BootstrapDumpTable(dbName, tableName, tableType, tableSeqNo, estimatedNumTables))
            .log(LogTag.TABLE_DUMP);
  }

  @Override
  public void functionLog(String funcName) {
    functionSeqNo++;
    (new BootstrapDumpFunction(dbName, funcName, functionSeqNo, estimatedNumFunctions))
            .log(LogTag.FUNCTION_DUMP);
  }

  @Override
  public void endLog(String lastReplId) {
    (new BootstrapDumpEnd(dbName, tableSeqNo, functionSeqNo, dumpDir, lastReplId))
            .log(LogTag.END);
  }
}
