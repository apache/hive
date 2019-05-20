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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.repl.load.log.state.*;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.ReplState.LogTag;

public class BootstrapLoadLogger extends ReplLogger {
  private String dbName;
  private String dumpDir;
  private long numTables;
  private long numFunctions;
  private long tableSeqNo;
  private long functionSeqNo;

  public BootstrapLoadLogger(String dbName, String dumpDir, long numTables, long numFunctions) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.numTables = numTables;
    this.numFunctions = numFunctions;
    this.tableSeqNo = 0;
    this.functionSeqNo = 0;
  }

  @Override
  public void startLog() {
    (new BootstrapLoadBegin(dbName, dumpDir, numTables, numFunctions)).log(LogTag.START);
  }

  @Override
  public void tableLog(String tableName, TableType tableType) {
    tableSeqNo++;
    (new BootstrapLoadTable(dbName, tableName, tableType, tableSeqNo, numTables))
            .log(LogTag.TABLE_LOAD);
  }

  @Override
  public void functionLog(String funcName) {
    functionSeqNo++;
    (new BootstrapLoadFunction(dbName, funcName, functionSeqNo, numFunctions))
            .log(LogTag.FUNCTION_LOAD);
  }

  @Override
  public void endLog(String lastReplId) {
    (new BootstrapLoadEnd(dbName, numTables, numFunctions, dumpDir, lastReplId))
            .log(LogTag.END);
  }
}
