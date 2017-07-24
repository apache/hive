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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.repl.log.message.*;

public class BootstrapDumpLogger extends ReplLogger {
  private String dbName;
  private Long estimatedNumTables;
  private Long estimatedNumFunctions;
  private Long tableSeqNo;
  private Long functionSeqNo;

  public BootstrapDumpLogger(String dbName, int estimatedNumTables, int estimatedNumFunctions) {
    this.dbName = dbName;
    this.estimatedNumTables = new Long(estimatedNumTables);
    this.estimatedNumFunctions = new Long(estimatedNumFunctions);
    this.tableSeqNo = new Long(0);
    this.functionSeqNo = new Long(0);
  }

  @Override
  public void startLog() {
    (new BootstrapDumpBeginLog(dbName, estimatedNumTables, estimatedNumFunctions))
            .log(LogTag.START);
  }

  @Override
  public void tableLog(String tableName, TableType tableType) {
    tableSeqNo++;
    (new BootstrapDumpTableLog(dbName, tableName, tableType, tableSeqNo, estimatedNumTables))
            .log(LogTag.TABLE_DUMP);
  }

  @Override
  public void functionLog(String funcName) {
    functionSeqNo++;
    (new BootstrapDumpFunctionLog(dbName, funcName, functionSeqNo, estimatedNumFunctions))
            .log(LogTag.FUNCTION_DUMP);
  }

  @Override
  public void endLog(String dumpDir, String lastReplId) {
    (new BootstrapDumpEndLog(dbName, tableSeqNo, functionSeqNo, dumpDir, lastReplId))
            .log(LogTag.END);
  }
}
