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

public class BootstrapLoadLogger extends ReplLogger {
  private String dbName;
  private String dumpDir;
  private Long numTables;
  private Long numFunctions;
  private Long tableSeqNo;
  private Long functionSeqNo;

  public BootstrapLoadLogger(String dbName, String dumpDir, long numTables, long numFunctions) {
    this.dbName = dbName;
    this.dumpDir = dumpDir;
    this.numTables = new Long(numTables);
    this.numFunctions = new Long(numFunctions);
    this.tableSeqNo = new Long(0);
    this.functionSeqNo = new Long(0);
  }

  @Override
  public void startLog() {
    (new BootstrapLoadBeginLog(dbName, dumpDir, numTables, numFunctions)).log(LogTag.START);
  }

  @Override
  public void tableLog(String tableName, TableType tableType) {
    tableSeqNo++;
    (new BootstrapLoadTableLog(dbName, tableName, tableType, tableSeqNo, numTables))
            .log(LogTag.TABLE_LOAD);
  }

  @Override
  public void functionLog(String funcName) {
    functionSeqNo++;
    (new BootstrapLoadFunctionLog(dbName, funcName, functionSeqNo, numFunctions))
            .log(LogTag.FUNCTION_LOAD);
  }

  @Override
  public void endLog(String dumpDir, String lastReplId) {
    (new BootstrapLoadEndLog(dbName, numTables, numFunctions, dumpDir, lastReplId)).log(LogTag.END);
  }
}
