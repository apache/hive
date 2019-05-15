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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;
import java.util.Map;


/**
 * ReplStateLogWork
 *
 */
@Explain(displayName = "Repl State Log", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ReplStateLogWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private final ReplLogger replLogger;
  private final LOG_TYPE logType;
  private String eventId;
  private String eventType;
  private String tableName;
  private TableType tableType;
  private String functionName;
  private String lastReplId;

  private enum LOG_TYPE {
    TABLE,
    FUNCTION,
    EVENT,
    END
  }

  public ReplStateLogWork(ReplLogger replLogger, String eventId, String eventType) {
    this.logType = LOG_TYPE.EVENT;
    this.replLogger = replLogger;
    this.eventId = eventId;
    this.eventType = eventType;
  }

  public ReplStateLogWork(ReplLogger replLogger, String tableName, TableType tableType) {
    this.logType = LOG_TYPE.TABLE;
    this.replLogger = replLogger;
    this.tableName = tableName;
    this.tableType = tableType;
  }

  public ReplStateLogWork(ReplLogger replLogger, String functionName) {
    this.logType = LOG_TYPE.FUNCTION;
    this.replLogger = replLogger;
    this.functionName = functionName;
  }

  public ReplStateLogWork(ReplLogger replLogger, Map<String,String> dbProps) {
    this.logType = LOG_TYPE.END;
    this.replLogger = replLogger;
    this.lastReplId = ReplicationSpec.getLastReplicatedStateFromParameters(dbProps);
  }

  public void replStateLog() {
    switch (logType) {
      case TABLE: {
        replLogger.tableLog(tableName, tableType);
        break;
      }
      case FUNCTION: {
        replLogger.functionLog(functionName);
        break;
      }
      case EVENT: {
        replLogger.eventLog(eventId, eventType);
        break;
      }
      case END: {
        replLogger.endLog(lastReplId);
        break;
      }
    }
  }
}
