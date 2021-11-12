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
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;
import java.util.HashSet;
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
  private String message = "";
  private String eventId;
  private String eventType;
  private String tableName;
  private TableType tableType;
  private String functionName;
  private String lastReplId;
  private boolean shouldFailover;
  String dumpDirectory;
  private final transient ReplicationMetricCollector metricCollector;

  private enum LOG_TYPE {
    TABLE,
    FUNCTION,
    EVENT,
    END,
    DATA_COPY_END
  }

  public ReplStateLogWork(ReplLogger replLogger, ReplicationMetricCollector metricCollector,
                          String eventId, String eventType) {
    this.logType = LOG_TYPE.EVENT;
    this.replLogger = replLogger;
    this.eventId = eventId;
    this.eventType = eventType;
    this.metricCollector = metricCollector;
  }

  public ReplStateLogWork(ReplLogger replLogger, ReplicationMetricCollector metricCollector,
                          String eventId, String eventType, String dumpDirectory) {
    this.logType = LOG_TYPE.EVENT;
    this.replLogger = replLogger;
    this.eventId = eventId;
    this.eventType = eventType;
    this.metricCollector = metricCollector;
    this.dumpDirectory = dumpDirectory;
  }

  public ReplStateLogWork(ReplLogger replLogger, ReplicationMetricCollector metricCollector,
                          String tableName, TableType tableType) {
    this.logType = LOG_TYPE.TABLE;
    this.replLogger = replLogger;
    this.tableName = tableName;
    this.tableType = tableType;
    this.metricCollector = metricCollector;
  }

  public ReplStateLogWork(ReplLogger replLogger, ReplicationMetricCollector metricCollector,
                          String tableName, TableType tableType, String dumpDirectory) {
    this.logType = LOG_TYPE.TABLE;
    this.replLogger = replLogger;
    this.tableName = tableName;
    this.tableType = tableType;
    this.metricCollector = metricCollector;
    this.dumpDirectory = dumpDirectory;
  }

  public ReplStateLogWork(ReplLogger replLogger, String functionName, ReplicationMetricCollector metricCollector) {
    this.logType = LOG_TYPE.FUNCTION;
    this.replLogger = replLogger;
    this.functionName = functionName;
    this.metricCollector = metricCollector;
  }

  public ReplStateLogWork(ReplLogger replLogger, String functionName, String dumpDirectory, ReplicationMetricCollector metricCollector) {
    this.logType = LOG_TYPE.FUNCTION;
    this.replLogger = replLogger;
    this.functionName = functionName;
    this.dumpDirectory = dumpDirectory;
    this.metricCollector = metricCollector;
  }

  public ReplStateLogWork(ReplLogger replLogger, Map<String, String> dbProps, String dumpDirectory,
                          ReplicationMetricCollector collector, boolean shouldFailover) {
    this.logType = LOG_TYPE.END;
    this.replLogger = replLogger;
    this.lastReplId = ReplicationSpec.getLastReplicatedStateFromParameters(dbProps);
    this.dumpDirectory = dumpDirectory;
    this.metricCollector = collector;
    this.shouldFailover = shouldFailover;
  }

  public ReplStateLogWork(ReplLogger replLogger, String message) {
    this.logType = LOG_TYPE.DATA_COPY_END;
    this.replLogger = replLogger;
    this.metricCollector = null;
    this.message = message;
  }


  public ReplicationMetricCollector getMetricCollector() { return metricCollector; }

  public String getDumpDirectory() { return dumpDirectory; }

  public void replStateLog() throws SemanticException {
    switch (logType) {
    case TABLE:
      replLogger.tableLog(tableName, tableType);
      metricCollector.reportStageProgress("REPL_LOAD", ReplUtils.MetricName.TABLES.name(), 1);
      break;
    case FUNCTION:
      replLogger.functionLog(functionName);
      metricCollector.reportStageProgress("REPL_LOAD", ReplUtils.MetricName.FUNCTIONS.name(), 1);
      break;
    case EVENT:
      replLogger.eventLog(eventId, eventType);
      metricCollector.reportStageProgress("REPL_LOAD", ReplUtils.MetricName.EVENTS.name(), 1);
      break;
    case END:
      replLogger.endLog(lastReplId);
      if (StringUtils.isEmpty(lastReplId) || "null".equalsIgnoreCase(lastReplId)) {
        metricCollector.reportStageEnd("REPL_LOAD", Status.SUCCESS);
      } else {
        metricCollector.reportStageEnd("REPL_LOAD", Status.SUCCESS,
            Long.parseLong(lastReplId), new SnapshotUtils.ReplSnapshotCount(), replLogger.getReplStatsTracker());
      }
      metricCollector.reportEnd((shouldFailover) ? Status.FAILOVER_READY : Status.SUCCESS);
      break;
    case DATA_COPY_END:
      replLogger.dataCopyLog(message);
      break;
    }
  }
}
