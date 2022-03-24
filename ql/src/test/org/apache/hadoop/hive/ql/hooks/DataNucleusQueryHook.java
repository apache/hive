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
package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * A hook printing selected DataNucleus queries to the session's console. For each Hive command, it prints the
 * DataNucleus queries that were executed in the underlying metastore database during compilation & execution of the
 * respective command.
 *
 * The hook extracts information from log files under the operation log directory and redirects the output to the
 * console. In order to work properly, the appropriate logging configuration must be present so that log files are 
 * created as expected and have the right content.
 *
 * Most of the intelligence lies in the logging configuration. If in the future we need to change the output most likely
 * the change should be done in the configuration and not on this class.
 */
public class DataNucleusQueryHook implements QueryLifeTimeHook {

  /**
   * A property indicating the current status of a query passing through the hook. It is used in conjunction with MDC,
   * so it is supposed to be thread-safe. It is used to filter log messages, which are only passed to the appenders
   * (and written to files), if the status is RUNNING.
   */
  private static final String STATUS = "hive.datanucleus.query.hook.status";

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx) {
    LogUtils.registerLoggingContext(ctx.getHiveConf());
    MDC.put(STATUS, "RUNNING");
  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
    // Do nothing
  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {
    // Do nothing
  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    LogUtils.unregisterLoggingContext();
    MDC.remove(STATUS);
    String operationLogDir = ctx.getHiveConf().getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION);
    String sessionid = ctx.getHiveConf().getVar(HiveConf.ConfVars.HIVESESSIONID);
    String queryid = ctx.getHiveConf().getVar(HiveConf.ConfVars.HIVEQUERYID);
    Path path = Paths.get(operationLogDir, sessionid, queryid + ".datanucleus.log");
    List<String> queryLogs = Collections.emptyList();
    if (Files.exists(path)) {
      try {
        queryLogs = Files.readAllLines(path);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    for (String query : queryLogs) {
      SessionState.getConsole().printInfo(query, false);
    }
  }

}
