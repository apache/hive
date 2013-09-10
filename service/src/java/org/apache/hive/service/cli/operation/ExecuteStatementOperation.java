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
package org.apache.hive.service.cli.operation;



import java.util.HashMap;
import java.util.Map;

import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;
  protected Map<String, String> confOverlay = new HashMap<String, String>();

  public ExecuteStatementOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay) {
    super(parentSession, OperationType.EXECUTE_STATEMENT);
    this.statement = statement;
    this.confOverlay = confOverlay;
  }

  public String getStatement() {
    return statement;
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(
      HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runAsync) {
    String[] tokens = statement.trim().split("\\s+");
    String command = tokens[0].toLowerCase();

    if ("set".equals(command)) {
      return new SetOperation(parentSession, statement, confOverlay);
    } else if ("dfs".equals(command)) {
      return new DfsOperation(parentSession, statement, confOverlay);
    } else if ("add".equals(command)) {
      return new AddResourceOperation(parentSession, statement, confOverlay);
    } else if ("delete".equals(command)) {
      return new DeleteResourceOperation(parentSession, statement, confOverlay);
    } else {
      return new SQLOperation(parentSession, statement, confOverlay, runAsync);
    }
  }
}
