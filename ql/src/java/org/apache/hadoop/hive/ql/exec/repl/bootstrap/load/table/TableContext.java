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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;

public class TableContext {
  final String dbNameToLoadIn;
  private final TaskTracker parentTracker;
  // this will only be available when we are doing table load only in replication not otherwise
  private final String tableNameToLoadIn;

  public TableContext(TaskTracker parentTracker, String dbNameToLoadIn,
      String tableNameToLoadIn) {
    this.dbNameToLoadIn = dbNameToLoadIn;
    this.parentTracker = parentTracker;
    this.tableNameToLoadIn = tableNameToLoadIn;
  }

  boolean waitOnPrecursor() {
    return parentTracker.hasTasks();
  }

  ImportTableDesc overrideProperties(ImportTableDesc importTableDesc)
      throws SemanticException {
    if (StringUtils.isNotBlank(tableNameToLoadIn)) {
      importTableDesc.setTableName(tableNameToLoadIn);
    }
    return importTableDesc;
  }
}
