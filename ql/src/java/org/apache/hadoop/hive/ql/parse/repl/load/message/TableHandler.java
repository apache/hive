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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TableHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context) throws SemanticException {
    // Path being passed to us is a table dump location. We go ahead and load it in as needed.
    // If tblName is null, then we default to the table name specified in _metadata, which is good.
    // or are both specified, in which case, that's what we are intended to create the new table as.
    if (context.isDbNameEmpty()) {
      throw new SemanticException("Database name cannot be null for a table load");
    }
    try {
      List<Task<? extends Serializable>> importTasks = new ArrayList<>();

      EximUtil.SemanticAnalyzerWrapperContext x =
          new EximUtil.SemanticAnalyzerWrapperContext(
              context.hiveConf, context.db, readEntitySet, writeEntitySet, importTasks, context.log,
              context.nestedContext);

      // REPL LOAD is not partition level. It is always DB or table level. So, passing null for partition specs.
      // Also, REPL LOAD doesn't support external table and hence no location set as well.
      ImportSemanticAnalyzer.prepareImport(false, false, false,
          (context.precursor != null), null, context.tableName, context.dbName,
          null, context.location, x,
          databasesUpdated, tablesUpdated);

      return importTasks;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }
}
