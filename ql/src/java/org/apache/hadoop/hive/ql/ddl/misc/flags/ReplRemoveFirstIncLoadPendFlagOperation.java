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

package org.apache.hadoop.hive.ql.ddl.misc.flags;

import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;

/**
 * Operation process of removing the REPL_FIRST_INC_PENDING_FLAG parameter from some tables or databases.
 */
public class ReplRemoveFirstIncLoadPendFlagOperation extends DDLOperation<ReplRemoveFirstIncLoadPendFlagDesc> {
  public ReplRemoveFirstIncLoadPendFlagOperation(DDLOperationContext context, ReplRemoveFirstIncLoadPendFlagDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws Exception {
    String dbNameOrPattern = desc.getDatabaseName();

    // Flag is set only in database for db level load.
    for (String dbName : Utils.matchesDb(context.getDb(), dbNameOrPattern)) {
      Database database = context.getDb().getMSC().getDatabase(dbName);
      Map<String, String> parameters = database.getParameters();
      String incPendPara = parameters != null ? parameters.get(ReplConst.REPL_FIRST_INC_PENDING_FLAG) : null;
      if (incPendPara != null) {
        parameters.remove(ReplConst.REPL_FIRST_INC_PENDING_FLAG);
        context.getDb().getMSC().alterDatabase(dbName, database);
      }
    }

    return 0;
  }
}
