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

package org.apache.hadoop.hive.ql.ddl.database.use;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of switching to another database.
 */
public class SwitchDatabaseOperation extends DDLOperation<SwitchDatabaseDesc> {
  public SwitchDatabaseOperation(DDLOperationContext context, SwitchDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String dbName = desc.getDatabaseName();
    if (!context.getDb().databaseExists(dbName)) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    SessionState.get().setCurrentDatabase(dbName);

    // set database specific parameters
    Database database = context.getDb().getDatabase(dbName);
    assert(database != null);

    Map<String, String> dbParams = database.getParameters();
    if (dbParams != null) {
      for (HiveConf.ConfVars var: HiveConf.dbVars) {
        String newValue = dbParams.get(var.varname);
        if (newValue != null) {
          LOG.info("Changing {} from {} to {}", var.varname, context.getConf().getVar(var), newValue);
          context.getConf().setVar(var, newValue);
        }
      }
    }

    return 0;
  }
}
