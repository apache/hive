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

package org.apache.hadoop.hive.ql.ddl.database.alter;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of altering a database.
 */
public abstract class AbstractAlterDatabaseOperation<T extends AbstractAlterDatabaseDesc> extends DDLOperation<T> {
  public AbstractAlterDatabaseOperation(DDLOperationContext context, T desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String dbName = desc.getDatabaseName();
    Database database = context.getDb().getDatabase(dbName);
    if (database == null) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    Map<String, String> params = database.getParameters();
    if ((null != desc.getReplicationSpec()) &&
        !desc.getReplicationSpec().allowEventReplacementInto(params)) {
      LOG.debug("DDLTask: Alter Database {} is skipped as database is newer than update", dbName);
      return 0; // no replacement, the existing database state is newer than our update.
    }

    doAlteration(database, params);

    context.getDb().alterDatabase(database.getName(), database);
    return 0;
  }

  protected abstract void doAlteration(Database database, Map<String, String> params) throws HiveException;
}
