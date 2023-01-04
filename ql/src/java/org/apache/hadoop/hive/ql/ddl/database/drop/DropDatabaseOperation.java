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

package org.apache.hadoop.hive.ql.ddl.database.drop;

import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.ProactiveEviction;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Operation process of creating a database.
 */
public class DropDatabaseOperation extends DDLOperation<DropDatabaseDesc> {
  public DropDatabaseOperation(DDLOperationContext context, DropDatabaseDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try {
      String dbName = desc.getDatabaseName();
      ReplicationSpec replicationSpec = desc.getReplicationSpec();
      if (replicationSpec.isInReplicationScope()) {
        Database database = context.getDb().getDatabase(dbName);
        if (database == null || !replicationSpec.allowEventReplacementInto(database.getParameters())) {
          return 0;
        }
      }
      context.getDb().dropDatabase(desc);

      if (LlapHiveUtils.isLlapMode(context.getConf())) {
        ProactiveEviction.Request.Builder llapEvictRequestBuilder = ProactiveEviction.Request.Builder.create();
        llapEvictRequestBuilder.addDb(dbName);
        ProactiveEviction.evict(context.getConf(), llapEvictRequestBuilder.build());
      }
      // Unregister the functions as well
      if (desc.isCasdade()) {
        FunctionRegistry.unregisterPermanentFunctions(dbName);
      }
    } catch (NoSuchObjectException ex) {
      throw new HiveException(ex, ErrorMsg.DATABASE_NOT_EXISTS, desc.getDatabaseName());
    }

    return 0;
  }
}
