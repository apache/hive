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

package org.apache.hadoop.hive.ql.ddl.function.drop;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.StringUtils;

/**
 * Operation process of dropping a function.
 */
public class DropFunctionOperation extends DDLOperation<DropFunctionDesc> {
  public DropFunctionOperation(DDLOperationContext context, DropFunctionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (desc.isTemporary()) {
      return dropTemporaryFunction();
    } else {
      try {
        return dropPermanentFunction();
      } catch (Exception e) {
        context.getTask().setException(e);
        LOG.error("Failed to drop function", e);
        return 1;
      }
    }
  }

  private int dropTemporaryFunction() {
    try {
      FunctionRegistry.unregisterTemporaryUDF(desc.getName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop function: ", e);
      return 1;
    }
  }

  // todo authorization
  private int dropPermanentFunction() throws HiveException {
    String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(desc.getName());
    String dbName = qualifiedNameParts[0];
    String functionName = qualifiedNameParts[1];

    if (skipIfNewerThenUpdate(dbName, functionName)) {
      return 0;
    }

    try {
      String registeredName = FunctionUtils.qualifyFunctionName(functionName, dbName);
      FunctionRegistry.unregisterPermanentFunction(registeredName);
      context.getDb().dropFunction(dbName, functionName);

      return 0;
    } catch (Exception e) {
      // For repl load flow, function may not exist for first incremental phase. So, just return success.
      if (desc.getReplicationSpec().isInReplicationScope() && (e.getCause() instanceof NoSuchObjectException)) {
        LOG.info("Drop function is idempotent as function: " + desc.getName() + " doesn't exist.");
        return 0;
      }
      LOG.info("drop function: ", e);
      context.getConsole().printError("FAILED: error during drop function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private boolean skipIfNewerThenUpdate(String dbName, String functionName) throws HiveException {
    if (desc.getReplicationSpec().isInReplicationScope()) {
      Map<String, String> dbProps = Hive.get().getDatabase(dbName).getParameters();
      if (!desc.getReplicationSpec().allowEventReplacementInto(dbProps)) {
        // If the database is newer than the drop event, then noop it.
        LOG.debug("FunctionTask: Drop Function {} is skipped as database {} is newer than update", functionName,
            dbName);
        return true;
      }
    }

    return false;
  }
}
