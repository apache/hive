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

package org.apache.hadoop.hive.ql.ddl.function;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Abstract ancestor of function related ddl analyzer classes.
 */
public abstract class AbstractFunctionAnalyzer extends BaseSemanticAnalyzer {
  public AbstractFunctionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  /**
   * Add write entities to the semantic analyzer to restrict function creation to privileged users.
   */
  protected void addEntities(String functionName, String className, boolean isTemporary,
      List<ResourceUri> resources) throws SemanticException {
    // If the function is being added under a database 'namespace', then add an entity representing
    // the database (only applicable to permanent/metastore functions).
    // We also add a second entity representing the function name.
    // The authorization api implementation can decide which entities it wants to use to
    // authorize the create/drop function call.

    // Add the relevant database 'namespace' as a WriteEntity
    Database database = null;

    // temporary functions don't have any database 'namespace' associated with it
    if (!isTemporary) {
      try {
        String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(functionName);
        String databaseName = qualifiedNameParts[0];
        functionName = qualifiedNameParts[1];
        database = getDatabase(databaseName);
      } catch (HiveException e) {
        LOG.error("Failed to get database ", e);
        throw new SemanticException(e);
      }
    }
    if (database != null) {
      inputs.add(new ReadEntity(database));
      // Add the permanent function as a WriteEntity
      Function function;
      if (queryState.getHiveOperation().equals(HiveOperation.CREATEFUNCTION)) {
        function = new Function(functionName, database.getName(), className,
                SessionState.getUserFromAuthenticator(), PrincipalType.USER,
                (int) (System.currentTimeMillis() / 1000), FunctionType.JAVA, resources);
      } else {
        try {
          function = db.getFunction(database.getName(), functionName);
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
      }
      outputs.add(new WriteEntity(function, WriteEntity.WriteType.DDL_NO_LOCK));
    } else { // Temporary functions
      outputs.add(new WriteEntity(database, functionName, className, Type.FUNCTION, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    if (resources != null) {
      for (ResourceUri resource : resources) {
        String uriPath = resource.getUri();
        outputs.add(toWriteEntity(uriPath));
      }
    }
  }
}
