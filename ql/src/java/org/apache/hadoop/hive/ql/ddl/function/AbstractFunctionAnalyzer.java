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
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

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
      outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    // Add the function name as a WriteEntity
    outputs.add(new WriteEntity(database, functionName, className, Type.FUNCTION, WriteEntity.WriteType.DDL_NO_LOCK));

    if (resources != null) {
      for (ResourceUri resource : resources) {
        String uriPath = resource.getUri();
        outputs.add(toWriteEntity(uriPath));
      }
    }
  }
}
