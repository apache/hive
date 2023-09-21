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

package org.apache.hadoop.hive.ql.ddl.table.misc.metadata;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public class StorageMetadataUpdateDesc implements DDLDesc {
  private TableName tableName;
  private ExprNodeGenericFuncDesc hiveFilter;
  private Context.Operation operation;

  public StorageMetadataUpdateDesc(TableName tableName, ExprNodeGenericFuncDesc hiveFilter,
      Context.Operation operation) {
    this.tableName = tableName;
    this.hiveFilter = hiveFilter;
    this.operation = operation;
  }

  public TableName getTableName() {
    return tableName;
  }

  public ExprNodeGenericFuncDesc getHiveFilter() {
    return hiveFilter;
  }

  public Context.Operation getOperation() {
    return operation;
  }
}
