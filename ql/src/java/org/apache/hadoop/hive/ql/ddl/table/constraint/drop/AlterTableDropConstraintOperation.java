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

package org.apache.hadoop.hive.ql.ddl.table.constraint.drop;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of dropping a new constraint.
 */
public class AlterTableDropConstraintOperation extends DDLOperation<AlterTableDropConstraintDesc> {
  public AlterTableDropConstraintOperation(DDLOperationContext context, AlterTableDropConstraintDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws Exception {
    if (!DDLUtils.allowOperationInReplicationScope(context.getDb(), desc.getDbTableName(), null,
        desc.getReplicationSpec())) {
      // no alter, the table is missing either due to drop/rename which follows the alter.
      // or the existing table is newer than our update.
      LOG.debug("DDLTask: Alter Table is skipped as table {} is newer than update", desc.getDbTableName());
      return 0;
    }

    try {
      context.getDb().dropConstraint(desc.getTableName().getDb(), desc.getTableName().getTable(),
          desc.getConstraintName());
    } catch (NoSuchObjectException e) {
      throw new HiveException(e);
    }

    return 0;
  }
}
