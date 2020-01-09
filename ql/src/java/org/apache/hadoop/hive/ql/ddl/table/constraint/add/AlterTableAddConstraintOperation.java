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

package org.apache.hadoop.hive.ql.ddl.table.constraint.add;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableWithConstraintsDesc;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Operation process of adding a new constraint.
 */
public class AlterTableAddConstraintOperation extends DDLOperation<AlterTableAddConstraintDesc> {
  public AlterTableAddConstraintOperation(DDLOperationContext context, AlterTableAddConstraintDesc desc) {
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

    addConstraints(desc, context.getDb());
    return 0;
  }

  // This function is used by other operations that may modify the constraints
  public static void addConstraints(AbstractAlterTableWithConstraintsDesc desc, Hive db) throws HiveException {
    try {
      Constraints constraints = desc.getConstraints();
      // This is either an alter table add foreign key or add primary key command.
      if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
        db.addPrimaryKey(constraints.getPrimaryKeys());
      }
      if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
        try {
          db.addForeignKey(constraints.getForeignKeys());
        } catch (HiveException e) {
          if (e.getCause() instanceof InvalidObjectException && desc.getReplicationSpec() != null &&
              desc.getReplicationSpec().isInReplicationScope()) {
            // During repl load, NoSuchObjectException in foreign key shall
            // ignore as the foreign table may not be part of the replication
            LOG.debug("InvalidObjectException: ", e);
          } else {
            throw e;
          }
        }
      }
      if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
        db.addUniqueConstraint(constraints.getUniqueConstraints());
      }
      if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
        db.addNotNullConstraint(constraints.getNotNullConstraints());
      }
      if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
        db.addDefaultConstraint(constraints.getDefaultConstraints());
      }
      if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
        db.addCheckConstraint(constraints.getCheckConstraints());
      }
    } catch (NoSuchObjectException e) {
      throw new HiveException(e);
    }
  }
}
