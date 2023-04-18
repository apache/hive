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

package org.apache.hadoop.hive.ql.ddl.table.misc.rename;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;

/**
 * Operation process of renaming a table.
 */
public class AlterTableRenameOperation extends AbstractAlterTableOperation<AlterTableRenameDesc> {
  public AlterTableRenameOperation(DDLOperationContext context, AlterTableRenameDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    TableName tableName = HiveTableName.of(desc.getDbTableName());
    if (Utils.isBootstrapDumpInProgress(context.getDb(), tableName.getDb())) {
      LOG.error("DDLTask: Rename Table not allowed as bootstrap dump in progress");
      throw new HiveException("Rename Table: Not allowed as bootstrap dump in progress");
    }

    return super.execute();
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    HiveTableName.setFrom(desc.getNewName(), table);
  }

  @Override
  protected void checkValidity(Table table, DDLOperationContext context) throws HiveException {
    table.validateName(context.getConf());
  }
}
