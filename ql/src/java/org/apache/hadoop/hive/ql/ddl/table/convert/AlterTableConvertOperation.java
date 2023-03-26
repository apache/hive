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

package org.apache.hadoop.hive.ql.ddl.table.convert;


import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;

/**
 * Operation process of ALTER TABLE ... CONVERT command
 */
public class AlterTableConvertOperation extends AbstractAlterTableOperation<AlterTableConvertDesc> {

  public AlterTableConvertOperation(DDLOperationContext context, AlterTableConvertDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    // Add the covert type
    String convertType = desc.getConvertSpec().getTargetType();
    if (convertType.equalsIgnoreCase("ICEBERG")) {
      table.getParameters().put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    } else {
      throw new SemanticException(
          "Conversion to type " + convertType + " is not supported via Alter table convert " + "command");
    }
    Map<String, String> params = table.getParameters();
    if (desc.getConvertSpec().getTblProperties() != null) {
      params.putAll(desc.getConvertSpec().getTblProperties());
    }
  }
}
