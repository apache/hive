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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of turning of skewing.
 */
public class AlterTableNotSkewedOperation extends AbstractAlterTableOperation<AlterTableNotSkewedDesc> {
  public AlterTableNotSkewedOperation(DDLOperationContext context, AlterTableNotSkewedDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    // Validation's been done at compile time. no validation is needed here.
    // Convert skewed table to non-skewed table.
    if (table.getSkewedInfo() == null) {
      table.setSkewedInfo(new SkewedInfo());
    }
    table.setSkewedColNames(new ArrayList<String>());
    table.setSkewedColValues(new ArrayList<List<String>>());

    table.setStoredAsSubDirectories(false);
  }
}
