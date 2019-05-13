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

package org.apache.hadoop.hive.ql.ddl.table.partition;

import java.io.DataOutputStream;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of showing the partitions of a table.
 */
public class ShowPartitionsOperation extends DDLOperation {
  private final ShowPartitionsDesc desc;

  public ShowPartitionsOperation(DDLOperationContext context, ShowPartitionsDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    Table tbl = context.getDb().getTable(desc.getTabName());
    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, desc.getTabName());
    }

    List<String> parts = null;
    if (desc.getPartSpec() != null) {
      parts = context.getDb().getPartitionNames(tbl.getDbName(), tbl.getTableName(), desc.getPartSpec(), (short) -1);
    } else {
      parts = context.getDb().getPartitionNames(tbl.getDbName(), tbl.getTableName(), (short) -1);
    }

    // write the results in the file
    try (DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      context.getFormatter().showTablePartitions(outStream, parts);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show partitions for table " + desc.getTabName());
    }

    return 0;
  }
}
