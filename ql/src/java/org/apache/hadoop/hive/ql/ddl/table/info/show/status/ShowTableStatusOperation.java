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

package org.apache.hadoop.hive.ql.ddl.table.info.show.status;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.table.info.show.status.formatter.ShowTableStatusFormatter;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.io.IOUtils;

/**
 * Operation process showing the table status.
 */
public class ShowTableStatusOperation extends DDLOperation<ShowTableStatusDesc> {
  public ShowTableStatusOperation(DDLOperationContext context, ShowTableStatusDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    List<Table> tables = new ArrayList<Table>();
    Map<String, String> partitionSpec = desc.getPartSpec();
    Partition partition = null;
    if (partitionSpec != null) {
      Table table = context.getDb().getTable(desc.getDbName(), desc.getPattern());
      partition = context.getDb().getPartition(table, partitionSpec, false);
      if (partition == null) {
        throw new HiveException("Partition " + partitionSpec + " for table " + desc.getPattern() + " does not exist.");
      }
      tables.add(table);
    } else {
      LOG.debug("pattern: {}", desc.getPattern());
      List<String> tableNames = context.getDb().getTablesForDb(desc.getDbName(), null);
      if (desc.getPattern() != null) {
        Pattern pattern = Pattern.compile(UDFLike.likePatternToRegExp(desc.getPattern()), Pattern.CASE_INSENSITIVE);
        tableNames = tableNames.stream()
            .filter(name -> pattern.matcher(name).matches())
            .collect(Collectors.toList());
      }
      Collections.sort(tableNames);
      for (String tableName : tableNames) {
        Table table = context.getDb().getTable(desc.getDbName(), tableName);
        tables.add(table);
      }
      LOG.info("Found {} table(s) matching the SHOW TABLE EXTENDED statement.", tableNames.size());
    }

    DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      ShowTableStatusFormatter formatter = ShowTableStatusFormatter.getFormatter(context.getConf());
      formatter.showTableStatus(outStream, context.getDb(), context.getConf(), tables, partition);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show table status");
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }
}
