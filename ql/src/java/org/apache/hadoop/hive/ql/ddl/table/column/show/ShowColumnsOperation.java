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

package org.apache.hadoop.hive.ql.ddl.table.column.show;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.ShowUtils.TextMetaDataTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of showing the columns.
 */
public class ShowColumnsOperation extends DDLOperation<ShowColumnsDesc> {
  public ShowColumnsOperation(DDLOperationContext context, ShowColumnsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // write the results in the file
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      List<FieldSchema> columns = getColumnsByPattern();
      writeColumns(outStream, columns);
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    return 0;
  }

  private List<FieldSchema> getColumnsByPattern() throws HiveException {
    List<FieldSchema> columns = getCols();
    Matcher matcher = getMatcher();
    return filterColumns(columns, matcher);
  }

  private List<FieldSchema> getCols() throws HiveException {
    Table table = context.getDb().getTable(desc.getTableName());
    List<FieldSchema> allColumns = new ArrayList<>();
    allColumns.addAll(table.getCols());
    allColumns.addAll(table.getPartCols());
    return allColumns;
  }

  private Matcher getMatcher() {
    String columnPattern = desc.getPattern();
    if (columnPattern == null) {
      columnPattern = "*";
    }
    columnPattern = columnPattern.toLowerCase();
    columnPattern = columnPattern.replaceAll("\\*", ".*");

    Pattern pattern = Pattern.compile(columnPattern);
    return pattern.matcher("");
  }

  private List<FieldSchema> filterColumns(List<FieldSchema> columns, Matcher matcher) {
    ArrayList<FieldSchema> result = new ArrayList<>();
    for (FieldSchema column : columns) {
      matcher.reset(column.getName());
      if (matcher.matches()) {
        result.add(column);
      }
    }

    if (desc.isSorted()) {
      result.sort(
          new Comparator<FieldSchema>() {
            @Override
            public int compare(FieldSchema f1, FieldSchema f2) {
              return f1.getName().compareTo(f2.getName());
            }
          });
    }
    return result;
  }

  private void writeColumns(DataOutputStream outStream, List<FieldSchema> columns) throws IOException {
    TextMetaDataTable tmd = new TextMetaDataTable();
    for (FieldSchema fieldSchema : columns) {
      // For show Columns operation, we just need the column name.
      tmd.addRow(fieldSchema.getName());
    }

    // In case the query is served by HiveServer2, don't pad it with spaces,
    // as HiveServer2 output is consumed by JDBC/ODBC clients.
    boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
    outStream.writeBytes(tmd.renderTable(isOutputPadded));
  }
}
