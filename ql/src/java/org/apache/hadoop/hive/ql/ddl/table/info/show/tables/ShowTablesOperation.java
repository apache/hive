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

package org.apache.hadoop.hive.ql.ddl.table.info.show.tables;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.udf.UDFLike;

/**
 * Operation process showing the tables.
 */
public class ShowTablesOperation extends DDLOperation<ShowTablesDesc> {
  public ShowTablesOperation(DDLOperationContext context, ShowTablesDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (!context.getDb().databaseExists(desc.getDbName())) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, desc.getDbName());
    }

    if (!desc.isExtended()) {
      showTables();
    } else {
      showTablesExtended();
    }

    return 0;
  }

  private void showTables() throws HiveException {
    List<String> tableNames = new ArrayList<>(
        context.getDb().getTablesByType(desc.getDbName(), null, desc.getTypeFilter()));
    if (desc.getPattern() != null) {
      Pattern pattern = Pattern.compile(UDFLike.likePatternToRegExp(desc.getPattern()), Pattern.CASE_INSENSITIVE);
      tableNames = tableNames.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toList());
    }
    Collections.sort(tableNames);
    LOG.debug("Found {} table(s) matching the SHOW TABLES statement.", tableNames.size());

    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      ShowTablesFormatter formatter = ShowTablesFormatter.getFormatter(context.getConf());
      formatter.showTables(os, tableNames);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database " + desc.getDbName());
    }
  }

  private void showTablesExtended() throws HiveException {
    List<Table> tableObjects = new ArrayList<>();
    tableObjects.addAll(context.getDb().getTableObjects(desc.getDbName(), null, desc.getTypeFilter()));
    if (desc.getPattern() != null) {
      Pattern pattern = Pattern.compile(UDFLike.likePatternToRegExp(desc.getPattern()), Pattern.CASE_INSENSITIVE);
      tableObjects = tableObjects.stream()
          .filter(object -> pattern.matcher(object.getTableName()).matches())
          .collect(Collectors.toList());
    }
    Collections.sort(tableObjects, Comparator.comparing(Table::getTableName));
    LOG.debug("Found {} table(s) matching the SHOW EXTENDED TABLES statement.", tableObjects.size());

    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      ShowTablesFormatter formatter = ShowTablesFormatter.getFormatter(context.getConf());
      formatter.showTablesExtended(os, tableObjects);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database " + desc.getDbName());
    }
  }
}
