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

package org.apache.hadoop.hive.ql.ddl.table;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.IOUtils;

/**
 * Operation process showing the tables.
 */
public class ShowTablesOperation extends DDLOperation {
  private final ShowTablesDesc desc;

  public ShowTablesOperation(DDLOperationContext context, ShowTablesDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    TableType type       = desc.getType(); // null for tables, VIRTUAL_VIEW for views, MATERIALIZED_VIEW for MVs
    String dbName        = desc.getDbName();
    String pattern       = desc.getPattern(); // if null, all tables/views are returned
    String resultsFile   = desc.getResFile();

    if (!context.getDb().databaseExists(dbName)) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    LOG.debug("pattern: {}", pattern);

    List<String> tablesOrViews = null;
    List<Table> materializedViews = null;
    if (type == null) {
      tablesOrViews = new ArrayList<>();
      tablesOrViews.addAll(context.getDb().getTablesByType(dbName, pattern, TableType.MANAGED_TABLE));
      tablesOrViews.addAll(context.getDb().getTablesByType(dbName, pattern, TableType.EXTERNAL_TABLE));
      LOG.debug("Found {} table(s) matching the SHOW TABLES statement.", tablesOrViews.size());
    } else if (type == TableType.MATERIALIZED_VIEW) {
      materializedViews = new ArrayList<>();
      materializedViews.addAll(context.getDb().getMaterializedViewObjectsByPattern(dbName, pattern));
      LOG.debug("Found {} materialized view(s) matching the SHOW MATERIALIZED VIEWS statement.",
          materializedViews.size());
    } else if (type == TableType.VIRTUAL_VIEW) {
      tablesOrViews = context.getDb().getTablesByType(dbName, pattern, type);
      LOG.debug("Found {} view(s) matching the SHOW VIEWS statement.", tablesOrViews.size());
    } else {
      throw new HiveException("Option not recognized in SHOW TABLES/VIEWS/MATERIALIZED VIEWS");
    }

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(resultsFile);
      FileSystem fs = resFile.getFileSystem(context.getConf());
      outStream = fs.create(resFile);
      // Sort by name and print
      if (tablesOrViews != null) {
        SortedSet<String> sortedSet = new TreeSet<String>(tablesOrViews);
        context.getFormatter().showTables(outStream, sortedSet);
      } else {
        Collections.sort(materializedViews, Comparator.comparing(Table::getTableName));
        context.getFormatter().showMaterializedViews(outStream, materializedViews);
      }
      outStream.close();
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database" + dbName);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }
}
