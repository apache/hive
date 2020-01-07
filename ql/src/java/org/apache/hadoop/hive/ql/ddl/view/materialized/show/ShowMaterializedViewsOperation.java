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

package org.apache.hadoop.hive.ql.ddl.view.materialized.show;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process showing the materialized views.
 */
public class ShowMaterializedViewsOperation extends DDLOperation<ShowMaterializedViewsDesc> {
  public ShowMaterializedViewsOperation(DDLOperationContext context, ShowMaterializedViewsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String dbName = desc.getDbName();
    String pattern = desc.getPattern(); // if null, all tables/views are returned
    String resultsFile = desc.getResFile();

    if (!context.getDb().databaseExists(dbName)) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, dbName);
    }

    // the returned list is not sortable as it is immutable, thus it must be put into a new ArrayList
    List<Table> tableObjects = new ArrayList<>(context.getDb().getMaterializedViewObjectsByPattern(dbName, pattern));
    LOG.debug("Found {} materialized view(s) matching the SHOW MATERIALIZED VIEWS statement.", tableObjects.size());

    try (DataOutputStream os = DDLUtils.getOutputStream(new Path(resultsFile), context)) {
      Collections.sort(tableObjects, Comparator.comparing(Table::getTableName));
      context.getFormatter().showMaterializedViews(os, tableObjects);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database" + dbName);
    }

    return 0;
  }
}
