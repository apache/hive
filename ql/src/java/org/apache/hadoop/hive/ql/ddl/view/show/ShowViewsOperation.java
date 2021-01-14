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

package org.apache.hadoop.hive.ql.ddl.view.show;

import java.io.DataOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.table.info.show.tables.ShowTablesFormatter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFLike;

/**
 * Operation process showing the views.
 */
public class ShowViewsOperation extends DDLOperation<ShowViewsDesc> {
  public ShowViewsOperation(DDLOperationContext context, ShowViewsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (!context.getDb().databaseExists(desc.getDbName())) {
      throw new HiveException(ErrorMsg.DATABASE_NOT_EXISTS, desc.getDbName());
    }

    List<String> viewNames = context.getDb().getTablesByType(desc.getDbName(), null, TableType.VIRTUAL_VIEW);
    if (desc.getPattern() != null) {
      Pattern pattern = Pattern.compile(UDFLike.likePatternToRegExp(desc.getPattern()), Pattern.CASE_INSENSITIVE);
      viewNames = viewNames.stream()
          .filter(name -> pattern.matcher(name).matches())
          .collect(Collectors.toList());
    }
    Collections.sort(viewNames);
    LOG.debug("Found {} view(s) matching the SHOW VIEWS statement.", viewNames.size());

    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      ShowTablesFormatter formatter = ShowTablesFormatter.getFormatter(context.getConf());
      formatter.showTables(os, viewNames);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "in database" + desc.getDbName());
    }

    return 0;
  }
}
