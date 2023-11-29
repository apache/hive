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
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public class NativeAcidMultiInsertSqlGenerator extends MultiInsertSqlGenerator {
  public NativeAcidMultiInsertSqlGenerator(Table table, String targetTableFullName, HiveConf conf, String subQueryAlias) {
    super(table, targetTableFullName, conf, subQueryAlias);
  }

  @Override
  public void appendAcidSelectColumns(Context.Operation operation) {
    queryStr.append("ROW__ID,");
    for (FieldSchema fieldSchema : targetTable.getPartCols()) {
      String identifier = HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
      queryStr.append(identifier);
      queryStr.append(",");
    }
  }

  @Override
  public List<String> getDeleteValues(Context.Operation operation) {
    List<String> deleteValues = new ArrayList<>(1 + targetTable.getPartCols().size());
    deleteValues.add(qualify("ROW__ID"));
    for (FieldSchema fieldSchema : targetTable.getPartCols()) {
      deleteValues.add(qualify(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf)));
    }
    return deleteValues;
  }

  @Override
  public List<String> getSortKeys() {
    return singletonList(qualify("ROW__ID"));
  }
}
