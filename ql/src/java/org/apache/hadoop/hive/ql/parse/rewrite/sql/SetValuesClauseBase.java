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
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

public abstract class SetValuesClauseBase {

  private final HiveConf conf;
  private final BinaryOperator<String> rhsExpValueFormatter;

  protected SetValuesClauseBase(HiveConf conf) {
    this(conf, (newValue, alias) -> newValue);
  }

  protected SetValuesClauseBase(HiveConf conf, BinaryOperator<String> rhsExpValueFormatter) {
    this.conf = conf;
    this.rhsExpValueFormatter = rhsExpValueFormatter;
  }

  public abstract void addValues(
      Table targetTable, String targetAlias, Map<String, String> newValues, List<String> values);

  protected void setColumnValue(
      String targetAlias, Map<String, String> newValues, List<String> values, FieldSchema fieldSchema) {
    if (newValues.containsKey(fieldSchema.getName())) {
      String rhsExp = newValues.get(fieldSchema.getName());
      values.add(rhsExpValueFormatter.apply(rhsExp, formatValue(targetAlias, fieldSchema.getName())));
    } else {
      values.add(formatValue(targetAlias, fieldSchema.getName()));
    }
  }

  protected String formatValue(String targetAlias, String name) {
    return String.format("%s.%s", targetAlias, HiveUtils.unparseIdentifier(name, conf));
  }
}
