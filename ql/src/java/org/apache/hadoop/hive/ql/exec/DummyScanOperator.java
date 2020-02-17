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

package org.apache.hadoop.hive.ql.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;

/**
 * The DummyScanOperator is just a dummy operator that doesn't do much.
 * But every execution needs to have a TableScanOperator, so this operator
 * serves the purpose of the plugins.  The TableScanOperator also needs
 * the colinfos to be set.
 * CDPD-8391: Refactor and get rid of this at some point.
 */
public class DummyScanOperator extends TableScanOperator {
  public DummyScanOperator(CompilationOpContext opContext, RowSchema schema) {
    super(opContext);
    this.setSchema(schema);
  }

  @Override
  public void process(Object row, int tag) {
  }

  @Override
  public OperatorType getType() {
    return null;
  }

  @Override
  public String getName() {
    return "DummyScanOperator";
  }

  @Override
  protected void initializeOp(Configuration conf) {
  }

  private ArrayList<ColumnInfo> getColInfos(RelNode relNode) {
    ArrayList<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();
    for (int i = 0; i < relNode.getRowType().getFieldList().size(); ++i) {
      String fieldName = relNode.getRowType().getFieldNames().get(i);
      RelDataType fieldType = relNode.getRowType().getFieldList().get(i).getType();
      TypeInfo typeInfo = TypeConverter.convert(fieldType);
      columnInfos.add(new ColumnInfo(fieldName, typeInfo, "", false));
    }
    return columnInfos;
  }
}

