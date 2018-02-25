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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorTableScanDesc.
 *
 * Extra parameters beyond TableScanDesc just for the VectorTableScanOperator.
 *
 * We don't extend TableScanDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorTableScanDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  private int[] projectedColumns;
  private String[] projectedColumnNames;
  private TypeInfo[] projectedColumnTypeInfos;
  private DataTypePhysicalVariation[] projectedColumnDataTypePhysicalVariation;

  public VectorTableScanDesc() {
  }

  public void setProjectedColumns(int[] projectedColumns) {
    this.projectedColumns = projectedColumns;
  }

  public int[] getProjectedColumns() {
    return projectedColumns;
  }

  public void setProjectedColumnNames(String[] projectedColumnNames) {
    this.projectedColumnNames = projectedColumnNames;
  }

  public String[] getProjectedColumnNames() {
    return projectedColumnNames;
  }

  public void setProjectedColumnTypeInfos(TypeInfo[] projectedColumnTypeInfos) {
    this.projectedColumnTypeInfos = projectedColumnTypeInfos;
  }

  public TypeInfo[] getProjectedColumnTypeInfos() {
    return projectedColumnTypeInfos;
  }

  public void setProjectedColumnDataTypePhysicalVariations(
      DataTypePhysicalVariation[] projectedColumnDataTypePhysicalVariation) {
    this.projectedColumnDataTypePhysicalVariation =
        projectedColumnDataTypePhysicalVariation;
  }

  public DataTypePhysicalVariation[] getProjectedColumnDataTypePhysicalVariations() {
    return projectedColumnDataTypePhysicalVariation;
  }
}
