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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.io.Serializable;
import java.util.List;

/**
 * Holds structures required for runtime values and mappings.
 */
public class RuntimeValuesInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private TableDesc tableDesc;
  private List<String> dynamicValueIDs;
  private List<ExprNodeDesc> colExprs;
  /**
   * Column expressions of the (target) table being filtered by the semi-join.
   */
  private List<ExprNodeDesc> targetColumns;

  // get-set methods
  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  public List<String> getDynamicValueIDs() {
    return dynamicValueIDs;
  }

  public void setDynamicValueIDs(List<String> dynamicValueIDs) {
    this.dynamicValueIDs = dynamicValueIDs;
  }

  public List<ExprNodeDesc> getColExprs() {
    return colExprs;
  }

  public void setColExprs(List<ExprNodeDesc> colExprs) {
    this.colExprs = colExprs;
  }

  public List<ExprNodeDesc> getTargetColumns() {
    return targetColumns;
  }

  public void setTargetColumns(List<ExprNodeDesc> targetColumns) {
    this.targetColumns = targetColumns;
  }
}

