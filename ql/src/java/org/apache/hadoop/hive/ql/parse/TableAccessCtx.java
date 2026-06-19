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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This class implements the context information that is used for typechecking
 * phase in query compilation.
 */
public class TableAccessCtx implements NodeProcessorCtx {

  /**
   * Map of operator id to table and key names.
   */
  private final TableAccessInfo tableAccessInfo;

  /**
   * Constructor.
   */
  public TableAccessCtx() {
    tableAccessInfo = new TableAccessInfo();
  }

  public TableAccessInfo getTableAccessInfo() {
    return tableAccessInfo;
  }

  public void addOperatorTableAccess(Operator<? extends OperatorDesc> op,
      Map<String, List<String>> tableToKeysMap) {
    assert(tableToKeysMap != null);
    assert(op != null);
    tableAccessInfo.add(op, tableToKeysMap);
  }

}
