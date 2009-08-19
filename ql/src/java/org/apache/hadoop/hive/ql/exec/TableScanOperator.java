/**
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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;

/**
 * Table Scan Operator
 * If the data is coming from the map-reduce framework, just forward it.
 * This will be needed as part of local work when data is not being read as part of map-reduce framework
 **/
public class TableScanOperator extends Operator<tableScanDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Currently, the table scan operator does not do anything special other than just forwarding the row. Since the 
   * table data is always read as part of the map-reduce framework by the mapper. But, this assumption is not true,
   * i.e table data is not only read by the mapper, this operator will be enhanced to read the table.
   **/
  @Override
  public void processOp(Object row, int tag)
      throws HiveException {
    forward(row, inputObjInspectors[tag]);    
  }

  /**
   * The operator name for this operator type. This is used to construct the rule for an operator
   * @return the operator name
   **/
  public String getName() {
    return new String("TS");
  }
  
  // this 'neededColumnIDs' field is included in this operator class instead of
  // its desc class.The reason is that 1)tableScanDesc can not be instantiated,
  // and 2) it will fail some join and union queries if this is added forcibly
  // into tableScanDesc
  java.util.ArrayList<Integer> neededColumnIDs;

  public void setNeededColumnIDs(java.util.ArrayList<Integer> orign_columns) {
    this.neededColumnIDs = orign_columns;
  }

  public java.util.ArrayList<Integer> getNeededColumnIDs() {
    return neededColumnIDs;
  }

}
