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
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


/**
 * Join operator implementation.
 */
public class JoinOperator extends CommonJoinOperator<joinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    initializeChildren(hconf);
  }
  
  public void processOp(Object row, int tag)
      throws HiveException {
    try {
      // get alias
      alias = (byte)tag;
    
      if ((lastAlias == null) || (!lastAlias.equals(alias)))
        nextSz = joinEmitInterval;
      
      ArrayList<Object> nr = computeValues(row, joinValues.get(alias), joinValuesObjectInspectors.get(alias));
      
      // number of rows for the key in the given table
      int sz = storage.get(alias).size();
    
      // Are we consuming too much memory
      if (alias == numAliases - 1) {
        if (sz == joinEmitInterval) {
          // The input is sorted by alias, so if we are already in the last join operand,
          // we can emit some results now.
          // Note this has to be done before adding the current row to the storage,
          // to preserve the correctness for outer joins.
          checkAndGenObject();
          storage.get(alias).clear();
        }
      } else {
        if (sz == nextSz) {
          // Output a warning if we reached at least 1000 rows for a join operand
          // We won't output a warning for the last join operand since the size
          // will never goes to joinEmitInterval.
          StructObjectInspector soi = (StructObjectInspector)inputObjInspectors[tag];
          StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY.toString());
          Object keyObject = soi.getStructFieldData(row, sf);
          LOG.warn("table " + alias + " has " + sz + " rows for join key " + keyObject);
          nextSz = getNextSize(nextSz);
        }
      }
    
      // Add the value to the vector
      storage.get(alias).add(nr);
    
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }
  
  public int getType() {
    return OperatorType.JOIN;
  }

  
}

