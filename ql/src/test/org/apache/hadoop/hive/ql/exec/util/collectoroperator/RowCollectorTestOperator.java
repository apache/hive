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

package org.apache.hadoop.hive.ql.exec.util.collectoroperator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public abstract class RowCollectorTestOperator extends RowCollectorTestOperatorBase {

  private static final long serialVersionUID = 1L;

  private final ObjectInspector[] outputObjectInspectors;
  private final int columnSize;

  public RowCollectorTestOperator(ObjectInspector[] outputObjectInspectors) {
    super();
    this.outputObjectInspectors = outputObjectInspectors;
    columnSize = outputObjectInspectors.length;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    rowCount++;
    Object[] resultObjectArray = new Object[columnSize];
    if (row instanceof ArrayList) {
      List<Object> rowObjectList = (ArrayList<Object>) row;
      for (int c = 0; c < columnSize; c++) {
        resultObjectArray[c] =
            ((PrimitiveObjectInspector) outputObjectInspectors[c]).copyObject(rowObjectList.get(c));
      }
    } else {
      Object[] rowObjectArray = (Object[]) row;
      for (int c = 0; c < columnSize; c++) {
        resultObjectArray[c] =
            ((PrimitiveObjectInspector) outputObjectInspectors[c]).copyObject(rowObjectArray[c]);
      }
    }
    nextTestRow(new RowTestObjects(resultObjectArray));
  }

  @Override
  public String getName() {
    return RowCollectorTestOperator.class.getSimpleName();
  }
}