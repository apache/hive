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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Vectorized extract operator implementation.  Consumes rows and outputs a
 * vectorized batch of subobjects.
 **/
public class VectorExtractOperator extends ExtractOperator {
  private static final long serialVersionUID = 1L;

  private int keyColCount;
  private int valueColCount;
  
  private transient VectorizedRowBatch outputBatch;
  private transient int remainingColCount;

  public VectorExtractOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    this.conf = (ExtractDesc) conf;
  }

  public VectorExtractOperator() {
    super();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    StructObjectInspector structInputObjInspector = (StructObjectInspector) inputObjInspectors[0];
    List<? extends StructField> fields = structInputObjInspector.getAllStructFieldRefs();
    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ArrayList<String> colNames = new ArrayList<String>();
    for (int i = keyColCount; i < fields.size(); i++) {
      StructField field = fields.get(i);
      String fieldName = field.getFieldName();

      // Remove "VALUE." prefix.
      int dotIndex = fieldName.indexOf(".");
      colNames.add(fieldName.substring(dotIndex + 1));
      ois.add(field.getFieldObjectInspector());
    }
    outputObjInspector = ObjectInspectorFactory
              .getStandardStructObjectInspector(colNames, ois);
    remainingColCount = fields.size() - keyColCount;
    outputBatch =  new VectorizedRowBatch(remainingColCount);
    initializeChildren(hconf);
  }

  public void setKeyAndValueColCounts(int keyColCount, int valueColCount) {
      this.keyColCount = keyColCount;
      this.valueColCount = valueColCount;
  }
  
  @Override
  // Remove the key columns and forward the values (and scratch columns).
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch inputBatch = (VectorizedRowBatch) row;

    // Copy references to the input columns array starting after the keys...
    for (int i = 0; i < remainingColCount; i++) {
      outputBatch.cols[i] = inputBatch.cols[keyColCount + i];
    }
    outputBatch.size = inputBatch.size;

    forward(outputBatch, outputObjInspector);
  }
}
