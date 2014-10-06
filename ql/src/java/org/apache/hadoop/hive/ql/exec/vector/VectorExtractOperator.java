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
  
  private transient int [] projectedColumns = null;

  public VectorExtractOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    this.conf = (ExtractDesc) conf;
  }

  public VectorExtractOperator() {
    super();
  }

  private StructObjectInspector makeStandardStructObjectInspector(StructObjectInspector structObjectInspector) {
    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ArrayList<String> colNames = new ArrayList<String>();
    for (StructField field: fields) {
      colNames.add(field.getFieldName());
      ois.add(field.getFieldObjectInspector());
    }
    return ObjectInspectorFactory
              .getStandardStructObjectInspector(colNames, ois);
    }
 
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    outputObjInspector = inputObjInspectors[0];
    LOG.info("VectorExtractOperator class of outputObjInspector is " + outputObjInspector.getClass().getName());
    projectedColumns = new int [valueColCount];
    for (int i = 0; i < valueColCount; i++) {
      projectedColumns[i] = keyColCount + i;
    }
    initializeChildren(hconf);
  }

  public void setKeyAndValueColCounts(int keyColCount, int valueColCount) {
      this.keyColCount = keyColCount;
      this.valueColCount = valueColCount;
  }
  
  @Override
  // Evaluate vectorized batches of rows and forward them.
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    // Project away the key columns...
    int[] originalProjections = vrg.projectedColumns;
    int originalProjectionSize = vrg.projectionSize;
    vrg.projectionSize = valueColCount;
    vrg.projectedColumns = this.projectedColumns;

    forward(vrg, outputObjInspector);

    // Revert the projected columns back, because vrg will be re-used.
    vrg.projectionSize = originalProjectionSize;
    vrg.projectedColumns = originalProjections;
  }
}
