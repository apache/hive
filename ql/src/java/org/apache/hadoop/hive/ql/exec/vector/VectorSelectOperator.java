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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * Select operator implementation.
 */
public class VectorSelectOperator extends Operator<SelectDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  protected transient VectorExpression[] vExpressions;

  private final VectorizationContext vContext;

  private int [] projectedColumns = null;

  private VectorExpressionWriter [] valueWriters = null;

  public VectorSelectOperator(VectorizationContext ctxt, OperatorDesc conf) {
    this.vContext = ctxt;
    this.conf = (SelectDesc) conf;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      initializeChildren(hconf);
      return;
    }

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    List<ExprNodeDesc> colList = conf.getColList();
    vContext.setOperatorType(OperatorType.SELECT);
    vExpressions = new VectorExpression[colList.size()];
    for (int i = 0; i < colList.size(); i++) {
      vExpressions[i] = vContext.getVectorExpression(colList.get(i));
      String columnName = conf.getOutputColumnNames().get(i);
      // Update column map with output column names
      vContext.addToColumnMap(columnName, vExpressions[i].getOutputColumn());
    }
    valueWriters = VectorExpressionWriterFactory.getExpressionWriters(colList);
    for (VectorExpressionWriter vew : valueWriters) {
      objectInspectors.add(vew.getObjectInspector());
    }

    List<String> outputFieldNames = conf.getOutputColumnNames();
    outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        outputFieldNames, objectInspectors);

    initializeChildren(hconf);
    projectedColumns = new int [vExpressions.length];
    for (int i = 0; i < projectedColumns.length; i++) {
      projectedColumns[i] = vExpressions[i].getOutputColumn();
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      forward(row, inputObjInspectors[tag]);
      return;
    }

    VectorizedRowBatch vrg = (VectorizedRowBatch) row;
    for (int i = 0; i < vExpressions.length; i++) {
      try {
        vExpressions[i].evaluate(vrg);
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }

    // Prepare output, set the projections
    VectorExpressionWriter [] originalValueWriters = vrg.valueWriters;
    vrg.setValueWriters(valueWriters);
    int[] originalProjections = vrg.projectedColumns;
    int originalProjectionSize = vrg.projectionSize;
    vrg.projectionSize = vExpressions.length;
    vrg.projectedColumns = this.projectedColumns;
    forward(vrg, outputObjInspector);

    // Revert the projected columns back, because vrg will be re-used.
    vrg.projectionSize = originalProjectionSize;
    vrg.projectedColumns = originalProjections;
    vrg.valueWriters = originalValueWriters;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "SEL";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SELECT;
  }
}
