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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

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
    VectorizationContextRegion {

  private static final long serialVersionUID = 1L;

  protected VectorExpression[] vExpressions = null;

  private transient int [] projectedColumns = null;

  private transient VectorExpressionWriter [] valueWriters = null;

  // Create a new outgoing vectorization context because column name map will change.
  private VectorizationContext vOutContext;

  public VectorSelectOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this.conf = (SelectDesc) conf;
    List<ExprNodeDesc> colList = this.conf.getColList();
    vExpressions = new VectorExpression[colList.size()];
    for (int i = 0; i < colList.size(); i++) {
      ExprNodeDesc expr = colList.get(i);
      VectorExpression ve = vContext.getVectorExpression(expr);
      vExpressions[i] = ve;
    }

    /**
     * Create a new vectorization context to create a new projection, but keep
     * same output column manager must be inherited to track the scratch the columns.
     */
    vOutContext = new VectorizationContext(getName(), vContext);

    vOutContext.resetProjectionColumns();
    for (int i=0; i < colList.size(); ++i) {
      String columnName = this.conf.getOutputColumnNames().get(i);
      VectorExpression ve = vExpressions[i];
      vOutContext.addProjectionColumn(columnName,
              ve.getOutputColumn());
    }
  }

  public VectorSelectOperator() {
  }

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      return null;
    }

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    List<ExprNodeDesc> colList = conf.getColList();
    valueWriters = VectorExpressionWriterFactory.getExpressionWriters(colList);
    for (VectorExpressionWriter vew : valueWriters) {
      objectInspectors.add(vew.getObjectInspector());
    }

    List<String> outputFieldNames = conf.getOutputColumnNames();
    outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        outputFieldNames, objectInspectors);

    projectedColumns = new int [vExpressions.length];
    for (int i = 0; i < projectedColumns.length; i++) {
      projectedColumns[i] = vExpressions[i].getOutputColumn();
    }
    return result;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

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

  static public String getOperatorName() {
    return "SEL";
  }

  public VectorExpression[] getvExpressions() {
    return vExpressions;
  }

  public VectorExpression[] getVExpressions() {
    return vExpressions;
  }

  public void setvExpressions(VectorExpression[] vExpressions) {
    this.vExpressions = vExpressions;
  }

  public void setVExpressions(VectorExpression[] vExpressions) {
    this.vExpressions = vExpressions;
  }

  @Override
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SELECT;
  }
}
