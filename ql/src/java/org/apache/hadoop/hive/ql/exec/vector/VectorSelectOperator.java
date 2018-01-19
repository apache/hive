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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Select operator implementation.
 */
public class VectorSelectOperator extends Operator<SelectDesc>
    implements VectorizationOperator, VectorizationContextRegion {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorSelectDesc vectorDesc;

  private VectorExpression[] vExpressions = null;

  private int [] projectedOutputColumns = null;

  private transient VectorExpressionWriter [] valueWriters = null;

  // Create a new outgoing vectorization context because column name map will change.
  private VectorizationContext vOutContext;

  public VectorSelectOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc)
          throws HiveException {
    this(ctx);
    this.conf = (SelectDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorSelectDesc) vectorDesc;
    vExpressions = this.vectorDesc.getSelectExpressions();
    projectedOutputColumns = this.vectorDesc.getProjectedOutputColumns();

    /**
     * Create a new vectorization context to create a new projection, but keep
     * same output column manager must be inherited to track the scratch the columns.
     * Some of which may be the input columns for this operator.
     */
    vOutContext = new VectorizationContext(getName(), vContext);

    // NOTE: We keep the TypeInfo and dataTypePhysicalVariation arrays.
    vOutContext.resetProjectionColumns();
    List<String> outputColumnNames = this.conf.getOutputColumnNames();
    for (int i=0; i < projectedOutputColumns.length; ++i) {
      String columnName = outputColumnNames.get(i);
      vOutContext.addProjectionColumn(columnName, projectedOutputColumns[i]);
    }
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorSelectOperator() {
    super();
  }

  public VectorSelectOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      return;
    }
    VectorExpression.doTransientInit(vExpressions);

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    List<ExprNodeDesc> colList = conf.getColList();
    valueWriters = VectorExpressionWriterFactory.getExpressionWriters(colList);
    for (VectorExpressionWriter vew : valueWriters) {
      objectInspectors.add(vew.getObjectInspector());
    }

    List<String> outputFieldNames = conf.getOutputColumnNames();
    outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        outputFieldNames, objectInspectors);
  }

  // Must send on to VectorPTFOperator...
  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setNextVectorBatchGroupStatus(isLastGroupBatch);
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      forward(row, inputObjInspectors[tag], true);
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
    int[] originalProjections = vrg.projectedColumns;
    int originalProjectionSize = vrg.projectionSize;
    vrg.projectionSize = projectedOutputColumns.length;
    vrg.projectedColumns = this.projectedOutputColumns;
    forward(vrg, outputObjInspector, true);

    // Revert the projected columns back, because vrg will be re-used.
    vrg.projectionSize = originalProjectionSize;
    vrg.projectedColumns = originalProjections;
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
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SELECT;
  }

  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "SEL";
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

}
