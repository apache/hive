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
package org.apache.hadoop.hive.ql.exec.vector.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * A VectorUDFAdaptor is a vectorized expression for invoking a custom
 * UDF on zero or more input vectors or constants which are the function arguments.
 */
public class VectorUDFAdaptor extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private String resultType;
  private VectorUDFArgDesc[] argDescs;
  private ExprNodeGenericFuncDesc expr;
  private boolean suppressEvaluateExceptions;

  private transient GenericUDF genericUDF;
  private transient GenericUDF.DeferredObject[] deferredChildren;
  private transient TypeInfo outputTypeInfo;
  private transient VectorAssignRow outputVectorAssignRow;
  private transient ObjectInspector[] childrenOIs;
  private transient VectorExpressionWriter[] writers;

  public VectorUDFAdaptor() {
    super();
  }

  public VectorUDFAdaptor (
      ExprNodeGenericFuncDesc expr,
      int outputColumnNum,
      String resultType,
      VectorUDFArgDesc[] argDescs) throws HiveException {

    super(-1, outputColumnNum);
    this.expr = expr;
    this.resultType = resultType;
    this.argDescs = argDescs;
  }

  public void setSuppressEvaluateExceptions(boolean suppressEvaluateExceptions) {
    this.suppressEvaluateExceptions = suppressEvaluateExceptions;
  }

  // Initialize transient fields. To be called after deserialization of other fields.
  public void init() throws HiveException, UDFArgumentException {
    genericUDF = expr.getGenericUDF();
    deferredChildren = new GenericUDF.DeferredObject[expr.getChildren().size()];
    childrenOIs = new ObjectInspector[expr.getChildren().size()];
    writers = VectorExpressionWriterFactory.getExpressionWriters(expr.getChildren());
    for (int i = 0; i < childrenOIs.length; i++) {
      childrenOIs[i] = writers[i].getObjectInspector();
    }
    MapredContext context = MapredContext.get();
    if (context != null) {
      context.setup(genericUDF);
    }
    outputTypeInfo = expr.getTypeInfo();
    outputVectorAssignRow = new VectorAssignRow();
    outputVectorAssignRow.init(outputTypeInfo, outputColumnNum);

    genericUDF.initialize(childrenOIs);

    // Initialize constant arguments
    for (int i = 0; i < argDescs.length; i++) {
      if (argDescs[i].isConstant()) {
        argDescs[i].prepareConstant();
      }
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (genericUDF == null) {
      try {
        init();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int[] sel = batch.selected;
    int n = batch.size;
    ColumnVector outV = batch.cols[outputColumnNum];

    // If the output column is of type string, initialize the buffer to receive data.
    if (outV instanceof BytesColumnVector) {
      ((BytesColumnVector) outV).initBuffer();
    }

    if (n == 0) {
      //Nothing to do
      return;
    }

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    /* If all input columns are repeating, just evaluate function
     * for row 0 in the batch and set output repeating.
     */
    if (allInputColsRepeating(batch)) {
      setResult(0, batch);
      batch.cols[outputColumnNum].isRepeating = true;
      return;
    } else {
      batch.cols[outputColumnNum].isRepeating = false;
    }

    if (batch.selectedInUse) {
      for(int j = 0; j != n; j++) {
        int i = sel[j];
        setResult(i, batch);
      }
    } else {
      for (int i = 0; i != n; i++) {
        setResult(i, batch);
      }
    }
  }

  /* Return false if any input column is non-repeating, otherwise true.
   * This returns false if all the arguments are constant or there
   * are zero arguments.
   *
   * A possible future optimization is to set the output to isRepeating
   * for cases of all-constant arguments for deterministic functions.
   */
  private boolean allInputColsRepeating(VectorizedRowBatch batch) {
    int varArgCount = 0;
    for (int i = 0; i < argDescs.length; i++) {
      if (argDescs[i].isVariable() && !batch.cols[argDescs[i].getColumnNum()].isRepeating) {
        return false;
      }
      varArgCount += 1;
    }
    if (varArgCount > 0) {
      return true;
    } else {
      return false;
    }
  }

  /* Calculate the function result for row i of the batch and
   * set the output column vector entry i to the result.
   */
  private void setResult(int i, VectorizedRowBatch b) throws HiveException {

    // get arguments
    for (int j = 0; j < argDescs.length; j++) {
      deferredChildren[j] = argDescs[j].getDeferredJavaObject(i, b, j, writers);
    }

    // call function
    Object result;
    if (!suppressEvaluateExceptions) {
      result = genericUDF.evaluate(deferredChildren);
    } else {
      try {
        result = genericUDF.evaluate(deferredChildren);
      } catch (HiveException e) {

        /* For UDFs that expect primitive types (like int instead of Integer or IntWritable),
         * this will catch the the exception that happens if they are passed a NULL value.
         * Then the default NULL handling logic will apply, and the result will be NULL.
         */
        result = null;
      }
    }

    // Set output column vector entry.  Since we have one output column, the logical index = 0.
    outputVectorAssignRow.assignRowColumn(
        b, /* batchIndex */ i, /* logicalColumnIndex */ 0, result);
  }

  @Override
  public String vectorExpressionParameters() {
    return expr.getExprString();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }

  public VectorUDFArgDesc[] getArgDescs() {
    return argDescs;
  }

  public void setArgDescs(final VectorUDFArgDesc[] argDescs) {
    this.argDescs = argDescs;
  }
}
