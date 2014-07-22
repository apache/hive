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
package org.apache.hadoop.hive.ql.exec.vector.udf;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Text;

/**
 * A VectorUDFAdaptor is a vectorized expression for invoking a custom
 * UDF on zero or more input vectors or constants which are the function arguments.
 */
public class VectorUDFAdaptor extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int outputColumn;
  private String resultType;
  private VectorUDFArgDesc[] argDescs;
  private ExprNodeGenericFuncDesc expr;

  private transient GenericUDF genericUDF;
  private transient GenericUDF.DeferredObject[] deferredChildren;
  private transient ObjectInspector outputOI;
  private transient ObjectInspector[] childrenOIs;
  private transient VectorExpressionWriter[] writers;

  public VectorUDFAdaptor() {
    super();
  }

  public VectorUDFAdaptor (
      ExprNodeGenericFuncDesc expr,
      int outputColumn,
      String resultType,
      VectorUDFArgDesc[] argDescs) throws HiveException {

    this();
    this.expr = expr;
    this.outputColumn = outputColumn;
    this.resultType = resultType;
    this.argDescs = argDescs;
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
    outputOI = VectorExpressionWriterFactory.genVectorExpressionWritable(expr)
        .getObjectInspector();

    genericUDF.initialize(childrenOIs);

    // Initialize constant arguments
    for (int i = 0; i < argDescs.length; i++) {
      if (argDescs[i].isConstant()) {
        argDescs[i].prepareConstant();
      }
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

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
    ColumnVector outV = batch.cols[outputColumn];

    // If the output column is of type string, initialize the buffer to receive data.
    if (outV instanceof BytesColumnVector) {
      ((BytesColumnVector) outV).initBuffer();
    }

    if (n == 0) {
      //Nothing to do
      return;
    }

    batch.cols[outputColumn].noNulls = true;

    /* If all input columns are repeating, just evaluate function
     * for row 0 in the batch and set output repeating.
     */
    if (allInputColsRepeating(batch)) {
      setResult(0, batch);
      batch.cols[outputColumn].isRepeating = true;
      return;
    } else {
      batch.cols[outputColumn].isRepeating = false;
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
  private void setResult(int i, VectorizedRowBatch b) {

    // get arguments
    for (int j = 0; j < argDescs.length; j++) {
      deferredChildren[j] = argDescs[j].getDeferredJavaObject(i, b, j, writers);
    }

    // call function
    Object result;
    try {
      result = genericUDF.evaluate(deferredChildren);
    } catch (HiveException e) {

      /* For UDFs that expect primitive types (like int instead of Integer or IntWritable),
       * this will catch the the exception that happens if they are passed a NULL value.
       * Then the default NULL handling logic will apply, and the result will be NULL.
       */
      result = null;
    }

    // set output column vector entry
    if (result == null) {
      b.cols[outputColumn].noNulls = false;
      b.cols[outputColumn].isNull[i] = true;
    } else {
      b.cols[outputColumn].isNull[i] = false;
      setOutputCol(b.cols[outputColumn], i, result);
    }
  }

  private void setOutputCol(ColumnVector colVec, int i, Object value) {

    /* Depending on the output type, get the value, cast the result to the
     * correct type if needed, and assign the result into the output vector.
     */
    if (outputOI instanceof WritableStringObjectInspector) {
      BytesColumnVector bv = (BytesColumnVector) colVec;
      Text t;
      if (value instanceof String) {
        t = new Text((String) value);
      } else {
        t = ((WritableStringObjectInspector) outputOI).getPrimitiveWritableObject(value);
      }
      bv.setVal(i, t.getBytes(), 0, t.getLength());
    } else if (outputOI instanceof WritableIntObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      if (value instanceof Integer) {
        lv.vector[i] = (Integer) value;
      } else {
        lv.vector[i] = ((WritableIntObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableLongObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      if (value instanceof Long) {
        lv.vector[i] = (Long) value;
      } else {
        lv.vector[i] = ((WritableLongObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableDoubleObjectInspector) {
      DoubleColumnVector dv = (DoubleColumnVector) colVec;
      if (value instanceof Double) {
        dv.vector[i] = (Double) value;
      } else {
        dv.vector[i] = ((WritableDoubleObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableFloatObjectInspector) {
      DoubleColumnVector dv = (DoubleColumnVector) colVec;
      if (value instanceof Float) {
        dv.vector[i] = (Float) value;
      } else {
        dv.vector[i] = ((WritableFloatObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableShortObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      if (value instanceof Short) {
        lv.vector[i] = (Short) value;
      } else {
        lv.vector[i] = ((WritableShortObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableByteObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      if (value instanceof Byte) {
        lv.vector[i] = (Byte) value;
      } else {
        lv.vector[i] = ((WritableByteObjectInspector) outputOI).get(value);
      }
    } else if (outputOI instanceof WritableTimestampObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      Timestamp ts;
      if (value instanceof Timestamp) {
        ts = (Timestamp) value;
      } else {
        ts = ((WritableTimestampObjectInspector) outputOI).getPrimitiveJavaObject(value);
      }
      /* Calculate the number of nanoseconds since the epoch as a long integer. By convention
       * that is how Timestamp values are operated on in a vector.
       */
      long l = ts.getTime() * 1000000  // Shift the milliseconds value over by 6 digits
                                       // to scale for nanosecond precision.
                                       // The milliseconds digits will by convention be all 0s.
            + ts.getNanos() % 1000000; // Add on the remaining nanos.
                                       // The % 1000000 operation removes the ms values
                                       // so that the milliseconds are not counted twice.
      lv.vector[i] = l;
    } else if (outputOI instanceof WritableDateObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      Date ts;
      if (value instanceof Date) {
        ts = (Date) value;
      } else {
        ts = ((WritableDateObjectInspector) outputOI).getPrimitiveJavaObject(value);
      }
      long l = DateWritable.dateToDays(ts);
      lv.vector[i] = l;
    } else if (outputOI instanceof WritableBooleanObjectInspector) {
      LongColumnVector lv = (LongColumnVector) colVec;
      if (value instanceof Boolean) {
        lv.vector[i] = (Boolean) value ? 1 : 0;
      } else {
        lv.vector[i] = ((WritableBooleanObjectInspector) outputOI).get(value) ? 1 : 0;
      }
    } else if (outputOI instanceof WritableHiveDecimalObjectInspector) {
      DecimalColumnVector dcv = (DecimalColumnVector) colVec;
      if (value instanceof HiveDecimal) {
        dcv.vector[i].update(((HiveDecimal) value).bigDecimalValue());
      } else {
        HiveDecimal hd = ((WritableHiveDecimalObjectInspector) outputOI).getPrimitiveJavaObject(value);
        dcv.vector[i].update(hd.bigDecimalValue());
      }
    } else {
      throw new RuntimeException("Unhandled object type " + outputOI.getTypeName());
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public String getOutputType() {
    return resultType;
  }

  public String getResultType() {
    return resultType;
  }

  public void setResultType(String resultType) {
    this.resultType = resultType;
  }

  public VectorUDFArgDesc[] getArgDescs() {
    return argDescs;
  }

  public void setArgDescs(VectorUDFArgDesc[] argDescs) {
    this.argDescs = argDescs;
  }

  public ExprNodeGenericFuncDesc getExpr() {
    return expr;
  }

  public void setExpr(ExprNodeGenericFuncDesc expr) {
    this.expr = expr;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
