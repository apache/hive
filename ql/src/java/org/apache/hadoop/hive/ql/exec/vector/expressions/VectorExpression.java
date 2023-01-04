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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class for vector expressions.
 * <p>
 * A vector expression is a vectorized execution tree that evaluates the same result as a (row-mode)
 * ExprNodeDesc tree describes.
 * <p>
 * A vector expression has 0, 1, or more parameters and an optional output column.  These are
 * normally passed to the vector expression object' constructor.  A few special case classes accept
 * extra parameters via set* method.
 * <p>
 * A ExprNodeColumnDesc vectorizes to the IdentityExpression class where the input column number
 * parameter is the same as the output column number.
 * <p>
 * A ExprNodeGenericFuncDesc's generic function can vectorize to many different vectorized objects
 * depending on the parameter expression kinds (column, constant, etc) and data types.  Each
 * vectorized class implements the getDecription which indicates the particular expression kind
 * and data type specialization that class is designed for.  The Description is used by the
 * VectorizationContext class in matching the right vectorized class.
 * <p>
 * The constructor parameters need to be in the same order as the generic function because
 * the VectorizationContext class automates parameter generation and object construction.
 * <p>
 * Type information is remembered for the input parameters and the output type.
 * <p>
 * A vector expression has optional children vector expressions when 1 or more parameters need
 * to be calculated into vector scratch columns.  Columns and constants do not need children
 * expressions.
 * <p>
 * HOW TO to extend VectorExpression (some basic steps and hints):
 * 1. Create a subclass, and write a proper getDescriptor() (column/scalar?, number for args?, etc.)
 * 2. Define an explicit parameterless constructor
 * 3. Define a proper parameterized constructor (according to descriptor)
 * 4. In case of UDF, add non-vectorized UDF class to Vectorizer.supported*UDFs
 * 5. Add the new vectorized expression class to VectorizedExpressions annotation of the original UDF
 * 6. If you subclass an expression, do the same steps (2,3,5) for subclasses as well (ctors)
 * 7. If your base expression class is abstract, don't add it to VectorizedExpressions annotation
 */
public abstract class VectorExpression implements Serializable {

  private static final long serialVersionUID = 1L;
  protected transient final Logger LOG = LoggerFactory.getLogger(getClass().getName());

  /**
   * Child expressions for parameters -- but only those that need to be computed.
   * <p>
   * NOTE: Columns and constants are not included in the children.  That is: column numbers and
   * scalar values are passed via the constructor and remembered by the individual vector expression
   * classes. They are not represented in the children.
   */
  protected VectorExpression [] childExpressions;

  /**
   * ALL input parameter type information is here including those for (non-computed) columns and
   * scalar values.
   * <p>
   * The vectorExpressionParameters() method is used to get the displayable string for the
   * parameters used by EXPLAIN, logging, etc.
   */
  protected TypeInfo[] inputTypeInfos;
  protected DataTypePhysicalVariation[] inputDataTypePhysicalVariations;

  /**
   * Output column number and type information of the vector expression.
   */
  public int outputColumnNum;

  protected TypeInfo outputTypeInfo;
  protected DataTypePhysicalVariation outputDataTypePhysicalVariation;

  /**
   * Input column numbers of the vector expression, which should be reused by vector expressions.
   */
  public int[] inputColumnNum = {-1, -1, -1};

  /*
   * Use this constructor when there is NO output column.
   */
  public VectorExpression() {
    // Initially, no children or inputs; set later with setInput* methods.
    childExpressions = null;
    inputTypeInfos = null;
    inputDataTypePhysicalVariations = null;

    // No output type information.
    outputColumnNum = -1;
    outputTypeInfo = null;
    outputDataTypePhysicalVariation = null;
  }

  /**
   * Constructor for 1 input column and 1 output column.
   */
  public VectorExpression(int inputColumnNum, int outputColumnNum) {
    this(inputColumnNum, -1, -1, outputColumnNum);
  }

  /**
   * Constructor for 2 input columns and 1 output column.
   */
  public VectorExpression(int inputColumnNum, int inputColumnNum2, int outputColumnNum) {
    this(inputColumnNum, inputColumnNum2, -1, outputColumnNum);
  }

  /**
   * Constructor for 3 input columns and 1 output column. Currently, VectorExpression is initialized
   * for a maximum of 3 input columns. In case an expression with more than 3 columns wants to reuse
   * logic here, inputColumnNum* fields should be extended.
   */
  public VectorExpression(int inputColumnNum, int inputColumnNum2, int inputColumnNum3, int outputColumnNum) {
    // By default, no children or inputs.
    childExpressions = null;
    inputTypeInfos = null;
    inputDataTypePhysicalVariations = null;

    this.inputColumnNum[0] = inputColumnNum;
    this.inputColumnNum[1] = inputColumnNum2;
    this.inputColumnNum[2] = inputColumnNum3;
    this.outputColumnNum = outputColumnNum;

    // Set later with setOutput* methods.
    outputTypeInfo = null;
    outputDataTypePhysicalVariation = null;
  }

  /**
   * Convenience method for expressions that uses arbitrary number of input columns in an array.
   */
  public VectorExpression(int[] inputColumnNum, int outputColumnNum) {
    // By default, no children or inputs.
    childExpressions = null;
    inputTypeInfos = null;
    inputDataTypePhysicalVariations = null;

    this.inputColumnNum = inputColumnNum;
    this.outputColumnNum = outputColumnNum;

    // Set later with setOutput* methods.
    outputTypeInfo = null;
    outputDataTypePhysicalVariation = null;
  }

  /**
   * Initialize the child expressions.
   */
  public void setChildExpressions(VectorExpression[] childExpressions) {
    this.childExpressions = childExpressions;
  }

  public VectorExpression[] getChildExpressions() {
    return childExpressions;
  }

  protected Collection<VectorExpression> getChildExpressionsForTransientInit() {
    if (getChildExpressions() != null) {
      return Arrays.asList(getChildExpressions());
    } else {
      return Collections.emptyList();
    }
  }

  //------------------------------------------------------------------------------------------------

  public void setInputTypeInfos(TypeInfo ...inputTypeInfos) {
    this.inputTypeInfos = inputTypeInfos;
  }

  public TypeInfo[] getInputTypeInfos() {
    return inputTypeInfos;
  }

  public void setInputDataTypePhysicalVariations(
      DataTypePhysicalVariation ...inputDataTypePhysicalVariations) {
    this.inputDataTypePhysicalVariations = inputDataTypePhysicalVariations;
  }

  public DataTypePhysicalVariation[] getInputDataTypePhysicalVariations() {
    return inputDataTypePhysicalVariations;
  }

  /*
   * Return a short string with the parameters of the vector expression that will be
   * shown in EXPLAIN output, etc.
   */
  public abstract String vectorExpressionParameters();

  //------------------------------------------------------------------------------------------------

  public void transientInit(Configuration conf) throws HiveException {
    // Do nothing by default.
  }

  public static void doTransientInit(VectorExpression vecExpr, Configuration conf) throws HiveException {
    if (vecExpr == null) {
      return;
    }
    doTransientInitRecurse(vecExpr, conf);
  }

  public static void doTransientInit(VectorExpression[] vecExprs, Configuration conf) throws HiveException {
    if (vecExprs == null) {
      return;
    }
    for (VectorExpression vecExpr : vecExprs) {
      doTransientInitRecurse(vecExpr, conf);
    }
  }

  private static void doTransientInitRecurse(VectorExpression vecExpr, Configuration conf) throws HiveException {

    // Well, don't recurse but make sure all children are initialized.
    vecExpr.transientInit(conf);

    Deque<VectorExpression> newChildren = new ArrayDeque<>(vecExpr.getChildExpressionsForTransientInit());

    while (!newChildren.isEmpty()) {
      VectorExpression childVecExpr = newChildren.removeFirst();
      newChildren.addAll(childVecExpr.getChildExpressionsForTransientInit());
      childVecExpr.transientInit(conf);
    }
  }

  //------------------------------------------------------------------------------------------------

  /**
   * Returns the index of the output column in the array
   * of column vectors. If not applicable, -1 is returned.
   * @return Index of the output column
   */
  public int getOutputColumnNum() {
    return outputColumnNum;
  }

  /**
   * Returns type of the output column.
   */
  public TypeInfo getOutputTypeInfo() {
    return outputTypeInfo;
  }

  /**
   * Set type of the output column.
   */
  public void setOutputTypeInfo(TypeInfo outputTypeInfo) {
    this.outputTypeInfo = outputTypeInfo;
  }

  /**
   * Set data type read variation.
   */
  public void setOutputDataTypePhysicalVariation(DataTypePhysicalVariation outputDataTypePhysicalVariation) {
    this.outputDataTypePhysicalVariation = outputDataTypePhysicalVariation;
  }

  public DataTypePhysicalVariation getOutputDataTypePhysicalVariation() {
    return outputDataTypePhysicalVariation;
  }

  public ColumnVector.Type getOutputColumnVectorType() throws HiveException {
    return
        VectorizationContext.getColumnVectorTypeFromTypeInfo(
            outputTypeInfo, outputDataTypePhysicalVariation);
  }
  /**
   * This is the primary method to implement expression logic.
   */
  public abstract void evaluate(VectorizedRowBatch batch) throws HiveException;

  public void init(Configuration conf) {
    if (childExpressions != null) {
      for (VectorExpression child : childExpressions) {
        child.init(conf);
      }
    }
  }

  public abstract VectorExpressionDescriptor.Descriptor getDescriptor();

  /**
   * Evaluate the child expressions on the given input batch.
   * @param vrg {@link VectorizedRowBatch}
   */
  final protected void evaluateChildren(VectorizedRowBatch vrg) throws HiveException {
    if (childExpressions != null) {
      for (VectorExpression ve : childExpressions) {
        ve.evaluate(vrg);
      }
    }
  }

  protected String getColumnParamString(int typeNum, int columnNum) {
    return "col " + columnNum + ":" + getParamTypeString(typeNum);
  }

  protected String getLongValueParamString(int typeNum, long value) {
    return "val " + value + ":" + getParamTypeString(typeNum);
  }

  protected String getDoubleValueParamString(int typeNum, double value) {
    return "val " + value + ":" + getParamTypeString(typeNum);
  }

  protected String getParamTypeString(int typeNum) {
    if (inputTypeInfos == null) {
      return "<input types is null>";
    }
    if (inputDataTypePhysicalVariations == null) {
      return "<input data type physical variations is null>";
    }
    return getTypeName(inputTypeInfos[typeNum], inputDataTypePhysicalVariations[typeNum]);
  }

  public static String getTypeName(TypeInfo typeInfo, DataTypePhysicalVariation dataTypePhysicalVariation) {
    if (typeInfo == null) {
      return "<input type is null>";
    }
    if (dataTypePhysicalVariation != null && dataTypePhysicalVariation != DataTypePhysicalVariation.NONE) {
      return typeInfo.toString() + "/" + dataTypePhysicalVariation;
    } else {
      return typeInfo.toString();
    }
  }

  /**
   * A vector expression which implements a checked execution to account for overflow handling
   * should override this method and return true. In such a case Vectorizer will use Checked
   * variation of the vector expression to process data
   * @return true if vector expression implements a Checked variation of vector expression
   */
  public boolean supportsCheckedExecution() {
    // default is false
    return false;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (this instanceof IdentityExpression) {
      b.append(vectorExpressionParameters());
    } else {
      b.append(this.getClass().getSimpleName());
      String vectorExpressionParameters = vectorExpressionParameters();
      if (vectorExpressionParameters != null) {
        b.append("(");
        b.append(vectorExpressionParameters);
        b.append(")");
      }
      if (childExpressions != null) {
        b.append("(children: ");
        for (int i = 0; i < childExpressions.length; i++) {
          b.append(childExpressions[i].toString());
          if (i < childExpressions.length-1) {
            b.append(", ");
          }
        }
        b.append(")");
      }

      if (outputColumnNum != -1) {
        b.append(" -> ");
        b.append(outputColumnNum);
        b.append(":");
        b.append(getTypeName(outputTypeInfo, outputDataTypePhysicalVariation));
      }
     }

    return b.toString();
  }

  public static String displayUtf8Bytes(byte[] bytes) {
    if (bytes == null) {
      return "NULL";
    } else {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  public static String displayArrayOfUtf8ByteArrays(byte[][] arrayOfByteArrays) {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    for (byte[] bytes : arrayOfByteArrays) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append(displayUtf8Bytes(bytes));
    }
    return sb.toString();
  }

  /**
   * By default vector expressions do not handle decimal64 types and should be
   * converted into Decimal types if its output cannot handle Decimal64. Decimal64
   * that only deal with Decimal64 types cannot handle conversions so they should
   * override this method and return false.
   */
  public boolean shouldConvertDecimal64ToDecimal() {
    return getOutputDataTypePhysicalVariation() == DataTypePhysicalVariation.NONE;
  }
}
