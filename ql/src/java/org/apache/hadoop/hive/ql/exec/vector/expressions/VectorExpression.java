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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class for vector expressions.
 *
 * A vector expression is a vectorized execution tree that evaluates the same result as a (row-mode)
 * ExprNodeDesc tree describes.
 *
 * A vector expression has 0, 1, or more parameters and an optional output column.  These are
 * normally passed to the vector expression object' constructor.  A few special case classes accept
 * extra parameters via set* method.
 *
 * A ExprNodeColumnDesc vectorizes to the IdentityExpression class where the input column number
 * parameter is the same as the output column number.
 *
 * A ExprNodeGenericFuncDesc's generic function can vectorize to many different vectorized objects
 * depending on the parameter expression kinds (column, constant, etc) and data types.  Each
 * vectorized class implements the getDecription which indicates the particular expression kind
 * and data type specialization that class is designed for.  The Description is used by the
 * VectorizationContext class in matching the right vectorized class.
 *
 * The constructor parameters need to be in the same order as the generic function because
 * the VectorizationContext class automates parameter generation and object construction.
 *
 * Type information is remembered for the input parameters and the output type.
 *
 * A vector expression has optional children vector expressions when 1 or more parameters need
 * to be calculated into vector scratch columns.  Columns and constants do not need children
 * expressions.
 */
public abstract class VectorExpression implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Child expressions for parameters -- but only those that need to be computed.
   *
   * NOTE: Columns and constants are not included in the children.  That is: column numbers and
   * scalar values are passed via the constructor and remembered by the individual vector expression
   * classes. They are not represented in the children.
   */
  protected VectorExpression [] childExpressions;

  /**
   * ALL input parameter type information is here including those for (non-computed) columns and
   * scalar values.
   *
   * The vectorExpressionParameters() method is used to get the displayable string for the
   * parameters used by EXPLAIN, logging, etc.
   */
  protected TypeInfo[] inputTypeInfos;
  protected DataTypePhysicalVariation[] inputDataTypePhysicalVariations;

  /**
   * Output column number and type information of the vector expression.
   */
  protected final int outputColumnNum;

  protected TypeInfo outputTypeInfo;
  protected DataTypePhysicalVariation outputDataTypePhysicalVariation;

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

  /*
   * Use this constructor when there is an output column.
   */
  public VectorExpression(int outputColumnNum) {

    // By default, no children or inputs.
    childExpressions = null;
    inputTypeInfos = null;
    inputDataTypePhysicalVariations = null;

    this.outputColumnNum = outputColumnNum;

    // Set later with setOutput* methods.
    outputTypeInfo = null;
    outputDataTypePhysicalVariation = null;
  }

  //------------------------------------------------------------------------------------------------

  /**
   * Initialize the child expressions.
   */
  public void setChildExpressions(VectorExpression[] childExpressions) {
    this.childExpressions = childExpressions;
  }

  public VectorExpression[] getChildExpressions() {
    return childExpressions;
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

  public void transientInit() throws HiveException {
    // Do nothing by default.
  }

  public static void doTransientInit(VectorExpression vecExpr) throws HiveException {
    if (vecExpr == null) {
      return;
    }
    doTransientInitRecurse(vecExpr);
  }

  public static void doTransientInit(VectorExpression[] vecExprs) throws HiveException {
    if (vecExprs == null) {
      return;
    }
    for (VectorExpression vecExpr : vecExprs) {
      doTransientInitRecurse(vecExpr);
    }
  }

  private static void doTransientInitRecurse(VectorExpression vecExpr) throws HiveException {

    // Well, don't recurse but make sure all children are initialized.
    vecExpr.transientInit();
    List<VectorExpression> newChildren = new ArrayList<VectorExpression>();
    VectorExpression[] children = vecExpr.getChildExpressions();
    if (children != null) {
      Collections.addAll(newChildren, children);
    }
    while (!newChildren.isEmpty()) {
      VectorExpression childVecExpr = newChildren.remove(0);
      children = childVecExpr.getChildExpressions();
      if (children != null) {
        Collections.addAll(newChildren, children);
      }
      childVecExpr.transientInit();
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
   * @param batch
   */
  public abstract void evaluate(VectorizedRowBatch batch);

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
  final protected void evaluateChildren(VectorizedRowBatch vrg) {
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
    if (inputTypeInfos == null || inputDataTypePhysicalVariations == null) {
      fake++;
    }
    if (typeNum >= inputTypeInfos.length || typeNum >= inputDataTypePhysicalVariations.length) {
      fake++;
    }
    return getTypeName(inputTypeInfos[typeNum], inputDataTypePhysicalVariations[typeNum]);
  }

  static int fake;

  public static String getTypeName(TypeInfo typeInfo, DataTypePhysicalVariation dataTypePhysicalVariation) {
    if (typeInfo == null) {
      fake++;
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
}