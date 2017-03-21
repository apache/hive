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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Cast a string to a long.
 *
 * If other functions besides cast need to take a string in and produce a long,
 * you can subclass this class or convert it to a superclass, and
 * implement different "func()" methods for each operation.
 */
public class CastStringToLong extends VectorExpression {
  private static final long serialVersionUID = 1L;
  int inputColumn;
  int outputColumn;

  private transient boolean integerPrimitiveCategoryKnown = false;
  protected transient PrimitiveCategory integerPrimitiveCategory;

  public CastStringToLong(int inputColumn, int outputColumn) {
    super();
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
  }

  public CastStringToLong() {
    super();
  }

  /**
   * Convert input string to a long, at position i in the respective vectors.
   */
  protected void func(LongColumnVector outV, BytesColumnVector inV, int batchIndex) {

    byte[] bytes = inV.vector[batchIndex];
    final int start = inV.start[batchIndex];
    final int length = inV.length[batchIndex];
    try {

      switch (integerPrimitiveCategory) {
      case BOOLEAN:
        {
          boolean booleanValue;
          int i = start;
          if (length == 4) {
            if ((bytes[i] == 'T' || bytes[i] == 't') &&
                (bytes[i + 1] == 'R' || bytes[i + 1] == 'r') &&
                (bytes[i + 2] == 'U' || bytes[i + 2] == 'u') &&
                (bytes[i + 3] == 'E' || bytes[i + 3] == 'e')) {
              booleanValue = true;
            } else {
              // No boolean value match for 4 char field.
              outV.noNulls = false;
              outV.isNull[batchIndex] = true;
              return;
            }
          } else if (length == 5) {
            if ((bytes[i] == 'F' || bytes[i] == 'f') &&
                (bytes[i + 1] == 'A' || bytes[i + 1] == 'a') &&
                (bytes[i + 2] == 'L' || bytes[i + 2] == 'l') &&
                (bytes[i + 3] == 'S' || bytes[i + 3] == 's') &&
                (bytes[i + 4] == 'E' || bytes[i + 4] == 'e')) {
              booleanValue = false;
            } else {
              // No boolean value match for 5 char field.
              outV.noNulls = false;
              outV.isNull[batchIndex] = true;
              return;
            }
          } else if (length == 1) {
            byte b = bytes[start];
            if (b == '1' || b == 't' || b == 'T') {
              booleanValue = true;
            } else if (b == '0' || b == 'f' || b == 'F') {
              booleanValue = false;
            } else {
              // No boolean value match for extended 1 char field.
              outV.noNulls = false;
              outV.isNull[batchIndex] = true;
              return;
            }
          } else {
            // No boolean value match for other lengths.
            outV.noNulls = false;
            outV.isNull[batchIndex] = true;
            return;
          }
          outV.vector[batchIndex] = (booleanValue ? 1 : 0);
        }
        break;
      case BYTE:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outV.noNulls = false;
          outV.isNull[batchIndex] = true;
          return;
        }
        outV.vector[batchIndex] = LazyByte.parseByte(bytes, start, length, 10);
        break;
      case SHORT:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outV.noNulls = false;
          outV.isNull[batchIndex] = true;
          return;
        }
        outV.vector[batchIndex] = LazyShort.parseShort(bytes, start, length, 10);
        break;
      case INT:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outV.noNulls = false;
          outV.isNull[batchIndex] = true;
          return;
        }
        outV.vector[batchIndex] = LazyInteger.parseInt(bytes, start, length, 10);
        break;
      case LONG:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outV.noNulls = false;
          outV.isNull[batchIndex] = true;
          return;
        }
        outV.vector[batchIndex] = LazyLong.parseLong(bytes, start, length, 10);
        break;
      default:
        throw new Error("Unexpected primitive category " + integerPrimitiveCategory);
      }
    } catch (Exception e) {

      // for any exception in conversion to integer, produce NULL
      outV.noNulls = false;
      outV.isNull[batchIndex] = true;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (!integerPrimitiveCategoryKnown) {
      String typeName = getOutputType().toLowerCase();
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      integerPrimitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      integerPrimitiveCategoryKnown = true;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV = (BytesColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];

    if (n == 0) {

      // Nothing to do
      return;
    }

    if (inV.noNulls) {
      outV.noNulls = true;
      if (inV.isRepeating) {
        outV.isRepeating = true;
        func(outV, inV, 0);
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          func(outV, inV, i);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          func(outV, inV, i);
        }
        outV.isRepeating = false;
      }
    } else {

      // Handle case with nulls. Don't do function if the value is null,
      // because the data may be undefined for a null value.
      outV.noNulls = false;
      if (inV.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inV.isNull[0];
        if (!inV.isNull[0]) {
          func(outV, inV, 0);
        }
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = inV.isNull[i];
          if (!inV.isNull[i]) {
            func(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      } else {
        System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inV.isNull[i]) {
            func(outV, inV, i);
          }
        }
        outV.isRepeating = false;
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public int getInputColumn() {
    return inputColumn;
  }

  public void setInputColumn(int inputColumn) {
    this.inputColumn = inputColumn;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}