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

import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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

  // Transient members initialized by transientInit method.
  protected transient PrimitiveCategory integerPrimitiveCategory;

  public CastStringToLong(int inputColumn, int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumn = inputColumn;
  }

  public CastStringToLong() {
    super();

    // Dummy final assignments.
    inputColumn = -1;
  }

  @Override
  public void transientInit() throws HiveException {
    super.transientInit();

    integerPrimitiveCategory = ((PrimitiveTypeInfo) outputTypeInfo).getPrimitiveCategory();
  }

  /**
   * Convert input string to a long, at position i in the respective vectors.
   */
  protected void func(LongColumnVector outputColVector, BytesColumnVector inputColVector, int batchIndex) {

    byte[] bytes = inputColVector.vector[batchIndex];
    final int start = inputColVector.start[batchIndex];
    final int length = inputColVector.length[batchIndex];
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
              outputColVector.noNulls = false;
              outputColVector.isNull[batchIndex] = true;
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
              outputColVector.noNulls = false;
              outputColVector.isNull[batchIndex] = true;
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
              outputColVector.noNulls = false;
              outputColVector.isNull[batchIndex] = true;
              return;
            }
          } else {
            // No boolean value match for other lengths.
            outputColVector.noNulls = false;
            outputColVector.isNull[batchIndex] = true;
            return;
          }
          outputColVector.vector[batchIndex] = (booleanValue ? 1 : 0);
        }
        break;
      case BYTE:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outputColVector.noNulls = false;
          outputColVector.isNull[batchIndex] = true;
          return;
        }
        outputColVector.vector[batchIndex] = LazyByte.parseByte(bytes, start, length, 10);
        break;
      case SHORT:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outputColVector.noNulls = false;
          outputColVector.isNull[batchIndex] = true;
          return;
        }
        outputColVector.vector[batchIndex] = LazyShort.parseShort(bytes, start, length, 10);
        break;
      case INT:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outputColVector.noNulls = false;
          outputColVector.isNull[batchIndex] = true;
          return;
        }
        outputColVector.vector[batchIndex] = LazyInteger.parseInt(bytes, start, length, 10);
        break;
      case LONG:
        if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
          outputColVector.noNulls = false;
          outputColVector.isNull[batchIndex] = true;
          return;
        }
        outputColVector.vector[batchIndex] = LazyLong.parseLong(bytes, start, length, 10);
        break;
      default:
        throw new Error("Unexpected primitive category " + integerPrimitiveCategory);
      }
    } catch (Exception e) {

      // for any exception in conversion to integer, produce NULL
      outputColVector.noNulls = false;
      outputColVector.isNull[batchIndex] = true;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];

    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    if (n == 0) {

      // Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        func(outputColVector, inputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           func(outputColVector, inputColVector, i);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            func(outputColVector, inputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          func(outputColVector, inputColVector, i);
        }
      }
    } else  /* there are NULLs in the inputColVector */ {

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            // Set isNull before call in case it changes it mind.
            outputColVector.isNull[i] = false;
            func(outputColVector, inputColVector, i);
          } else {
            outputColVector.isNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            // Set isNull before call in case it changes it mind.
            outputColVector.isNull[i] = false;
            func(outputColVector, inputColVector, i);
          } else {
            outputColVector.isNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumn);
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