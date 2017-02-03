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

import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hive.common.util.BloomFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorInBloomFilterColDynamicValue extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(VectorInBloomFilterColDynamicValue.class);

  protected int colNum;
  protected DynamicValue bloomFilterDynamicValue;
  protected transient boolean initialized = false;
  protected transient BloomFilter bloomFilter;
  protected transient BloomFilterCheck bfCheck;

  public VectorInBloomFilterColDynamicValue(int colNum, DynamicValue bloomFilterDynamicValue) {
    this.colNum = colNum;
    this.bloomFilterDynamicValue = bloomFilterDynamicValue;
  }

  public VectorInBloomFilterColDynamicValue() {
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    bloomFilterDynamicValue.setConf(conf);

    // Instantiate BloomFilterCheck based on input column type
    VectorExpression.Type colType = this.getInputTypes()[0];
    switch (colType) {
    case LONG:
    case DATE:
      bfCheck = new LongBloomFilterCheck();
      break;
    case DOUBLE:
      bfCheck = new DoubleBloomFilterCheck();
      break;
    case DECIMAL:
      bfCheck = new DecimalBloomFilterCheck();
      break;
    case STRING:
    case CHAR:
    case VARCHAR:
    case BINARY:
      bfCheck = new BytesBloomFilterCheck();
      break;
    case TIMESTAMP:
      bfCheck = new TimestampBloomFilterCheck();
      break;
    default:
      throw new IllegalStateException("Unsupported type " + colType);
    }
  }

  private void initValue()  {
    try {
      Object val = bloomFilterDynamicValue.getValue();
      if (val != null) {
        BinaryObjectInspector boi = (BinaryObjectInspector) bloomFilterDynamicValue.getObjectInspector();
        byte[] bytes = boi.getPrimitiveJavaObject(val);
        bloomFilter = BloomFilter.deserialize(new ByteArrayInputStream(bytes));
      } else {
        bloomFilter = null;
      }
      initialized = true;
    } catch (Exception err) {
      throw new RuntimeException(err);
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    if (!initialized) {
      initValue();
    }

    ColumnVector inputColVector = batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // In case the dynamic value resolves to a null value
    if (bloomFilter == null) {
      batch.size = 0;
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero. Repeating property will not change.
        if (!(bfCheck.checkValue(inputColVector, 0))) {

          //Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (bfCheck.checkValue(inputColVector, i)) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (bfCheck.checkValue(inputColVector, i)) {
            sel[newSize++] = i;
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }
      }
    } else {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero. Repeating property will not change.
        if (!nullPos[0]) {
          if (!(bfCheck.checkValue(inputColVector, 0))) {

            //Entire batch is filtered out.
            batch.size = 0;
          }
        } else {
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
           if (bfCheck.checkValue(inputColVector, i)) {
             sel[newSize++] = i;
           }
          }
        }

        //Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            if (bfCheck.checkValue(inputColVector, i)) {
              sel[newSize++] = i;
            }
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.ALL_FAMILY,
            VectorExpressionDescriptor.ArgumentType.BINARY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.DYNAMICVALUE);
    return b.build();
  }

  // Type-specific handling
  abstract class BloomFilterCheck {
    abstract public boolean checkValue(ColumnVector columnVector, int idx);
  }

  class BytesBloomFilterCheck extends BloomFilterCheck {
    @Override
    public boolean checkValue(ColumnVector columnVector, int idx) {
      BytesColumnVector col = (BytesColumnVector) columnVector;
      return bloomFilter.testBytes(col.vector[idx], col.start[idx], col.length[idx]);
    }
  }

  class LongBloomFilterCheck extends BloomFilterCheck {
    @Override
    public boolean checkValue(ColumnVector columnVector, int idx) {
      LongColumnVector col = (LongColumnVector) columnVector;
      return bloomFilter.testLong(col.vector[idx]);
    }
  }

  class DoubleBloomFilterCheck extends BloomFilterCheck {
    @Override
    public boolean checkValue(ColumnVector columnVector, int idx) {
      DoubleColumnVector col = (DoubleColumnVector) columnVector;
      return bloomFilter.testDouble(col.vector[idx]);
    }
  }

  class DecimalBloomFilterCheck extends BloomFilterCheck {
    private byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];

    @Override
    public boolean checkValue(ColumnVector columnVector, int idx) {
      DecimalColumnVector col = (DecimalColumnVector) columnVector;
      int startIdx = col.vector[idx].toBytes(scratchBuffer);
      return bloomFilter.testBytes(scratchBuffer, startIdx, scratchBuffer.length - startIdx);
    }
  }

  class TimestampBloomFilterCheck extends BloomFilterCheck {
    @Override
    public boolean checkValue(ColumnVector columnVector, int idx) {
      TimestampColumnVector col = (TimestampColumnVector) columnVector;
      return bloomFilter.testLong(col.time[idx]);
    }
  }
}
