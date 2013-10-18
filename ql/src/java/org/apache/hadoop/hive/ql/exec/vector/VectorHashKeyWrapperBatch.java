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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * Class for handling vectorized hash map key wrappers. It evaluates the key columns in a
 * row batch in a vectorized fashion.
 * This class stores additional information about keys needed to evaluate and output the key values.
 *
 */
public class VectorHashKeyWrapperBatch {

  /**
   * Helper class for looking up a key value based on key index.
   */
  private static class KeyLookupHelper {
    private int longIndex;
    private int doubleIndex;
    private int stringIndex;
  }

  /**
   * The key expressions that require evaluation and output the primitive values for each key.
   */
  private VectorExpression[] keyExpressions;

  /**
   * indices of LONG primitive keys.
   */
  private int[] longIndices;

  /**
   * indices of DOUBLE primitive keys.
   */
  private int[] doubleIndices;

  /**
   * indices of string (byte[]) primitive keys.
   */
  private int[] stringIndices;

  /**
   * Pre-allocated batch size vector of keys wrappers.
   * N.B. these keys are **mutable** and should never be used in a HashMap.
   * Always clone the key wrapper to obtain an immutable keywrapper suitable
   * to use a key in a HashMap.
   */
  private VectorHashKeyWrapper[] vectorHashKeyWrappers;

  /**
   * Lookup vector to map from key index to primitive type index.
   */
  private KeyLookupHelper[] indexLookup;

  /**
   * The fixed size of the key wrappers.
   */
  private int keysFixedSize;

   /**
   * Returns the compiled fixed size for the key wrappers.
   * @return
   */
  public int getKeysFixedSize() {
    return keysFixedSize;
  }

  /**
   * Accessor for the batch-sized array of key wrappers.
   */
  public VectorHashKeyWrapper[] getVectorHashKeyWrappers() {
    return vectorHashKeyWrappers;
  }

  /**
   * Processes a batch:
   * <ul>
   * <li>Evaluates each key vector expression.</li>
   * <li>Copies out each key's primitive values into the key wrappers</li>
   * <li>computes the hashcode of the key wrappers</li>
   * </ul>
   * @param batch
   * @throws HiveException
   */
  public void evaluateBatch(VectorizedRowBatch batch) throws HiveException {
    for(int i = 0; i < keyExpressions.length; ++i) {
      keyExpressions[i].evaluate(batch);
    }
    for(int i = 0; i< longIndices.length; ++i) {
      int keyIndex = longIndices[i];
      int columnIndex = keyExpressions[keyIndex].getOutputColumn();
      LongColumnVector columnVector = (LongColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignLongNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignLongNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNullsNoRepeatingSelection (i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Long null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<doubleIndices.length; ++i) {
      int keyIndex = doubleIndices[i];
      int columnIndex = keyExpressions[keyIndex].getOutputColumn();
      DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNullsNoRepeatingSelection (i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Double null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<stringIndices.length; ++i) {
      int keyIndex = stringIndices[i];
      int columnIndex = keyExpressions[keyIndex].getOutputColumn();
      BytesColumnVector columnVector = (BytesColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignStringNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignStringNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNullsNoRepeatingSelection (i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented String null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].setHashKey();
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for string type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignStringNullsNoRepeatingSelection(int index, int size,
      BytesColumnVector columnVector, int[] selected) {
    for(int i=0; i<size; ++i) {
      int row = selected[i];
      if (columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignNullString(index);
      } else {
        vectorHashKeyWrappers[i].assignString(index,
            columnVector.vector[row],
            columnVector.start[row],
            columnVector.length[row]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, repeat values.
   */
  private void assignStringNullsRepeating(int index, int size, BytesColumnVector columnVector) {
    if (columnVector.isNull[0]) {
      for(int i = 0; i < size; ++i) {
        vectorHashKeyWrappers[i].assignNullString(index);
      }
    } else {
      for(int i = 0; i < size; ++i) {
        vectorHashKeyWrappers[i].assignString(index,
            columnVector.vector[0],
            columnVector.start[0],
            columnVector.length[0]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for string type, possible nulls, no repeat values, no selection vector.
   */
  private void assignStringNullsNoRepeatingNoSelection(int index, int size,
      BytesColumnVector columnVector) {
    for(int i=0; i<size; ++i) {
      if (columnVector.isNull[i]) {
        vectorHashKeyWrappers[i].assignNullString(index);
      } else {
        vectorHashKeyWrappers[i].assignString(index,
            columnVector.vector[i],
            columnVector.start[i],
            columnVector.length[i]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignStringNoNullsRepeating(int index, int size, BytesColumnVector columnVector) {
    for(int i = 0; i < size; ++i) {
      vectorHashKeyWrappers[i].assignString(index,
          columnVector.vector[0],
          columnVector.start[0],
          columnVector.length[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignStringNoNullsNoRepeatingSelection(int index, int size,
      BytesColumnVector columnVector, int[] selected) {
    for(int i=0; i<size; ++i) {
      int row = selected[i];
      vectorHashKeyWrappers[i].assignString(index,
          columnVector.vector[row],
          columnVector.start[row],
          columnVector.length[row]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignStringNoNullsNoRepeatingNoSelection(int index, int size,
      BytesColumnVector columnVector) {
    for(int i=0; i<size; ++i) {
      vectorHashKeyWrappers[i].assignString(index,
          columnVector.vector[i],
          columnVector.start[i],
          columnVector.length[i]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignDoubleNullsNoRepeatingSelection(int index, int size,
      DoubleColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignDouble(index, columnVector.vector[row]);
      } else {
        vectorHashKeyWrappers[i].assignNullDouble(index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Double type, repeat null values.
   */
  private void assignDoubleNullsRepeating(int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullDouble(index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Double type, possible nulls, repeat values.
   */
  private void assignDoubleNullsNoRepeatingNoSelection(int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[r]);
      } else {
        vectorHashKeyWrappers[r].assignNullDouble(index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignDoubleNoNullsRepeating(int index, int size, DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignDoubleNoNullsNoRepeatingSelection(int index, int size,
      DoubleColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[selected[r]]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignDoubleNoNullsNoRepeatingNoSelection(int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignLongNullsNoRepeatingSelection(int index, int size,
      LongColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        vectorHashKeyWrappers[i].assignNullLong(index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, repeating nulls.
   */
  private void assignLongNullsRepeating(int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullLong(index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, no selection vector.
   */
  private void assignLongNullsNoRepeatingNoSelection(int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        vectorHashKeyWrappers[r].assignNullLong(index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignLongNoNullsRepeating(int index, int size, LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignLongNoNullsNoRepeatingSelection(int index, int size,
      LongColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignLongNoNullsNoRepeatingNoSelection(int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  /**
   * Prepares a VectorHashKeyWrapperBatch to work for a specific set of keys.
   * Computes the fast access lookup indices, preallocates all needed internal arrays.
   * This step is done only once per query, not once per batch. The information computed now
   * will be used to generate proper individual VectorKeyHashWrapper objects.
   */
  public static VectorHashKeyWrapperBatch compileKeyWrapperBatch(VectorExpression[] keyExpressions)
    throws HiveException {
    VectorHashKeyWrapperBatch compiledKeyWrapperBatch = new VectorHashKeyWrapperBatch();
    compiledKeyWrapperBatch.keyExpressions = keyExpressions;

    compiledKeyWrapperBatch.keysFixedSize = 0;

    // We'll overallocate and then shrink the array for each type
    int[] longIndices = new int[keyExpressions.length];
    int longIndicesIndex = 0;
    int[] doubleIndices = new int[keyExpressions.length];
    int doubleIndicesIndex  = 0;
    int[] stringIndices = new int[keyExpressions.length];
    int stringIndicesIndex = 0;
    KeyLookupHelper[] indexLookup = new KeyLookupHelper[keyExpressions.length];

    // Inspect the output type of each key expression.
    for(int i=0; i < keyExpressions.length; ++i) {
      indexLookup[i] = new KeyLookupHelper();
      String outputType = keyExpressions[i].getOutputType();
      if (VectorizationContext.isIntFamily(outputType) ||
          VectorizationContext.isDatetimeFamily(outputType)) {
        longIndices[longIndicesIndex] = i;
        indexLookup[i].longIndex = longIndicesIndex;
        indexLookup[i].doubleIndex = -1;
        indexLookup[i].stringIndex = -1;
        ++longIndicesIndex;
      } else if (VectorizationContext.isFloatFamily(outputType)) {
        doubleIndices[doubleIndicesIndex] = i;
        indexLookup[i].longIndex = -1;
        indexLookup[i].doubleIndex = doubleIndicesIndex;
        indexLookup[i].stringIndex = -1;
        ++doubleIndicesIndex;
      } else if (VectorizationContext.isStringFamily(outputType)) {
        stringIndices[stringIndicesIndex]= i;
        indexLookup[i].longIndex = -1;
        indexLookup[i].doubleIndex = -1;
        indexLookup[i].stringIndex = stringIndicesIndex;
        ++stringIndicesIndex;
      }
      else {
        throw new HiveException("Unsuported vector output type: " + outputType);
      }
    }
    compiledKeyWrapperBatch.indexLookup = indexLookup;
    compiledKeyWrapperBatch.longIndices = Arrays.copyOf(longIndices, longIndicesIndex);
    compiledKeyWrapperBatch.doubleIndices = Arrays.copyOf(doubleIndices, doubleIndicesIndex);
    compiledKeyWrapperBatch.stringIndices = Arrays.copyOf(stringIndices, stringIndicesIndex);
    compiledKeyWrapperBatch.vectorHashKeyWrappers =
        new VectorHashKeyWrapper[VectorizedRowBatch.DEFAULT_SIZE];
    for(int i=0;i<VectorizedRowBatch.DEFAULT_SIZE; ++i) {
      compiledKeyWrapperBatch.vectorHashKeyWrappers[i] =
          new VectorHashKeyWrapper(longIndicesIndex, doubleIndicesIndex, stringIndicesIndex);
    }

    JavaDataModel model = JavaDataModel.get();

    // Compute the fixed size overhead for the keys
    // start with the keywrapper itself
    compiledKeyWrapperBatch.keysFixedSize += JavaDataModel.alignUp(
        model.object() +
        model.ref() * 6+
        model.primitive1(),
        model.memoryAlign());

    // Now add the key wrapper arrays
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForLongArrayOfSize(longIndicesIndex);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForDoubleArrayOfSize(doubleIndicesIndex);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForObjectArrayOfSize(stringIndicesIndex);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForIntArrayOfSize(longIndicesIndex) * 2;
    compiledKeyWrapperBatch.keysFixedSize +=
        model.lengthForBooleanArrayOfSize(keyExpressions.length);

    return compiledKeyWrapperBatch;
  }

  /**
   * Get the row-mode writable object value of a key from a key wrapper
   * @param keyOutputWriter
   */
  public Object getWritableKeyValue(VectorHashKeyWrapper kw, int i,
      VectorExpressionWriter keyOutputWriter)
    throws HiveException {

    KeyLookupHelper klh = indexLookup[i];
    if (klh.longIndex >= 0) {
      return kw.getIsLongNull(klh.longIndex) ? null :
        keyOutputWriter.writeValue(kw.getLongValue(klh.longIndex));
    } else if (klh.doubleIndex >= 0) {
      return kw.getIsDoubleNull(klh.doubleIndex) ? null :
          keyOutputWriter.writeValue(kw.getDoubleValue(klh.doubleIndex));
    } else if (klh.stringIndex >= 0) {
      return kw.getIsBytesNull(klh.stringIndex) ? null :
          keyOutputWriter.writeValue(
              kw.getBytes(klh.stringIndex),
                kw.getByteStart(klh.stringIndex),
                kw.getByteLength(klh.stringIndex));
    } else {
      throw new HiveException(String.format(
          "Internal inconsistent KeyLookupHelper at index [%d]:%d %d %d",
          i, klh.longIndex, klh.doubleIndex, klh.stringIndex));
    }
  }

  public int getVariableSize(int batchSize) {
    int variableSize = 0;
    if ( 0 < stringIndices.length) {
      for (int k=0; k<batchSize; ++k) {
        VectorHashKeyWrapper hkw = vectorHashKeyWrappers[k];
        variableSize += hkw.getVariableSize();
      }
    }
    return variableSize;
  }
}

