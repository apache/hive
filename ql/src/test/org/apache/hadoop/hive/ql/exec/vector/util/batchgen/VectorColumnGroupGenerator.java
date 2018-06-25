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

package org.apache.hadoop.hive.ql.exec.vector.util.batchgen;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class VectorColumnGroupGenerator {

  private GenerateType[] generateTypes;
  private int[] columnNums;
  private Object[] arrays;

  public VectorColumnGroupGenerator(int columnNum, GenerateType generateType) {
    columnNums = new int[] {columnNum};
    generateTypes = new GenerateType[] {generateType};
    allocateArrays(VectorizedRowBatch.DEFAULT_SIZE);
  }

  public VectorColumnGroupGenerator(int startColumnNum, GenerateType[] generateTypes) {
    columnNums = new int[generateTypes.length];
    for (int i = 0; i < generateTypes.length; i++) {
      columnNums[i] = startColumnNum + i;
    }
    this.generateTypes = generateTypes;
    allocateArrays(VectorizedRowBatch.DEFAULT_SIZE);
  }

  public GenerateType[] generateTypes() {
    return generateTypes;
  }

  private void allocateArrays(int size) {
    arrays = new Object[generateTypes.length];
    for (int i = 0; i < generateTypes.length; i++) {
      GenerateType generateType = generateTypes[i];
      GenerateCategory category = generateType.getCategory();
      Object array = null;
      switch (category) {
      case BOOLEAN:
        array = new boolean[size];
        break;
      case BYTE:
        array = new byte[size];
        break;
      case SHORT:
        array = new short[size];
        break;
      case INT:
        array = new int[size];
        break;
      case LONG:
        array = new long[size];
        break;
      case FLOAT:
        array = new float[size];
        break;
      case DOUBLE:
        array = new double[size];
        break;
      case STRING:
        array = new String[size];
        break;
      case TIMESTAMP:
        array = new Timestamp[size];
        break;

      // UNDONE
      case DATE:
      case BINARY:
      case DECIMAL:
      case VARCHAR:
      case CHAR:

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
      }
      arrays[i] = array;
    }
  }

  public void clearColumnValueArrays() {
    for (int i = 0; i < generateTypes.length; i++) {
      GenerateType generateType = generateTypes[i];
      GenerateCategory category = generateType.getCategory();
      Object array = arrays[i];
      switch (category) {
      case BOOLEAN:
        Arrays.fill(((boolean[]) array), false);
        break;
      case BYTE:
        Arrays.fill(((byte[]) array), (byte) 0);
        break;
      case SHORT:
        Arrays.fill(((short[]) array), (short) 0);
        break;
      case INT:
        Arrays.fill(((int[]) array), 0);
        break;
      case LONG:
        Arrays.fill(((long[]) array), 0);
        break;
      case FLOAT:
        Arrays.fill(((float[]) array), 0);
        break;
      case DOUBLE:
        Arrays.fill(((double[]) array), 0);
        break;
      case STRING:
        Arrays.fill(((String[]) array), null);
        break;
      case TIMESTAMP:
        Arrays.fill(((Timestamp[]) array), null);
        break;

      // UNDONE
      case DATE:
      case BINARY:
      case DECIMAL:
      case VARCHAR:
      case CHAR:

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
      }
    }
  }

  public void generateRowValues(int rowIndex, Random random) {
    for (int i = 0; i < generateTypes.length; i++) {
      generateRowColumnValue(rowIndex, i, random);
    }
  }

  private void generateRowColumnValue(int rowIndex, int columnIndex, Random random) {
    GenerateType generateType = generateTypes[columnIndex];
    GenerateCategory category = generateType.getCategory();
    Object array = arrays[columnIndex];
    switch (category) {
    case BOOLEAN:
      {
        boolean value = random.nextBoolean();
        ((boolean[]) array)[rowIndex] = value;
      }
      break;
    case BYTE:
      {
        byte value =
            (byte)
                (random.nextBoolean() ?
                    -random.nextInt(-((int) Byte.MIN_VALUE) + 1) :
                      random.nextInt((int) Byte.MAX_VALUE + 1));
        ((byte[]) array)[rowIndex] = value;
      }
      break;
    case SHORT:
      {
        short value =
            (short)
                (random.nextBoolean() ?
                    -random.nextInt(-((int) Short.MIN_VALUE) + 1) :
                      random.nextInt((int) Short.MAX_VALUE + 1));
        ((short[]) array)[rowIndex] = value;
      }
      break;
    case INT:
      {
        int value = random.nextInt();
        ((int[]) array)[rowIndex] = value;
      }
      break;
    case LONG:
      {
        long value = random.nextLong();
        ((long[]) array)[rowIndex] = value;
      }
      break;
    case FLOAT:
      {
        float value = random.nextLong();
        ((float[]) array)[rowIndex] = value;
      }
      break;
    case DOUBLE:
      {
        double value = random.nextLong();
        ((double[]) array)[rowIndex] = value;
      }
      break;

    case STRING:
      {
        String value = RandomTypeUtil.getRandString(random);
        ((String[]) array)[rowIndex] = value;
      }
      break;

    case TIMESTAMP:
      {
        Timestamp value = RandomTypeUtil.getRandTimestamp(random);
        ((Timestamp[]) array)[rowIndex] = value;
      }
      break;

    // UNDONE
    case DATE:
      // UNDONE: Needed to longTest?

    case BINARY:
    case DECIMAL:
    case VARCHAR:
    case CHAR:

    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
    default:
    }
  }

  public void fillDownRowValues(int rowIndex, int seriesCount, Random random) {
    for (int i = 0; i < generateTypes.length; i++) {
      fillDownRowColumnValue(rowIndex, i, seriesCount, random);
    }
  }

  private void fillDownRowColumnValue(int rowIndex, int columnIndex, int seriesCount, Random random) {
    GenerateType generateType = generateTypes[columnIndex];
    GenerateCategory category = generateType.getCategory();
    Object array = arrays[columnIndex];
    switch (category) {
    case BOOLEAN:
      {
        boolean[] booleanArray = ((boolean[]) array);
        boolean value = booleanArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          booleanArray[rowIndex + i] = value;
        }
      }
      break;
    case BYTE:
      {
        byte[] byteArray = ((byte[]) array);
        byte value = byteArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          byteArray[rowIndex + i] = value;
        }
      }
      break;
    case SHORT:
      {
        short[] shortArray = ((short[]) array);
        short value = shortArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          shortArray[rowIndex + i] = value;
        }
      }
      break;
    case INT:
      {
        int[] intArray = ((int[]) array);
        int value = intArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          intArray[rowIndex + i] = value;
        }
      }
      break;
    case LONG:
      {
        long[] longArray = ((long[]) array);
        long value = longArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          longArray[rowIndex + i] = value;
        }
      }
      break;
    case FLOAT:
      {
        float[] floatArray = ((float[]) array);
        float value = floatArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          floatArray[rowIndex + i] = value;
        }
      }
      break;
    case DOUBLE:
      {
        double[] doubleArray = ((double[]) array);
        double value = doubleArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          doubleArray[rowIndex + i] = value;
        }
      }
      break;
    case STRING:
      {
        String[] stringArray = ((String[]) array);
        String value = stringArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          stringArray[rowIndex + i] = value;
        }
      }
      break;
    case TIMESTAMP:
      {
        Timestamp[] timestampArray = ((Timestamp[]) array);
        Timestamp value = timestampArray[rowIndex];
        for (int i = 1; i < seriesCount; i++) {
          timestampArray[rowIndex + i] = value;
        }
      }
      break;

    // UNDONE
    case DATE:

    case BINARY:
    case DECIMAL:
    case VARCHAR:
    case CHAR:

    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
    default:
    }
  }

  public void generateDownRowValues(int rowIndex, int seriesCount, Random random) {
    for (int i = 0; i < generateTypes.length; i++) {
      for (int g = 1; g < seriesCount; g++) {
        generateRowColumnValue(rowIndex + g, i,  random);
      }
    }
  }

  public void populateBatch(VectorizedRowBatch batch, int size, boolean isRepeated) {

    // UNDONE: Haven't finished isRepeated
    assert !isRepeated;

    for (int i = 0; i < size; i++) {
      for (int g = 0; g < generateTypes.length; g++) {
        populateBatchColumn(batch, g, size);
      }
    }
  }

  private void populateBatchColumn(VectorizedRowBatch batch, int logicalColumnIndex, int size) {
    int columnNum = columnNums[logicalColumnIndex];
    ColumnVector colVector = batch.cols[columnNum];

    GenerateType generateType = generateTypes[logicalColumnIndex];
    GenerateCategory category = generateType.getCategory();
    Object array = arrays[logicalColumnIndex];
    switch (category) {
    case BOOLEAN:
      {
        boolean[] booleanArray = ((boolean[]) array);
        long[] vector = ((LongColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = (booleanArray[i] ? 1 : 0);
        }
      }
      break;
    case BYTE:
      {
        byte[] byteArray = ((byte[]) array);
        long[] vector = ((LongColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = byteArray[i];
        }
      }
      break;
    case SHORT:
      {
        short[] shortArray = ((short[]) array);
        long[] vector = ((LongColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = shortArray[i];
        }
      }
      break;
    case INT:
      {
        int[] intArray = ((int[]) array);
        long[] vector = ((LongColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = intArray[i];
        }
      }
      break;
    case LONG:
      {
        long[] longArray = ((long[]) array);
        long[] vector = ((LongColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = longArray[i];
        }
      }
      break;
    case FLOAT:
      {
        float[] floatArray = ((float[]) array);
        double[] vector = ((DoubleColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = floatArray[i];
        }
      }
      break;
    case DOUBLE:
      {
        double[] doubleArray = ((double[]) array);
        double[] vector = ((DoubleColumnVector) colVector).vector;
        for (int i = 0; i < size; i++) {
          vector[i] = doubleArray[i];
        }
      }
      break;
    case STRING:
      {
        String[] stringArray = ((String[]) array);
        BytesColumnVector bytesColVec = ((BytesColumnVector) colVector);
        for (int i = 0; i < size; i++) {
          byte[] bytes = stringArray[i].getBytes();
          bytesColVec.setVal(i, bytes);
        }
      }
      break;
    case TIMESTAMP:
      {
        Timestamp[] timestampArray = ((Timestamp[]) array);
        TimestampColumnVector timestampColVec = ((TimestampColumnVector) colVector);
        for (int i = 0; i < size; i++) {
          Timestamp timestamp = timestampArray[i];
          timestampColVec.set(i, timestamp);
        }
      }
      break;

    // UNDONE

    case DATE:

    case BINARY:
    case DECIMAL:
    case VARCHAR:
    case CHAR:

    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
    default:
    }
  }
}