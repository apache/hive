/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.benchmark.vectorization;

import java.util.Random;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class ColumnVectorGenUtil {

  private static final long LONG_VECTOR_NULL_VALUE = 1;
  private static final double DOUBLE_VECTOR_NULL_VALUE = Double.NaN;

  public static VectorizedRowBatch getVectorizedRowBatch(int size, int numCol, int seed) {
    VectorizedRowBatch vrg = new VectorizedRowBatch(numCol, size);
    for (int j = 0; j < numCol; j++) {
      LongColumnVector lcv = new LongColumnVector(size);
      for (int i = 0; i < size; i++) {
        lcv.vector[i] = (i + 1) * seed * (j + 1);
      }
      vrg.cols[j] = lcv;
    }
    vrg.size = size;
    return vrg;
  }

  public static ColumnVector generateColumnVector(TypeInfo typeInfo, boolean nulls, boolean repeating, int size,
    Random rand) {
    if (typeInfo.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
      switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
          return generateLongColumnVector(nulls, repeating, size, rand);
        case FLOAT:
        case DOUBLE:
          return generateDoubleColumnVector(nulls, repeating, size, rand);
        case DECIMAL:
          return generateDecimalColumnVector(((DecimalTypeInfo) typeInfo), nulls, repeating, size, rand);
        case CHAR:
        case VARCHAR:
        case STRING:
        case BINARY:
          return generateBytesColumnVector(nulls, repeating, size, rand);
        case TIMESTAMP:
          return generateTimestampColumnVector(nulls, repeating, size, rand);
        // TODO: add interval and complex types
      }
    }
    throw new RuntimeException("Unsupported type info category: " + typeInfo.getCategory());
  }

  public static BytesColumnVector generateBytesColumnVector(
    boolean nulls, boolean repeating, int size, Random rand) {
    BytesColumnVector bcv = new BytesColumnVector(size);
    bcv.initBuffer(10);
    bcv.noNulls = !nulls;
    bcv.isRepeating = repeating;

    byte[] repeatingValue = new byte[10];
    rand.nextBytes(repeatingValue);

    int nullFrequency = generateNullFrequency(rand);

    for (int i = 0; i < size; i++) {
      if (nulls && (repeating || i % nullFrequency == 0)) {
        bcv.isNull[i] = true;
        bcv.setVal(0, new byte[]{0});
      } else {
        bcv.isNull[i] = false;
        if (repeating) {
          bcv.setVal(i, repeatingValue, 0, repeatingValue.length);
        } else {
          String val = String.valueOf("value_" + i);
          bcv.setVal(i, val.getBytes(), 0, val.length());
        }
      }
    }
    return bcv;
  }

  public static LongColumnVector generateLongColumnVector(
    boolean nulls, boolean repeating, int size, Random rand) {
    LongColumnVector lcv = new LongColumnVector(size);

    lcv.noNulls = !nulls;
    lcv.isRepeating = repeating;

    long repeatingValue;
    do {
      repeatingValue = rand.nextLong();
    } while (repeatingValue == 0);

    int nullFrequency = generateNullFrequency(rand);

    for (int i = 0; i < size; i++) {
      if (nulls && (repeating || i % nullFrequency == 0)) {
        lcv.isNull[i] = true;
        lcv.vector[i] = LONG_VECTOR_NULL_VALUE;

      } else {
        lcv.isNull[i] = false;
        lcv.vector[i] = repeating ? repeatingValue : rand.nextLong();
        if (lcv.vector[i] == 0) {
          i--;
        }
      }
    }
    return lcv;
  }

  private static ColumnVector generateTimestampColumnVector(final boolean nulls,
    final boolean repeating, final int size, final Random rand) {
    Timestamp[] timestamps = new Timestamp[size];
    for (int i = 0; i < size; i++) {
      timestamps[i] = Timestamp.ofEpochMilli(rand.nextInt());
    }
    return generateTimestampColumnVector(nulls, repeating, size, rand, timestamps);
  }

  public static TimestampColumnVector generateTimestampColumnVector(
    boolean nulls, boolean repeating, int size, Random rand, Timestamp[] timestampValues) {
    TimestampColumnVector tcv = new TimestampColumnVector(size);

    tcv.noNulls = !nulls;
    tcv.isRepeating = repeating;

    Timestamp repeatingTimestamp = RandomTypeUtil.getRandTimestamp(rand);

    int nullFrequency = generateNullFrequency(rand);

    for (int i = 0; i < size; i++) {
      if (nulls && (repeating || i % nullFrequency == 0)) {
        tcv.isNull[i] = true;
        tcv.setNullValue(i);
        timestampValues[i] = null;
      } else {
        tcv.isNull[i] = false;
        if (!repeating) {
          Timestamp randomTimestamp = RandomTypeUtil.getRandTimestamp(rand);
          tcv.set(i, randomTimestamp.toSqlTimestamp());
          timestampValues[i] = randomTimestamp;
        } else {
          tcv.set(i, repeatingTimestamp.toSqlTimestamp());
          timestampValues[i] = repeatingTimestamp;
        }
      }
    }
    return tcv;
  }

  public static DoubleColumnVector generateDoubleColumnVector(boolean nulls,
    boolean repeating, int size, Random rand) {
    DoubleColumnVector dcv = new DoubleColumnVector(size);

    dcv.noNulls = !nulls;
    dcv.isRepeating = repeating;

    double repeatingValue;
    do {
      repeatingValue = rand.nextDouble();
    } while (repeatingValue == 0);

    int nullFrequency = generateNullFrequency(rand);

    for (int i = 0; i < size; i++) {
      if (nulls && (repeating || i % nullFrequency == 0)) {
        dcv.isNull[i] = true;
        dcv.vector[i] = DOUBLE_VECTOR_NULL_VALUE;

      } else {
        dcv.isNull[i] = false;
        dcv.vector[i] = repeating ? repeatingValue : rand.nextDouble();

        if (dcv.vector[i] == 0) {
          i--;
        }
      }
    }
    return dcv;
  }

  public static DecimalColumnVector generateDecimalColumnVector(DecimalTypeInfo typeInfo, boolean nulls,
    boolean repeating, int size, Random rand) {
    DecimalColumnVector dcv =
      new DecimalColumnVector(size, typeInfo.precision(), typeInfo.scale());

    dcv.noNulls = !nulls;
    dcv.isRepeating = repeating;

    HiveDecimalWritable repeatingValue = new HiveDecimalWritable();
    do {
      repeatingValue.set(HiveDecimal.create(((Double) rand.nextDouble()).toString())
        .setScale((short) typeInfo.scale(), HiveDecimal.ROUND_HALF_UP));
    } while (repeatingValue.getHiveDecimal().doubleValue() == 0);

    int nullFrequency = generateNullFrequency(rand);

    for (int i = 0; i < size; i++) {
      if (nulls && (repeating || i % nullFrequency == 0)) {
        dcv.isNull[i] = true;
        dcv.vector[i] = null;

      } else {
        dcv.isNull[i] = false;
        if (repeating) {
          dcv.vector[i].set(repeatingValue);
        } else {
          dcv.vector[i].set(HiveDecimal.create(((Double) rand.nextDouble()).toString())
            .setScale((short) typeInfo.scale(), HiveDecimal.ROUND_HALF_UP));
        }

        if (dcv.vector[i].getHiveDecimal().doubleValue() == 0) {
          i--;
        }
      }
    }
    return dcv;
  }

  private static int generateNullFrequency(Random rand) {
    return 60 + rand.nextInt(20);
  }

}
