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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class VectorizedRowGroupGenUtil {

  private static final long LONG_VECTOR_NULL_VALUE = 1;
  private static final double DOUBLE_VECTOR_NULL_VALUE = Double.NaN;

  public static VectorizedRowBatch getVectorizedRowBatch(int size, int numCol, int seed) {
    VectorizedRowBatch vrg = new VectorizedRowBatch(numCol, size);
    for (int j = 0; j < numCol; j++) {
      LongColumnVector lcv = new LongColumnVector(size);
      for (int i = 0; i < size; i++) {
        lcv.vector[i] = (i+1) * seed * (j+1);
      }
      vrg.cols[j] = lcv;
    }
    vrg.size = size;
    return vrg;
  }

  public static LongColumnVector generateLongColumnVector(
      boolean nulls, boolean repeating, int size, Random rand) {
    LongColumnVector lcv = new LongColumnVector(size);

    lcv.noNulls = !nulls;
    lcv.isRepeating = repeating;

    long repeatingValue;
    do{
      repeatingValue= rand.nextLong();
    }while(repeatingValue == 0);

    int nullFrequency = generateNullFrequency(rand);

    for(int i = 0; i < size; i++) {
      if(nulls && (repeating || i % nullFrequency == 0)) {
        lcv.isNull[i] = true;
        lcv.vector[i] = LONG_VECTOR_NULL_VALUE;

      }else {
        lcv.isNull[i] = false;
        lcv.vector[i] = repeating ? repeatingValue : rand.nextLong();
        if(lcv.vector[i] == 0) {
          i--;
        }
      }
    }
    return lcv;
  }

  public static DoubleColumnVector generateDoubleColumnVector(boolean nulls,
      boolean repeating, int size, Random rand) {
    DoubleColumnVector dcv = new DoubleColumnVector(size);

    dcv.noNulls = !nulls;
    dcv.isRepeating = repeating;

    double repeatingValue;
    do{
      repeatingValue= rand.nextDouble();
    }while(repeatingValue == 0);

    int nullFrequency = generateNullFrequency(rand);

    for(int i = 0; i < size; i++) {
      if(nulls && (repeating || i % nullFrequency == 0)) {
        dcv.isNull[i] = true;
        dcv.vector[i] = DOUBLE_VECTOR_NULL_VALUE;

      }else {
        dcv.isNull[i] = false;
        dcv.vector[i] = repeating ? repeatingValue : rand.nextDouble();

        if(dcv.vector[i] == 0) {
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
