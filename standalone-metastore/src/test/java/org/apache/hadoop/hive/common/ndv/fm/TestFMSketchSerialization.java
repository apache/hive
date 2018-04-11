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
package org.apache.hadoop.hive.common.ndv.fm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;

import javolution.util.FastBitSet;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestFMSketchSerialization {

  private FastBitSet[] deserialize(String s, int numBitVectors) {
    FastBitSet[] b = new FastBitSet[numBitVectors];
    for (int j = 0; j < numBitVectors; j++) {
      b[j] = new FastBitSet(FMSketch.BIT_VECTOR_SIZE);
      b[j].clear();
    }

    int vectorIndex = 0;

    /*
     * Parse input string to obtain the indexes that are set in the bitvector.
     * When a toString() is called on a FastBitSet object to serialize it, the
     * serialization adds { and } to the beginning and end of the return String.
     * Skip "{", "}", ",", " " in the input string.
     */
    for (int i = 1; i < s.length() - 1;) {
      char c = s.charAt(i);
      i = i + 1;

      // Move on to the next bit vector
      if (c == '}') {
        vectorIndex = vectorIndex + 1;
      }

      // Encountered a numeric value; Extract out the entire number
      if (c >= '0' && c <= '9') {
        String t = new String();
        t = t + c;
        c = s.charAt(i);
        i = i + 1;

        while (c != ',' && c != '}') {
          t = t + c;
          c = s.charAt(i);
          i = i + 1;
        }

        int bitIndex = Integer.parseInt(t);
        assert (bitIndex >= 0);
        assert (vectorIndex < numBitVectors);
        b[vectorIndex].set(bitIndex);
        if (c == '}') {
          vectorIndex = vectorIndex + 1;
        }
      }
    }
    return b;
  }

  @Test
  public void testSerDe() throws IOException {
    String bitVectors = "{0, 4, 5, 7}{0, 1}{0, 1, 2}{0, 1, 4}{0}{0, 2}{0, 3}{0, 2, 3, 4}{0, 1, 4}{0, 1}{0}{0, 1, 3, 8}{0, 2}{0, 2}{0, 9}{0, 1, 4}";
    FastBitSet[] fastBitSet = deserialize(bitVectors, 16);
    FMSketch sketch = new FMSketch(16);
    for (int i = 0; i < 16; i++) {
      sketch.setBitVector(fastBitSet[i], i);
    }
    assertEquals(sketch.estimateNumDistinctValues(), 3);
    byte[] buf = sketch.serialize();
    FMSketch newSketch = (FMSketch) NumDistinctValueEstimatorFactory
        .getNumDistinctValueEstimator(buf);
    sketch.equals(newSketch);
    assertEquals(newSketch.estimateNumDistinctValues(), 3);
    assertArrayEquals(newSketch.serialize(), buf);
  }

}