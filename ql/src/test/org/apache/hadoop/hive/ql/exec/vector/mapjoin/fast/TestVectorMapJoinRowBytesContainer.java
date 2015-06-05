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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinRowBytesContainer;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestVectorMapJoinRowBytesContainer  {

  public void doFillReplay(Random random, int maxCount) throws Exception {

    RandomByteArrayStream randomByteArrayStream = new RandomByteArrayStream(random);
    VectorMapJoinRowBytesContainer vectorMapJoinRowBytesContainer = new VectorMapJoinRowBytesContainer();

    int count = Math.min(maxCount, random.nextInt(500));
    for (int i = 0; i < count; i++) {
      byte[] bytes = randomByteArrayStream.next();
      Output output = vectorMapJoinRowBytesContainer.getOuputForRowBytes();
      output.write(bytes);
      vectorMapJoinRowBytesContainer.finishRow();
    }
    vectorMapJoinRowBytesContainer.prepareForReading();

    for (int i = 0; i < count; i++) {
      if (!vectorMapJoinRowBytesContainer.readNext()) {
        assertTrue(false);
      }
      byte[] readBytes = vectorMapJoinRowBytesContainer.currentBytes();
      int readOffset = vectorMapJoinRowBytesContainer.currentOffset();
      int readLength = vectorMapJoinRowBytesContainer.currentLength();
      byte[] expectedBytes = randomByteArrayStream.get(i);
      if (readLength != expectedBytes.length) {
        assertTrue(false);
      }
      for (int j = 0; j < readLength; j++) {
        byte readByte = readBytes[readOffset + j];
        byte expectedByte = expectedBytes[j];
        if (readByte != expectedByte) {
          assertTrue(false);
        }
      }
    }
  }

  @Test
  public void testFillReplay() throws Exception {
    Random random = new Random(47496);

    for (int i = 0; i < 10; i++) {
      doFillReplay(random, 1 << i);
    }
  }
}