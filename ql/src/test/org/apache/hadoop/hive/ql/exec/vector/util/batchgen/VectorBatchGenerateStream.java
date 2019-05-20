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

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class VectorBatchGenerateStream {

  private final long randomSeed;
  private final VectorBatchGenerator generator;
  private final int rowCount;

  // Stream variables.
  private Random random;
  private int sizeCountDown;

  public VectorBatchGenerateStream(long randomSeed, VectorBatchGenerator generator,
      int rowCount) {
    this.randomSeed = randomSeed;
    this.generator = generator;
    this.rowCount = rowCount;

    reset();
  }

  public int getRowCount() {
    return rowCount;
  }

  public void reset() {
    random = new Random(randomSeed);
    sizeCountDown = rowCount;
  }

  public boolean isNext() {
    return (sizeCountDown > 0);
  }

  public void fillNext(VectorizedRowBatch batch) {
    final int size = Math.min(sizeCountDown, batch.DEFAULT_SIZE);
    batch.reset();
    generator.generateBatch(batch, random, size);
    sizeCountDown -= size;
  }
}