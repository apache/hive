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

import org.apache.hadoop.hive.ql.exec.vector.VectorBatchDebug;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.junit.Test;

import java.util.Random;

public class TestVectorBatchGenerate {

  @Test
  public void testTryIt() throws Exception {
    GenerateType[] generateTypes =
        new GenerateType[] {new GenerateType(GenerateCategory.INT), new GenerateType(GenerateCategory.BYTE)};
    VectorBatchGenerator generator = new VectorBatchGenerator(generateTypes);

    VectorizedRowBatch batch = generator.createBatch();

    Random random = new Random();
    generator.generateBatch(batch, random, VectorizedRowBatch.DEFAULT_SIZE);
    VectorBatchDebug.debugDisplayBatch(batch, "testTryIt");
  }

  @Test
  public void testTryIt2() throws Exception {
    GenerateType[] generateTypes =
        new GenerateType[] {new GenerateType(GenerateCategory.BOOLEAN), new GenerateType(GenerateCategory.LONG), new GenerateType(GenerateCategory.DOUBLE)};
    VectorBatchGenerator generator = new VectorBatchGenerator(generateTypes);

    VectorizedRowBatch batch = generator.createBatch();

    Random random = new Random();
    generator.generateBatch(batch, random, VectorizedRowBatch.DEFAULT_SIZE);
    VectorBatchDebug.debugDisplayBatch(batch, "testTryIt2");
  }
}