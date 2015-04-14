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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import junit.framework.TestCase;

/**
 * Unit test for the vectorized conversion to and from row object[].
 */
public class TestVectorRowObject extends TestCase {

  void examineBatch(VectorizedRowBatch batch, VectorExtractRowSameBatch vectorExtractRow,
              Object[][] randomRows, int firstRandomRowIndex ) {

    int rowSize = vectorExtractRow.getCount();
    Object[] row = new Object[rowSize];
    for (int i = 0; i < batch.size; i++) {
      vectorExtractRow.extractRow(i, row);
      Object[] expectedRow = randomRows[firstRandomRowIndex + i];
      for (int c = 0; c < rowSize; c++) {
        if (!row[c].equals(expectedRow[c])) {
          fail("Row " + (firstRandomRowIndex + i) + " and column " + c + " mismatch");
        }
      }
    }
  }

  void testVectorRowObject(int caseNum, Random r) throws HiveException {

    Map<Integer, String> emptyScratchMap = new HashMap<Integer, String>();

    RandomRowObjectSource source = new RandomRowObjectSource();
    source.init(r);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(emptyScratchMap, source.rowStructObjectInspector());
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorAssignRowSameBatch vectorAssignRow = new VectorAssignRowSameBatch();
    vectorAssignRow.init(source.typeNames());
    vectorAssignRow.setOneBatch(batch);
    
    VectorExtractRowSameBatch vectorExtractRow = new VectorExtractRowSameBatch();
    vectorExtractRow.init(source.typeNames());
    vectorExtractRow.setOneBatch(batch);

    Object[][] randomRows = source.randomRows(100000);
    int firstRandomRowIndex = 0;
    for (int i = 0; i < randomRows.length; i++) {
      Object[] row = randomRows[i];

      vectorAssignRow.assignRow(batch.size, row);
      batch.size++;
      if (batch.size == batch.DEFAULT_SIZE) {
        examineBatch(batch, vectorExtractRow, randomRows, firstRandomRowIndex);
        firstRandomRowIndex = i + 1;
        batch.reset();
      }
    }
    if (batch.size > 0) {
      examineBatch(batch, vectorExtractRow, randomRows, firstRandomRowIndex);
    }
  }

  public void testVectorRowObject() throws Throwable {

  try {
    Random r = new Random(5678);
    for (int c = 0; c < 10; c++) {
      testVectorRowObject(c, r);
    }
  } catch (Throwable e) {
    e.printStackTrace();
    throw e;
  }
  }
}