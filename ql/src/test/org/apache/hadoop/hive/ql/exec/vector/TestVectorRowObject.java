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
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import junit.framework.TestCase;

/**
 * Unit test for the vectorized conversion to and from row object[].
 */
public class TestVectorRowObject extends TestCase {

  void examineBatch(VectorizedRowBatch batch, VectorExtractRow vectorExtractRow,
              Object[][] randomRows, int firstRandomRowIndex ) {

    int rowSize = vectorExtractRow.getCount();
    Object[] row = new Object[rowSize];
    for (int i = 0; i < batch.size; i++) {
      vectorExtractRow.extractRow(batch, i, row);
      Object[] expectedRow = randomRows[firstRandomRowIndex + i];
      for (int c = 0; c < rowSize; c++) {
        if (!row[c].equals(expectedRow[c])) {
          fail("Row " + (firstRandomRowIndex + i) + " and column " + c + " mismatch");
        }
      }
    }
  }

  void testVectorRowObject(int caseNum, boolean sort, Random r) throws HiveException {

    String[] emptyScratchTypeNames = new String[0];

    VectorRandomRowSource source = new VectorRandomRowSource();
    source.init(r);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(source.rowStructObjectInspector(), emptyScratchTypeNames);
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    // junk the destination for the 1st pass
    for (ColumnVector cv : batch.cols) {
      Arrays.fill(cv.isNull, true);
      cv.noNulls = false;
    }

    VectorAssignRow vectorAssignRow = new VectorAssignRow();
    vectorAssignRow.init(source.typeNames());

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(source.typeNames());

    Object[][] randomRows = source.randomRows(10000);
    if (sort) {
      source.sort(randomRows);
    }
    int firstRandomRowIndex = 0;
    for (int i = 0; i < randomRows.length; i++) {
      Object[] row = randomRows[i];

      vectorAssignRow.assignRow(batch, batch.size, row);
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

      int caseNum = 0;
      for (int i = 0; i < 10; i++) {
        testVectorRowObject(caseNum, false, r);
        caseNum++;
      }

      // Try one sorted.
      testVectorRowObject(caseNum, true, r);
      caseNum++;

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}