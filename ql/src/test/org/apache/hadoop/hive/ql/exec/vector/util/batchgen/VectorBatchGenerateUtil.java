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

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

public class VectorBatchGenerateUtil {

  public static Object[][] generateRowObjectArray(TypeInfo[] typeInfos,
      VectorBatchGenerateStream batchStream, VectorizedRowBatch batch,
      ObjectInspector[] objectInspectors) throws HiveException {

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(typeInfos);

    final int rowCount = batchStream.getRowCount();
    final int columnCount = typeInfos.length;

    Object[][] rowObjectArray = new Object[rowCount][];

    Object[] row = new Object[columnCount];

    int index = 0;
    batchStream.reset();
    while (batchStream.isNext()) {
      batch.reset();
      batchStream.fillNext(batch);

      // Extract rows and call process per row
      final int size = batch.size;
      for (int r = 0; r < size; r++) {
        vectorExtractRow.extractRow(batch, r, row);
        Object[] resultObjectArray = new Object[columnCount];
        for (int c = 0; c < columnCount; c++) {
          resultObjectArray[c] = ((PrimitiveObjectInspector) objectInspectors[c]).copyObject(row[c]);
        }
        rowObjectArray[index++] = resultObjectArray;
      }
    }
    return rowObjectArray;
  }

  public static VectorizedRowBatch[] generateBatchArray(VectorBatchGenerateStream batchStream,
      VectorizedRowBatch batch) throws HiveException {

    final int rowCount = batchStream.getRowCount();
    final int batchCount = (rowCount + VectorizedRowBatch.DEFAULT_SIZE - 1) /
        VectorizedRowBatch.DEFAULT_SIZE;
    VectorizedRowBatch[] batches = new VectorizedRowBatch[batchCount];

    int index = 0;
    batchStream.reset();
    while (batchStream.isNext()) {
      VectorizedRowBatch nextBatch = VectorizedBatchUtil.makeLike(batch);
      batchStream.fillNext(nextBatch);
      batches[index++] = nextBatch;
    }
    return batches;
  }
}