/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.List;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorizedBatchUtil {

  @Test
  public void testSetDecimal64ColumnVector() throws Exception {
    Decimal64ColumnVector dec64ColVector = new Decimal64ColumnVector(10, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = dec64ColVector;

    Object[] row = new Object[] {new HiveDecimalWritable("123.45")};

    StructObjectInspector oi =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            List.of("col1"),
            List.of(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector));

    DataOutputBuffer buffer = new DataOutputBuffer();

    try {
      VectorizedBatchUtil.addProjectedRowToBatchFrom(row, oi, 0, batch, buffer);

      Assert.assertEquals(12345L, dec64ColVector.vector[0]);
    } catch (ClassCastException e) {
      Assert.fail(
          "ClassCastException thrown when adding projected row with Decimal64ColumnVector: "
              + e.getMessage());
    }
  }
}
