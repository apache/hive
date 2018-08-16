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

package org.apache.hadoop.hive.ql.exec.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromObjectIterables;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Unit test for VectorHashKeyWrapperBatch class.
 */
public class TestVectorHashKeyWrapperBatch {

  // Specific test for HIVE-18744 --
  // Tests Timestamp assignment.
  @Test
  public void testVectorHashKeyWrapperBatch() throws HiveException {

    VectorExpression[] keyExpressions =
        new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos =
        new TypeInfo[] {TypeInfoFactory.timestampTypeInfo};
    VectorHashKeyWrapperBatch vhkwb =
        VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
            keyExpressions,
            typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.selectedInUse = false;
    batch.size = 10;
    TimestampColumnVector timestampColVector = new TimestampColumnVector(batch.DEFAULT_SIZE);;
    batch.cols[0] = timestampColVector;
    timestampColVector.reset();
    // Cause Timestamp object to be replaced (in buggy code) with ZERO_TIMESTAMP.
    timestampColVector.noNulls = false;
    timestampColVector.isNull[0] = true;
    Timestamp scratch = new Timestamp(2039);
    Timestamp ts0 = new Timestamp(2039);
    scratch.setTime(ts0.getTime());
    scratch.setNanos(ts0.getNanos());
    timestampColVector.set(1, scratch);
    Timestamp ts1 = new Timestamp(33222);
    scratch.setTime(ts1.getTime());
    scratch.setNanos(ts1.getNanos());
    timestampColVector.set(2, scratch);
    batch.size = 3;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapper[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    VectorHashKeyWrapper vhk = vhkwArray[0];
    assertTrue(vhk.isNull(0));
    vhk = vhkwArray[1];
    assertFalse(vhk.isNull(0));
    assertEquals(vhk.getTimestamp(0), ts0);
    vhk = vhkwArray[2];
    assertFalse(vhk.isNull(0));
    assertEquals(vhk.getTimestamp(0), ts1);
  }
}