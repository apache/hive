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
package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordIdentifier {
  @Test
  public void TestOrdering() throws Exception {
    RecordIdentifier left = new RecordIdentifier(100, 200, 1200);
    RecordIdentifier right = new RecordIdentifier();
    right.setValues(100L, 200, 1000L);
    assertTrue(right.compareTo(left) < 0);
    assertTrue(left.compareTo(right) > 0);
    left.set(right);
    assertTrue(right.compareTo(left) == 0);
    right.setRowId(2000);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3);
    right.setValues(100, 2, 3);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3);
    right.setValues(1, 100, 3);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
  }

  @Test
  public void testHashEquals() throws Exception {
    long origTxn = ThreadLocalRandom.current().nextLong(1, 10000000000L);
    int bucketId = ThreadLocalRandom.current().nextInt(1, 512);
    long rowId = ThreadLocalRandom.current().nextLong(1, 10000000000L);
    long currTxn = origTxn + ThreadLocalRandom.current().nextLong(0, 10000000000L);
    int stmtId = ThreadLocalRandom.current().nextInt(1, 512);

    RecordIdentifier left = new RecordIdentifier(origTxn, bucketId, rowId);
    RecordIdentifier right = new RecordIdentifier(origTxn, bucketId, rowId);
    OrcRawRecordMerger.ReaderKey rkLeft = new OrcRawRecordMerger.ReaderKey(origTxn, bucketId, rowId, currTxn, stmtId);
    OrcRawRecordMerger.ReaderKey rkRight = new OrcRawRecordMerger.ReaderKey(origTxn, bucketId, rowId, currTxn, stmtId);

    assertEquals("RecordIdentifier.equals", left, right);
    assertEquals("RecordIdentifier.hashCode", left.hashCode(), right.hashCode());

    assertEquals("ReaderKey", rkLeft, rkLeft);
    assertEquals("ReaderKey.hashCode", rkLeft.hashCode(), rkRight.hashCode());

    //debatable if this is correct, but that's how it's implemented
    assertNotEquals("RecordIdentifier <> ReaderKey", left, rkRight);
  }
}
