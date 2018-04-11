/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.common.util;

import java.util.BitSet;

import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTxnIdUtils {

  @Test
  public void testCheckEquivalentWriteIds() throws Exception {
    ValidReaderWriteIdList id1 = new ValidReaderWriteIdList("default.table1",
        new long[] {1,2,3,4,5}, new BitSet(), 5, 1);
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id1));

    // write ID with additional uncommitted IDs. Should match.
    ValidReaderWriteIdList id2 = new ValidReaderWriteIdList("default.table1",
        new long[] {1,2,3,4,5,6,7,8}, new BitSet(), 8, 1);
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id2));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id2, id1));

    // ID 1 has been committed, all others open
    ValidReaderWriteIdList id3 = new ValidReaderWriteIdList("default.table1",
        new long[] {2,3,4,5,6,7,8}, new BitSet(), 8, 2);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id3));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id3, id2));

    // ID 5 has been committed, all others open
    ValidReaderWriteIdList id4 = new ValidReaderWriteIdList("default.table1",
        new long[] {1,2,3,4,6,7,8}, new BitSet(), 8, 1);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id4));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id4, id2));

    // ID 8 was committed, all others open
    ValidReaderWriteIdList id5 = new ValidReaderWriteIdList("default.table1",
        new long[] {1,2,3,4,6,7}, new BitSet(), 8, 1);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id5));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id5, id2));

    // Different table name
    ValidReaderWriteIdList id6 = new ValidReaderWriteIdList("default.tab2",
        new long[] {1,2,3,4,5}, new BitSet(), 5, 1);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id6));

    // WriteID for table1, way in the future
    ValidReaderWriteIdList id7 = new ValidReaderWriteIdList("default.table1",
        new long[] {100, 101, 105}, new BitSet(), 105, 100);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id7));

    // Before any activity on the table, no open IDs
    ValidReaderWriteIdList id8 = new ValidReaderWriteIdList("default.table1",
        new long[] {}, new BitSet(), 0);
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id8));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id8, id2));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id8, id3));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id8, id7));

    // ID 5 committed, no open IDs
    ValidReaderWriteIdList id9 = new ValidReaderWriteIdList("default.table1",
        new long[] {}, new BitSet(), 5);
    ValidReaderWriteIdList id10 = new ValidReaderWriteIdList("default.table1",
        new long[] {}, new BitSet(), 5);
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id9, id10));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id8, id9));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id9, id8));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id9, id1));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id9, id2));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id9, id7));
  }

  @Test
  public void testCheckEquivalentWriteIds2() throws Exception {
    ValidReaderWriteIdList id1 = new ValidReaderWriteIdList("default.table1",
        new long[] {}, new BitSet(), 5);
    ValidReaderWriteIdList id2 = new ValidReaderWriteIdList("default.table1",
        new long[] {6}, BitSet.valueOf(new byte[]{1}), 6);
    ValidReaderWriteIdList id3 = new ValidReaderWriteIdList("default.table1",
        new long[] {6,7}, BitSet.valueOf(new byte[]{1,0}), 7, 7);
    ValidReaderWriteIdList id4 = new ValidReaderWriteIdList("default.table1",
        new long[] {6,7,8}, BitSet.valueOf(new byte[]{1,0,1}), 8, 7);

    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id2));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id3));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id1, id4));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id2, id3));
    assertTrue(TxnIdUtils.checkEquivalentWriteIds(id2, id4));

    // If IDs 6,7,8 were all aborted and the metadata cleaned up, we would lose the record
    // of the aborted IDs. In this case we are not able to determine the new WriteIDList has
    // an equivalent commit state compared to the previous WriteIDLists.
    ValidReaderWriteIdList id5 = new ValidReaderWriteIdList("default.table1",
        new long[] {}, new BitSet(), 8);
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id1, id5));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id2, id5));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id3, id5));
    assertFalse(TxnIdUtils.checkEquivalentWriteIds(id4, id5));
  }
}
