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
package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFInfo;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorPTFGroupBatches {

  private Configuration getConf(int batchSize) {
    Configuration hconf = new HiveConf();
    HiveConf.setIntVar(hconf, HiveConf.ConfVars.HIVE_VECTORIZATION_TESTING_REDUCER_BATCH_SIZE,
        batchSize);
    return hconf;
  }

  @Test
  public void testTestForwardIterationWithSpill() throws HiveException {
    int numberedOfBufferedBatches = 1; // 1 block = 1 batch
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches = new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    // S[spilled records] M[in-memory records]
    init(groupBatches);
    // haven't spilled yet
    Assert.assertEquals(0, groupBatches.blocks.size());

    // 1 in memory, 0 spilled
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    Assert.assertEquals(0, groupBatches.blocks.size());

    // assert buffered batch
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{0L, 1L});
    Assert.assertEquals(2, groupBatches.size());

    // 1 in memory, 1 spilled
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] M[[2,3]]
    Assert.assertTrue(groupBatches.blocks.get(0).didSpillToDisk);
    Assert.assertEquals(batchSize, groupBatches.blocks.get(0).spillRowCount);
    Assert.assertEquals(0, groupBatches.blocks.get(0).startBatchIndex);
    // assert buffered batch
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{2L, 3L});
    Assert.assertEquals(4, groupBatches.size());

    // 1 in memory, 2 spilled
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] S[[2,3]] M[[4,5]]
    Assert.assertTrue(groupBatches.blocks.get(1).didSpillToDisk);
    Assert.assertEquals(batchSize, groupBatches.blocks.get(1).spillRowCount);
    Assert.assertEquals(1, groupBatches.blocks.get(1).startBatchIndex);
    // assert buffered batch
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(6, groupBatches.size());

    // 1 in memory, 3 spilled
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] S[[2,3]] S[[4,5]] M[[6,7]]
    Assert.assertTrue(groupBatches.blocks.get(2).didSpillToDisk);
    Assert.assertEquals(batchSize, groupBatches.blocks.get(2).spillRowCount);
    Assert.assertEquals(2, groupBatches.blocks.get(2).startBatchIndex);
    // assert buffered batch
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{6L, 7L});
    Assert.assertEquals(8, groupBatches.size());
  }

  @Test
  public void testTestForwardBackwardIterationWithSpill() throws HiveException {
    int numberedOfBufferedBatches = 1; // 1 block = 1 batch
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    // S[spilled records] M[in-memory records]
    init(groupBatches);
    // haven't spilled yet
    Assert.assertEquals(0, groupBatches.blocks.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] M[[2,3]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] S[[2,3]] M[[4,5]]
    Assert.assertEquals(6, groupBatches.size());

    // assert buffered batch M[[4,5]]
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});

    // S[[1,2]] S[[3,4]] S[[4,5]] M[[4,5]] -> S[[0,1]] S[[2,3]] [[4,5]] M[[2,3]]
    boolean hasMoved = groupBatches.previousBlock();
    Assert.assertTrue("previousBlock should have returned true while moving to M[[2,3]]", hasMoved);
    Assert.assertEquals(6, groupBatches.size());

    // first spill is untouched
    Assert.assertTrue(groupBatches.blocks.get(0).didSpillToDisk);
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillRowCount);
    Assert.assertEquals(0, groupBatches.blocks.get(0).startBatchIndex);

    // second spill is untouched
    Assert.assertTrue(groupBatches.blocks.get(1).didSpillToDisk);
    Assert.assertEquals(2, groupBatches.blocks.get(1).spillRowCount);
    Assert.assertEquals(1, groupBatches.blocks.get(1).startBatchIndex);

    // third spill: this is created while moving backward, as we needed to save currently buffered
    // batches
    Assert.assertTrue(groupBatches.blocks.get(2).didSpillToDisk);
    Assert.assertEquals(2, groupBatches.blocks.get(2).spillRowCount);
    Assert.assertEquals(2, groupBatches.blocks.get(2).startBatchIndex);

    // assert buffered batch M[[2,3]]
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{2L, 3L});

    // S[[1,2]] S[[3,4]] S[[4,5]] M[[2,3]] -> S[[1,2]] S[[3,4]] S[[4,5]] M[[4,5]]
    hasMoved = groupBatches.nextBlock();
    Assert.assertTrue("nextBlock should have returned true while moving to M[[4,5]]", hasMoved);
    Assert.assertEquals(6, groupBatches.size());

    // assert buffered batch M[[4,5]]
    Assert.assertEquals(numberedOfBufferedBatches, groupBatches.bufferedBatches.size());
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});

    hasMoved = groupBatches.nextBlock();
    Assert.assertFalse("nextBlock should have returned false, cannot move forward", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(6, groupBatches.size());

    hasMoved = groupBatches.previousBlock();
    Assert.assertTrue("previousBlock should have returned true while moving to M[[2,3]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[]{2L, 3L});
    Assert.assertEquals(6, groupBatches.size());

    hasMoved = groupBatches.previousBlock();
    Assert.assertTrue("previousBlock should have returned true while moving to M[[0,1]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[]{0L, 1L});
    Assert.assertEquals(6, groupBatches.size());

    hasMoved = groupBatches.previousBlock();
    Assert.assertFalse("previousBlock should have returned false, cannot move backward", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[]{0L, 1L});
    Assert.assertEquals(6, groupBatches.size());
  }

  @Test
  public void testRandomBlockJump() throws HiveException {
    int numberedOfBufferedBatches = 1; // 1 block = 1 batch
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    // S[spilled records] M[in-memory records]
    init(groupBatches);
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] M[[2,3]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] S[[2,3]] M[[4,5]]
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(2, groupBatches.blocks.size());
    Assert.assertEquals(6, groupBatches.size());

    groupBatches.jumpToFirstBlock();
    assertBufferedBatchValues(groupBatches, new Long[]{0L, 1L});
    // a spill happened while jumping to first
    Assert.assertEquals(3, groupBatches.blocks.size());
    Assert.assertEquals(6, groupBatches.size());

    groupBatches.jumpToLastBlock();
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(6, groupBatches.size());
  }

  @Test
  public void testJumpToLastWithUnspilledBatches() throws HiveException {
    int numberedOfBufferedBatches = 1; // 1 block = 1 batch
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    // S[spilled records] M[in-memory records]
    init(groupBatches);
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] M[[2,3]]
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1]] S[[2,3]] M[[4,5]]
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(2, groupBatches.blocks.size());
    Assert.assertEquals(6, groupBatches.size());

    // here we wan't {4L, 5L} to be spilled first, even if we jump to there, because jump should
    // be transparent regardless of the current contents of in-memory batches
    groupBatches.jumpToLastBlock();
    assertBufferedBatchValues(groupBatches, new Long[]{4L, 5L});
    Assert.assertEquals(3, groupBatches.blocks.size());
    Assert.assertEquals(6, groupBatches.size());

    groupBatches.jumpToFirstBlock();
    assertBufferedBatchValues(groupBatches, new Long[]{0L, 1L});
    Assert.assertEquals(3, groupBatches.blocks.size());
    Assert.assertEquals(6, groupBatches.size());
  }

  @Test
  public void testWithNonSingleBatchBlocks() throws HiveException {
    int numberedOfBufferedBatches = 2; // 1 block = 2 batches
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    init(groupBatches);
    // haven't spilled yet
    Assert.assertEquals(0, groupBatches.blocks.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    // haven't spilled after the first batch
    Assert.assertEquals(0, groupBatches.blocks.size());
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L });
    Assert.assertEquals(2, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1],[2,3]]
    // still haven't spilled after the second batch (blocks can contain 2 batches)
    Assert.assertEquals(0, groupBatches.blocks.size());
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(4, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1],[2,3]] M[[4,5]]
    Assert.assertEquals(1, groupBatches.blocks.size());
    Assert.assertTrue(groupBatches.blocks.get(0).didSpillToDisk);
    Assert.assertEquals(batchSize * numberedOfBufferedBatches,
        groupBatches.blocks.get(0).spillRowCount);
    Assert.assertEquals(0, groupBatches.blocks.get(0).startBatchIndex);
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L });
    Assert.assertEquals(6, groupBatches.size());

    // moving back with half full in-memory block (1 batch present)
    boolean hasMoved = groupBatches.previousBlock();
    Assert.assertTrue("previousBlock should have returned true while moving to M[[0,1],[2,3]]",
        hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(2, groupBatches.blocks.size());
    Assert.assertEquals(1, groupBatches.blocks.get(1).spillBatchCount); //it contained 1 batches when it was spilled
    Assert.assertEquals(6, groupBatches.size());

    hasMoved = groupBatches.nextBlock();
    Assert.assertTrue("nextBlock should have returned true while moving to M[[4,5]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L });
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(1, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(6, groupBatches.size());

    hasMoved = groupBatches.nextBlock();
    Assert.assertFalse("nextBlock should have returned false while staying on M[[4,5]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L });
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(1, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(6, groupBatches.size());

    //adding a new batch after moving back to this block
    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1],[2,3]] M[[4,5],[6,7]]
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L }, new Long[] { 6L, 7L });
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(1, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(8, groupBatches.size());

    hasMoved = groupBatches.previousBlock();
    Assert.assertTrue("previousBlock should have returned true while moving to M[[0,1],[2,3]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(2, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(8, groupBatches.size());

    hasMoved = groupBatches.nextBlock();
    Assert.assertTrue("nextBlock should have returned true while moving to M[[4,5],[6,7]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L }, new Long[] { 6L, 7L });
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(2, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(8, groupBatches.size());
  }

  @Test
  public void testWithNonSingleBatchAndSingleRowBatches() throws HiveException {
    int numberedOfBufferedBatches = 2; // 1 block = 2 batches
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    init(groupBatches);
    // haven't spilled yet
    Assert.assertEquals(0, groupBatches.blocks.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    // haven't spilled after the first batch
    Assert.assertEquals(0, groupBatches.blocks.size());
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L });
    Assert.assertEquals(2, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1],[2,3]]
    // still haven't spilled after the second batch (blocks can contain 2 batches)
    Assert.assertEquals(0, groupBatches.blocks.size());
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(4, groupBatches.size());

    // the point of this test to check the behavior when a batch comes that having less rows than batchSize
    groupBatches.bufferGroupBatch(createNewBatch(1, rowindex), false); // S[[0,1],[2,3]] M[[4]]
    Assert.assertEquals(1, groupBatches.blocks.size());
    Assert.assertTrue(groupBatches.blocks.get(0).didSpillToDisk);
    Assert.assertEquals(batchSize * numberedOfBufferedBatches,
        groupBatches.blocks.get(0).spillRowCount);
    Assert.assertEquals(0, groupBatches.blocks.get(0).startBatchIndex);
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L });
    Assert.assertEquals(5, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(1, rowindex), false); // S[[0,1],[2,3]] M[[4], [5]]
    assertBufferedBatchValues(groupBatches, new Long[] { 4L }, new Long[] { 5L });
    Assert.assertEquals(6, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(1, rowindex), false); // S[[0,1],[2,3]] S[[4], [5]] M[[6]]
    assertBufferedBatchValues(groupBatches, new Long[] { 6L });
    Assert.assertEquals(7, groupBatches.size());

    Assert.assertEquals(0, groupBatches.blocks.get(0).startBatchIndex);
    Assert.assertEquals(0, groupBatches.blocks.get(0).startRowIndex[0]);
    Assert.assertEquals(2, groupBatches.blocks.get(0).spillBatchCount);
    Assert.assertEquals(4, groupBatches.blocks.get(0).spillRowCount);

    Assert.assertEquals(2, groupBatches.blocks.get(1).startBatchIndex);
    Assert.assertEquals(4, groupBatches.blocks.get(1).startRowIndex[0]);
    Assert.assertEquals(2, groupBatches.blocks.get(1).spillBatchCount);
    Assert.assertEquals(2, groupBatches.blocks.get(1).spillRowCount);

    groupBatches.jumpToFirstBlock(); // 6L will be spilled, new block is created

    Assert.assertEquals(4, groupBatches.blocks.get(2).startBatchIndex);
    Assert.assertEquals(6, groupBatches.blocks.get(2).startRowIndex[0]);
    Assert.assertEquals(1, groupBatches.blocks.get(2).spillBatchCount);
    Assert.assertEquals(1, groupBatches.blocks.get(2).spillRowCount);

    groupBatches.jumpToLastBlock();
    assertBufferedBatchValues(groupBatches, new Long[] { 6L });
    /*
     * In this scenario, the in-memory pointer will jump to 6L, but so the first batch will contain
     * that value. Even if there is 2 buffered batches, only the first one should be considered, so
     * the second contains "garbage".
     */
    Assert.assertEquals(2, groupBatches.bufferedBatches.size());
    Assert.assertEquals(1, groupBatches.currentBufferedBatchCount);
    // we can also assert on the "garbage" content, 2L, 3L which is left from the previous
    // in-memory load (jumpToFirstBlock)
    assertBufferedBatchValues(groupBatches, new Long[] { 6L }, new Long[] { 2L, 3L });

    groupBatches.previousBlock();
    assertBufferedBatchValues(groupBatches, new Long[] { 4L }, new Long[] { 5L });
    Assert.assertEquals(2, groupBatches.bufferedBatches.size());
  }

  @Test
  public void testIterateOverBatches() throws HiveException {
    int numberedOfBufferedBatches = 2; // 1 block = 2 batches
    int batchSize = 2;
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberedOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    init(groupBatches);
    // haven't spilled yet
    Assert.assertEquals(0, groupBatches.blocks.size());
    Assert.assertEquals(0, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1]]
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L });
    Assert.assertEquals(2, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[] M[[0,1],[2,3]]
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(4, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1],[2,3]] M[[4,5]]
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L });
    Assert.assertEquals(6, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1],[2,3]] M[[4,5]] M[[6,7]]
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L }, new Long[] { 6L, 7L });
    Assert.assertEquals(8, groupBatches.size());

    groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false); // S[[0,1],[2,3]] S[[4,5]] S[[6,7]] M[[8,9]]
    assertBufferedBatchValues(groupBatches, new Long[] { 8L, 9L });
    Assert.assertEquals(10, groupBatches.size());

    // start iterating over
    groupBatches.jumpToFirstBlock();
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(10, groupBatches.size());

    boolean hasMoved = groupBatches.nextBlock();
    Assert.assertTrue("nextBlock should have returned true while moving to M[[4,5]] M[[6,7]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 4L, 5L }, new Long[] { 6L, 7L });
    Assert.assertEquals(10, groupBatches.size());

    hasMoved = groupBatches.nextBlock();
    Assert.assertTrue("nextBlock should have returned true while moving to M[[8,9]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 8L, 9L });
    Assert.assertEquals(10, groupBatches.size());

    hasMoved = groupBatches.nextBlock();
    Assert.assertFalse("nextBlock should have returned false while staying on  M[[8,9]]", hasMoved);
    assertBufferedBatchValues(groupBatches, new Long[] { 8L, 9L });
    Assert.assertEquals(10, groupBatches.size());

    // do the whole iteration again with while loop
    groupBatches.jumpToFirstBlock();
    assertBufferedBatchValues(groupBatches, new Long[] { 0L, 1L }, new Long[] { 2L, 3L });
    Assert.assertEquals(10, groupBatches.size());

    while (groupBatches.nextBlock()) {}
    assertBufferedBatchValues(groupBatches, new Long[] { 8L, 9L });
    Assert.assertEquals(10, groupBatches.size());
  }

  @Test
  public void testFindInmemoryBatchIndex(){
    VectorPTFGroupBatches batches = new VectorPTFGroupBatches(new Configuration(), 2);
    batches.currentBufferedBatchCount = 2;
    batches.inMemoryStartRowIndex = Arrays.asList(7, 9, 10);

    Assert.assertEquals(-1, batches.findInMemoryBatchIndex(0));
    Assert.assertEquals(-1, batches.findInMemoryBatchIndex(1));
    Assert.assertEquals(-1, batches.findInMemoryBatchIndex(6));
    Assert.assertEquals(0, batches.findInMemoryBatchIndex(7));
    Assert.assertEquals(0, batches.findInMemoryBatchIndex(8));
    Assert.assertEquals(1, batches.findInMemoryBatchIndex(9));
    Assert.assertEquals(2, batches.findInMemoryBatchIndex(10));
  }

  @Test
  public void testGetAt() {
    for (int i = 1; i < 20; i++) {
      for (int j = 1; j < 20; j++) {
        int numberOfBufferedBatches = i;
        int batchSize = j;
        try {
          runTestGetAt(batchSize, numberOfBufferedBatches);
          runTestGetAtRandomBatchSize(batchSize, numberOfBufferedBatches);
        } catch (Throwable t) {
          throw new RuntimeException(String.format(
              "Failed while testing getAt for batchSize: %d, numberOfBufferedBatches: %d", batchSize,
              numberOfBufferedBatches), t);
        }
      }
    }
  }

  private void runTestGetAt(int batchSize, int numberOfBufferedBatches) throws HiveException {
    Configuration hconf = getConf(batchSize);
    VectorPTFGroupBatches groupBatches =
        new VectorPTFGroupBatches(hconf, numberOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);

    init(groupBatches);

    for (int b = 0; b < 20; b++){
      groupBatches.bufferGroupBatch(createNewBatch(batchSize, rowindex), false);
    }
    groupBatches.preFinishPartition();

    Assert.assertEquals(20 * batchSize, groupBatches.size());

    for (int i = 0; i < groupBatches.size(); i++) {
      Assert.assertEquals("failed to assert at index: " + i, (long) i, ((Object[])groupBatches.getAt(i))[0]);
    }
  }

  /*
   * This should be also tested, as VectorPTFOperator doesn't always feed batches of the same size.
   */
  private void runTestGetAtRandomBatchSize(int maxBatchSize, int numberOfBufferedBatches)
      throws HiveException {
    Configuration hconf = getConf(maxBatchSize);
    VectorPTFGroupBatches groupBatches = new VectorPTFGroupBatches(hconf, numberOfBufferedBatches);

    AtomicLong rowindex = new AtomicLong(-1);
    Random random = new Random();
    int allSize = 0;
    init(groupBatches);

    for (int b = 0; b < 20; b++) {
      int size = Math.min(maxBatchSize, random.nextInt(maxBatchSize) + 1);
      allSize += size;
      groupBatches.bufferGroupBatch(createNewBatch(size, rowindex), false);
    }
    groupBatches.preFinishPartition();

    Assert.assertEquals(allSize, groupBatches.size());

    for (int i = 0; i < groupBatches.size(); i++) {
      Assert.assertEquals((long) i, ((Object[])groupBatches.getAt(i))[0]);
    }
  }

  private void assertBufferedBatchValues(VectorPTFGroupBatches groupBatches, Long[]...longs) {
    //cols[1]: data starts from second column as the first is the ordering key in this example
    for (int b = 0; b < longs.length; b++){ //iterate on batches
      for (int s = 0; s < longs[b].length; s++) { //iterate on longs
        Assert.assertEquals((long) longs[b][s],
            ((LongColumnVector) groupBatches.bufferedBatches.get(b).cols[0]).vector[s]);
      }
    }
  }

  /* inititialized to have state which is present in case of the following query:
   * select p_mfgr, p_name, rowindex,
   * count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
   * count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2
   * from vector_ptf_part_simple_orc;
   */
  private void init(VectorPTFGroupBatches groupBatches) throws HiveException {
    int[] outputProjectionColumnMap = new int[] { 3, 4, 0, 2, 1 };
    TypeInfo[] outputTypeInfos = new TypeInfo[] { getTypeInfo("bigint"), getTypeInfo("bigint"),
        getTypeInfo("string"), getTypeInfo("string"), getTypeInfo("int") };

    groupBatches.init(
        /* evaluators */ new VectorPTFEvaluatorBase[] {
            new VectorPTFEvaluatorCountStar(new WindowFrameDef(WindowType.RANGE,
                new BoundaryDef(Direction.PRECEDING, 1), new BoundaryDef(Direction.CURRENT, 0)),
                null, 2),
            new VectorPTFEvaluatorCountStar(new WindowFrameDef(WindowType.RANGE,
                new BoundaryDef(Direction.PRECEDING, 3), new BoundaryDef(Direction.CURRENT, 0)),
                null, 3) },
        /* outputProjectionColumnMap */ outputProjectionColumnMap,
        /* bufferedColumnMap */ new int[] { 1, 2 },
        /* bufferedTypeInfos */ new TypeInfo[] { getTypeInfo("int"), getTypeInfo("string") },
        /* orderColumnMap */ new int[] { 1 }, // p_date
        /* keyWithoutOrderColumnMap */ new int[] { 0 }, // p_mfgr
        getFakeOperator().setupOverflowBatch(3, new String[] { "bigint", "bigint" },
            outputProjectionColumnMap, outputTypeInfos));
  }

  private VectorPTFOperator getFakeOperator() throws HiveException {
    VectorPTFDesc vectorPTFDesc = new VectorPTFDesc();
    vectorPTFDesc.setVectorPTFInfo(new VectorPTFInfo());
    vectorPTFDesc.setOutputColumnNames(new String[0]);
    vectorPTFDesc.setEvaluatorFunctionNames(new String[0]);
    return new VectorPTFOperator(new CompilationOpContext(), new PTFDesc(),
        new VectorizationContext("fake"), vectorPTFDesc);
  }

  /* Create a batch like below:
   * Column vector types: 0:BYTES, 1:BYTES, 2:BYTES
   *   ["Manufacturer#1", "almond antique burnished rose metallic", "39"]
   *   ["Manufacturer#1", "almond antique burnished rose metallic", "40"]
   */
  private VectorizedRowBatch createNewBatch(int size, AtomicLong rowIndex) {
    VectorizedRowBatch batch = new VectorizedRowBatch(5, size);

    batch.cols[0] = createStringColumnVector(getRepeatingString("Manufacturer#1", size));
    batch.cols[1] =
        createLongColumnVector(getIncreasingLong(rowIndex, size));
    batch.cols[2] = createStringColumnVector(getRepeatingString("almond aquamarine rose maroon antique", size));
    batch.cols[3] = VectorizedBatchUtil.createColumnVector("bigint");
    batch.cols[4] = VectorizedBatchUtil.createColumnVector("bigint");

    return batch;
  }

  private String[] getRepeatingString(String string, int occurrences) {
    String[] strings = new String[occurrences];
    Arrays.fill(strings, string);
    return strings;
  }

  private Long[] getIncreasingLong(AtomicLong rowIndex, int occurrences) {
    Long[] longs = new Long[occurrences];
    for (int i = 0; i < occurrences; i++) {
      longs[i] = rowIndex.incrementAndGet();
    }
    return longs;
  }

  private ColumnVector createStringColumnVector(String[] strings) {
    BytesColumnVector vector = (BytesColumnVector) VectorizedBatchUtil.createColumnVector("string");
    vector.init();
    int i = 0;
    for (String string : strings) {
      vector.setVal(i, string.getBytes());
      i++;
    }
    return vector;
  }

  private ColumnVector createLongColumnVector(Long[] longs) {
    LongColumnVector vector = (LongColumnVector) VectorizedBatchUtil.createColumnVector("int");
    vector.init();
    int i = 0;
    for (Long l : longs) {
      vector.vector[i] = l;
      i++;
    }
    return vector;
  }

  private TypeInfo getTypeInfo(String typeString) {
    return TypeInfoUtils.getTypeInfoFromTypeString(typeString);
  }
}
