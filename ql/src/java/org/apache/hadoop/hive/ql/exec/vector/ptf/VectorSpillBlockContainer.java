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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.rowbytescontainer.VectorRowBytesContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * VectorSpillBlockContainer is a convenience wrapper class for handling block spills from
 * VectorPTFGroupBatches.
 */
class VectorSpillBlockContainer {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private final List<VectorSpillBlock> blocks = new ArrayList<>();
  private final int blockSize;
  private final String spillLocalDirs;
  private final int[] bufferedColumnMap;
  private final TypeInfo[] bufferedTypeInfos;

  /**
   * Should contain the index of the first batch which is now in memory. So if the the block
   * containing [M2, M3] is loaded into memory, startBatchIndexInMemory = 2.
   * [S0, S1] [M2, M3] -> 2
   */
  private int startBatchIndexInMemory = 0;

  /**
   * VectorSpillBlock is an abstraction of spilled VectorizedRowBatch instances.
   * A block contains blockSize batches.
   * As we support forward/backward movement on batches involving spills,
   * we need to be able randomly spill and load a block of vectorized row batches.
   *
   * A simple scenario which is possible with this approach:
   * 1. buffering batches
   * [] [M0]
   * [] [M0 M1]
   *
   * 2. further buffering batches, spill current in-memory content if it's full
   * [S0 S1] [M2]
   * [S0 S1] [M2 M3]
   * [S0 S1] [S2 S3] [M4]
   * -> [S0 S1] has startBatchIndex = 0
   * -> [S2 S3] has startBatchIndex = 2
   *
   * 3. moving backward (previousBlock), want to have S2,S3 in the memory,
   * so need to spill M4 if it's not spilled earlier:
   * [S0 S1] [S2 S3] [S4   ] [M2 M3]
   *
   * 4. moving forward (nextBlock), want to have S4 in the memory again:
   * [S0 S1] [S2 S3] [S4   ] [M4 ]
   */
  @SuppressWarnings("rawtypes")
  class VectorSpillBlock {
    /**
     * Convenience index of the block.
     */
    final int blockIndex;

    /**
     * Indicates if a spill has been invoked on this block.
     */
    @VisibleForTesting
    boolean didSpillToDisk;

    /**
     * Indicates the number of spilled rows. As the incoming batches can have different number of
     * rows, this needs to be tracked, as this cannot be calculated simply by spillBatchCount *
     * "some batch size"
     */
    @VisibleForTesting
    long spillRowCount;

    /**
     * The number of currently spilled batches, that is incremented through a spilling loop. After
     * the block is full, this is supposed to be equal to blockSize
     */
    int spillBatchCount;

    /**
     * The logical index of the first batch in this block. Assuming that we have 25 batches in the memory, which is the same as the block size:
     * 0. block:    startBatchIndex =  0
     * 1. block:    startBatchIndex = 25
     * 2. block:    startBatchIndex = 50
     * ...
     * n.th block:  startBatchIndex = n * blockSize
     */
    final int startBatchIndex;

    /**
     * When vectorized row batches arrive to bufferGroupBatch, they can have different sizes
     * (different number of rows in them). We primarily have control on the number of batches held
     * in memory, not on the number of rows, as we need to spill incoming batches as they are
     * without any effort to compact smaller batches into larger once. So the starting row index of
     * a spill block cannot be calculated by a simple multiplication (batch number * rows per
     * batch), so we need to keep track of the starting row index of a block, this is what's
     * startRowIndex for, which keeps track of indices of all batch-starting rows.
     *
     * So startRowIndex[0] is the index of the first record in the first batch in this group, so the
     * index of the very-first record in this block.
     */
    int[] startRowIndex;

    /**
     * Stores the isLastGroupBatch property for buffered/spilled batches. Some evaluators do work
     * when an ordering group changes (evaluator.doLastBatchWork()).
     */
    boolean[] isLastGroupBatch;

    /**
     * Stores the isInputExpressionEvaluated property for buffered/spilled batches.
     */
    boolean[] isInputExpressionEvaluated;
    /**
     * <pre>
      * Deserializer needs to know which columns to put the deserialized values into. Basically,
      * deserialization can happen in 2 cases:
      * 1. final read: deserialized values should go back into
      * the proper output columns in the overflow batch, the mapping is stored in
      * bufferedColumnMap.
      * 2. "intermediate" read: deserialized values should be just put back into
      * columns in the same order as they arrived to the serializer (like: [0, 1, 2, 3, 4, 5]
     * </pre>
     */
    private boolean doFinalRead = false;

    private VectorRowBytesContainer spillRowBytesContainer;
    private transient VectorSerializeRow bufferedBatchVectorSerializeRow;
    private transient VectorDeserializeRow bufferedBatchVectorDeserializeRow;

    VectorSpillBlock(int blockIndex) {
      this.blockIndex = blockIndex;
      startBatchIndex = blockIndex * blockSize;
      startRowIndex = new int[blockSize];
      isLastGroupBatch = new boolean[blockSize];
      isInputExpressionEvaluated = new boolean[blockSize];
    }

    void releaseRowBytesContainer() {
      if (spillRowBytesContainer != null){
        spillRowBytesContainer.clear();
        spillRowBytesContainer = null;
      }
    }

    VectorRowBytesContainer getSpillRowBytesContainer() throws HiveException {
      if (spillRowBytesContainer == null) {
        spillRowBytesContainer = new VectorRowBytesContainer(spillLocalDirs);

        if (bufferedBatchVectorSerializeRow == null) {
          initSerialization();
        }
      }
      return spillRowBytesContainer;
    }

    VectorSerializeRow getVectorSerializeRow() throws HiveException {
      if (bufferedBatchVectorSerializeRow == null) {
        initSerialization();
      }
      return bufferedBatchVectorSerializeRow;
    }

    private void initSerialization() throws HiveException {
      initVectorSerializeRow();
      initVectorDeserializeRow();
    }

    private void initVectorSerializeRow() throws HiveException {
      bufferedBatchVectorSerializeRow = new VectorSerializeRow<LazyBinarySerializeWrite>(
              new LazyBinarySerializeWrite(bufferedColumnMap.length));

      // Deserialize just the columns we a buffered batch, which has only the non-key inputs and
      // streamed column outputs.
      bufferedBatchVectorSerializeRow.init(bufferedTypeInfos);
    }

    private void initVectorDeserializeRow() throws HiveException{
      bufferedBatchVectorDeserializeRow =
          new VectorDeserializeRow<LazyBinaryDeserializeRead>(
              new LazyBinaryDeserializeRead(
                  bufferedTypeInfos,
                  /* useExternalBuffer */ true));

      // Deserialize the fields into a batch using the buffered batch column map.
      bufferedBatchVectorDeserializeRow.init(
          doFinalRead ? bufferedColumnMap : IntStream.range(0, bufferedColumnMap.length).toArray());
    }

    public boolean isEmpty() {
      return spillRowCount == 0;
    }

    public void setDoFinalRead(boolean doFinalRead) throws HiveException {
      this.doFinalRead = doFinalRead;
      initVectorDeserializeRow();
    }

    @Override
    public String toString() {
      return String.format(
          "[%s: blockIndex: %d, startBatchIndex: %d, startRowIndex: %s, spilled: %s, spillBatchCount: %d, spillRowCount: %d]",
          getClass().getSimpleName(), blockIndex, startBatchIndex, Arrays.toString(startRowIndex),
          didSpillToDisk, spillBatchCount, spillRowCount);
    }

    public void cleanup() {
      releaseRowBytesContainer();
    }

    public boolean isFullySpilled() {
      return didSpillToDisk && spillBatchCount == blockSize;
    }

    public int getLastRowIndex() {
      return (int) (startRowIndex[0] + spillRowCount - 1);
    }

    public VectorDeserializeRow getBufferedBatchVectorDeserializeRow() {
      return bufferedBatchVectorDeserializeRow;
    }

    public void spillBatch(BufferedVectorizedRowBatch batch) throws HiveException {
      VectorRowBytesContainer rowBytesContainer = getSpillRowBytesContainer();
      VectorSerializeRow vectorSerializeRow = getVectorSerializeRow();

      final boolean selectedInUse = batch.selectedInUse;
      int[] selected = batch.selected;
      final int size = batch.size;

      try {
        for (int logicalIndex = 0; logicalIndex < size; logicalIndex++) {
          final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);

          Output output = rowBytesContainer.getOutputForRowBytes();
          vectorSerializeRow.setOutputAppend(output);
          vectorSerializeRow.serializeWrite(batch, batchIndex);
          rowBytesContainer.finishRow();
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    /**
     * Reads a single row from a VectorRowBytesContainer. Caller is responsible for calling and
     * checking readNext in advance.
     *
     * @param batch
     * @param vectorDeserializeRow
     * @param rowBytesContainer
     * @throws HiveException
     */
    public void readSingleRowFromBytesContainer(VectorizedRowBatch batch) throws HiveException {
      VectorRowBytesContainer rowBytesContainer = getSpillRowBytesContainer();
      VectorDeserializeRow vectorDeserializeRow = getBufferedBatchVectorDeserializeRow();

      byte[] bytes = rowBytesContainer.currentBytes();
      int offset = rowBytesContainer.currentOffset();
      int length = rowBytesContainer.currentLength();

      vectorDeserializeRow.setBytes(bytes, offset, length);
      try {
        vectorDeserializeRow.deserialize(batch, batch.size);
      } catch (Exception e) {
        throw new HiveException(
            "\nDeserializeRead detail: " + vectorDeserializeRow.getDetailedReadPositionString(), e);
      }
    }
  }

  public VectorSpillBlockContainer(int blockSize, String spillLocalDirs,
      int[] bufferedColumnMap, TypeInfo[] bufferedTypeInfos) {
    this.blockSize = blockSize;
    this.spillLocalDirs = spillLocalDirs;
    this.bufferedColumnMap = bufferedColumnMap;
    this.bufferedTypeInfos = bufferedTypeInfos;
  }

  int size() {
    return blocks.size();
  }

  boolean isEmpty() {
    return blocks.isEmpty();
  }

  VectorSpillBlock get(int i) {
    return blocks.get(i);
  }

  boolean add(VectorSpillBlock block) {
    return blocks.add(block);
  }

  void clear() {
    for (VectorSpillBlock block : blocks) {
      block.cleanup();
    }
    blocks.clear();
    startBatchIndexInMemory = 0;
  }

  int getRowSize(int countBufferedRows) {
    int count = 0;
    boolean countedInMemoryBlock = false;

    for (VectorSpillBlock block : blocks) {
      if (block.startBatchIndex == startBatchIndexInMemory) {
        count += countBufferedRows;
        countedInMemoryBlock = true;
      } else {
        count += block.spillRowCount;
      }
    }
    if (!countedInMemoryBlock) {
      count += countBufferedRows;
    }
    return count;
  }

  void updateStartBatchIndexInMemory() {
    this.startBatchIndexInMemory = blocks.get(blocks.size() - 1).startBatchIndex + blockSize;
  }

  boolean isBlockInMemory(VectorSpillBlock block) {
    return block.startBatchIndex == startBatchIndexInMemory;
  }

  int getStartBatchIndexInMemory() {
    return startBatchIndexInMemory;
  }

  void setStartBatchIndexInMemory(int startBatchIndexInMemory) {
    this.startBatchIndexInMemory = startBatchIndexInMemory;
  }

  void setStartBatchIndexInMemory(VectorSpillBlock block) {
    this.startBatchIndexInMemory = block.startBatchIndex;
  }

  VectorSpillBlock getLast() {
    return blocks.get(blocks.size() - 1);
  }

  boolean isStandingOnLast() {
    return isStandingOnBlock(getLast());
  }

  boolean isStandingOnFirst() {
    return startBatchIndexInMemory <= 0;
  }

  boolean isStandingOnBlock(VectorSpillBlock block) {
    return block.startBatchIndex == startBatchIndexInMemory;
  }

  VectorSpillBlock getCurrentBlock() {
    int spillBlockIndex = this.startBatchIndexInMemory / blockSize;
    VectorSpillBlock block;

    if (spillBlockIndex > blocks.size() - 1) {
      LOG.debug("Creating new spill group for current buffered batches, block index: {}", spillBlockIndex);
      block = newBlock(spillBlockIndex);

      if (spillBlockIndex > 0) {
        block.startRowIndex[0] = (int) (blocks.get(spillBlockIndex - 1).startRowIndex[0]
            + blocks.get(spillBlockIndex - 1).spillRowCount);
      }
      blocks.add(block);
    } else {
      block = blocks.get(spillBlockIndex);
    }
    return block;
  }

  private VectorSpillBlock newBlock(int spillBlockIndex) {
    return new VectorSpillBlock(spillBlockIndex);
  }
}