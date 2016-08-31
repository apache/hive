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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
/**
 * A fast vectorized batch reader class for ACID when split-update behavior is enabled.
 * When split-update is turned on, row-by-row stitching could be avoided to create the final
 * version of a row. Essentially, there are only insert and delete events. Insert events can be
 * directly read from the base files/insert_only deltas in vectorized row batches. The deleted
 * rows can then be easily indicated via the 'selected' field of the vectorized row batch.
 * Refer HIVE-14233 for more details.
 */
public class VectorizedOrcAcidRowBatchReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable,VectorizedRowBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedOrcAcidRowBatchReader.class);

  private org.apache.hadoop.hive.ql.io.orc.RecordReader baseReader;
  private VectorizedRowBatchCtx rbCtx;
  private VectorizedRowBatch vectorizedRowBatchBase;
  private long offset;
  private long length;
  private float progress = 0.0f;
  private Object[] partitionValues;
  private boolean addPartitionCols = true;
  private ValidTxnList validTxnList;
  private DeleteEventRegistry deleteEventRegistry;

  public VectorizedOrcAcidRowBatchReader(InputSplit inputSplit, JobConf conf,
        Reporter reporter) throws IOException {

    final boolean isAcidRead = HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
    final AcidUtils.AcidOperationalProperties acidOperationalProperties
            = AcidUtils.getAcidOperationalProperties(conf);

    // This type of VectorizedOrcAcidRowBatchReader can only be created when split-update is
    // enabled for an ACID case and the file format is ORC.
    boolean isReadNotAllowed = !isAcidRead || !acidOperationalProperties.isSplitUpdate()
                                   || !(inputSplit instanceof OrcSplit);
    if (isReadNotAllowed) {
      OrcInputFormat.raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }
    final OrcSplit orcSplit = (OrcSplit) inputSplit;

    rbCtx = Utilities.getVectorizedRowBatchCtx(conf);

    reporter.setStatus(orcSplit.toString());
    Reader reader = OrcInputFormat.createOrcReaderForSplit(conf, orcSplit);
    Reader.Options readerOptions = OrcInputFormat.createOptionsForReader(conf);
    readerOptions = OrcRawRecordMerger.createEventOptions(readerOptions);

    this.offset = orcSplit.getStart();
    this.length = orcSplit.getLength();

    // Careful with the range here now, we do not want to read the whole base file like deltas.
    this.baseReader = reader.rowsOptions(readerOptions.range(offset, length));

    // VectorizedRowBatchBase schema is picked up from the baseReader because the SchemaEvolution
    // stuff happens at the ORC layer that understands how to map user schema to acid schema.
    if (this.baseReader instanceof RecordReaderImpl) {
      this.vectorizedRowBatchBase = ((RecordReaderImpl) this.baseReader).createRowBatch();
    } else {
      throw new IOException("Failed to create vectorized row batch for the reader of type "
          + this.baseReader.getClass().getName());
    }

    int partitionColumnCount = (rbCtx != null) ? rbCtx.getPartitionColumnCount() : 0;
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, orcSplit, partitionValues);
    } else {
      partitionValues = null;
    }

    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    this.validTxnList = (txnString == null) ? new ValidReadTxnList() : new ValidReadTxnList(txnString);

    // Clone readerOptions for deleteEvents.
    Reader.Options deleteEventReaderOptions = readerOptions.clone();
    // Set the range on the deleteEventReaderOptions to 0 to INTEGER_MAX because
    // we always want to read all the delete delta files.
    deleteEventReaderOptions.range(0, Long.MAX_VALUE);
    //  Disable SARGs for deleteEventReaders, as SARGs have no meaning.
    deleteEventReaderOptions.searchArgument(null, null);
    try {
      // See if we can load all the delete events from all the delete deltas in memory...
      this.deleteEventRegistry = new ColumnizedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions);
    } catch (DeleteEventsOverflowMemoryException e) {
      // If not, then create a set of hanging readers that do sort-merge to find the next smallest
      // delete event on-demand. Caps the memory consumption to (some_const * no. of readers).
      this.deleteEventRegistry = new SortMergedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions);
    }
  }

  /**
   * Returns whether it is possible to create a valid instance of this class for a given split.
   * @param conf is the job configuration
   * @param inputSplit
   * @return true if it is possible, else false.
   */
  public static boolean canCreateVectorizedAcidRowBatchReaderOnSplit(JobConf conf, InputSplit inputSplit) {
    if (!(inputSplit instanceof OrcSplit)) {
      return false; // must be an instance of OrcSplit.
    }
    // First check if we are reading any original files in the split.
    // To simplify the vectorization logic, the vectorized acid row batch reader does not handle
    // original files for now as they have a different schema than a regular ACID file.
    final OrcSplit split = (OrcSplit) inputSplit;
    if (AcidUtils.getAcidOperationalProperties(conf).isSplitUpdate() && !split.isOriginal()) {
      // When split-update is turned on for ACID, a more optimized vectorized batch reader
      // can be created. But still only possible when we are *NOT* reading any originals.
      return true;
    }
    return false; // no split-update or possibly reading originals!
  }

  private static Path[] getDeleteDeltaDirsFromSplit(OrcSplit orcSplit) throws IOException {
    Path path = orcSplit.getPath();
    Path root;
    if (orcSplit.hasBase()) {
      if (orcSplit.isOriginal()) {
        root = path.getParent();
      } else {
        root = path.getParent().getParent();
      }
    } else {
      root = path;
    }
    return AcidUtils.deserializeDeleteDeltas(root, orcSplit.getDeltas());
  }

  @Override
  public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
    try {
      // Check and update partition cols if necessary. Ideally, this should be done
      // in CreateValue as the partition is constant per split. But since Hive uses
      // CombineHiveRecordReader and
      // as this does not call CreateValue for each new RecordReader it creates, this check is
      // required in next()
      if (addPartitionCols) {
        if (partitionValues != null) {
          rbCtx.addPartitionColsToBatch(value, partitionValues);
        }
        addPartitionCols = false;
      }
      if (!baseReader.nextBatch(vectorizedRowBatchBase)) {
        return false;
      }
    } catch (Exception e) {
      throw new IOException("error iterating", e);
    }

    // Once we have read the VectorizedRowBatchBase from the file, there are two kinds of cases
    // for which we might have to discard rows from the batch:
    // Case 1- when the row is created by a transaction that is not valid, or
    // Case 2- when the row has been deleted.
    // We will go through the batch to discover rows which match any of the cases and specifically
    // remove them from the selected vector. Of course, selectedInUse should also be set.

    BitSet selectedBitSet = new BitSet(vectorizedRowBatchBase.size);
    if (vectorizedRowBatchBase.selectedInUse) {
      // When selectedInUse is true, start with every bit set to false and selectively set
      // certain bits to true based on the selected[] vector.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, false);
      for (int j = 0; j < vectorizedRowBatchBase.size; ++j) {
        int i = vectorizedRowBatchBase.selected[j];
        selectedBitSet.set(i);
      }
    } else {
      // When selectedInUse is set to false, everything in the batch is selected.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, true);
    }

    // Case 1- find rows which belong to transactions that are not valid.
    findRecordsWithInvalidTransactionIds(vectorizedRowBatchBase, selectedBitSet);

    // Case 2- find rows which have been deleted.
    this.deleteEventRegistry.findDeletedRecords(vectorizedRowBatchBase, selectedBitSet);

    if (selectedBitSet.cardinality() == vectorizedRowBatchBase.size) {
      // None of the cases above matched and everything is selected. Hence, we will use the
      // same values for the selected and selectedInUse.
      value.size = vectorizedRowBatchBase.size;
      value.selected = vectorizedRowBatchBase.selected;
      value.selectedInUse = vectorizedRowBatchBase.selectedInUse;
    } else {
      value.size = selectedBitSet.cardinality();
      value.selectedInUse = true;
      value.selected = new int[selectedBitSet.cardinality()];
      // This loop fills up the selected[] vector with all the index positions that are selected.
      for (int setBitIndex = selectedBitSet.nextSetBit(0), selectedItr = 0;
           setBitIndex >= 0;
           setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1), ++selectedItr) {
        value.selected[selectedItr] = setBitIndex;
      }
    }

    // Finally, link up the columnVector from the base VectorizedRowBatch to outgoing batch.
    // NOTE: We only link up the user columns and not the ACID metadata columns because this
    // vectorized code path is not being used in cases of update/delete, when the metadata columns
    // would be expected to be passed up the operator pipeline. This is because
    // currently the update/delete specifically disable vectorized code paths.
    // This happens at ql/exec/Utilities.java::3293 when it checks for mapWork.getVectorMode()
    StructColumnVector payloadStruct = (StructColumnVector) vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW];
    // Transfer columnVector objects from base batch to outgoing batch.
    System.arraycopy(payloadStruct.fields, 0, value.cols, 0, value.getDataColumnCount());
    progress = baseReader.getProgress();
    return true;
  }

  private void findRecordsWithInvalidTransactionIds(VectorizedRowBatch batch, BitSet selectedBitSet) {
    if (batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid transaction.
      long currentTransactionIdForBatch = ((LongColumnVector)
          batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector[0];
      if (!validTxnList.isTxnValid(currentTransactionIdForBatch)) {
        selectedBitSet.clear(0, batch.size);
      }
      return;
    }
    long[] currentTransactionVector =
        ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector;
    // Loop through the bits that are set to true and mark those rows as false, if their
    // current transactions are not valid.
    for (int setBitIndex = selectedBitSet.nextSetBit(0);
        setBitIndex >= 0;
        setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
      if (!validTxnList.isTxnValid(currentTransactionVector[setBitIndex])) {
        selectedBitSet.clear(setBitIndex);
      }
   }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return offset + (long) (progress * length);
  }

  @Override
  public void close() throws IOException {
    try {
      this.baseReader.close();
    } finally {
      this.deleteEventRegistry.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return progress;
  }

  @VisibleForTesting
  DeleteEventRegistry getDeleteEventRegistry() {
    return deleteEventRegistry;
  }

  /**
   * An interface that can determine which rows have been deleted
   * from a given vectorized row batch. Implementations of this interface
   * will read the delete delta files and will create their own internal
   * data structures to maintain record ids of the records that got deleted.
   */
  static interface DeleteEventRegistry {
    /**
     * Modifies the passed bitset to indicate which of the rows in the batch
     * have been deleted. Assumes that the batch.size is equal to bitset size.
     * @param batch
     * @param selectedBitSet
     * @throws IOException
     */
    public void findDeletedRecords(VectorizedRowBatch batch, BitSet selectedBitSet) throws IOException;

    /**
     * The close() method can be called externally to signal the implementing classes
     * to free up resources.
     * @throws IOException
     */
    public void close() throws IOException;
  }

  /**
   * An implementation for DeleteEventRegistry that opens the delete delta files all
   * at once, and then uses the sort-merge algorithm to maintain a sorted list of
   * delete events. This internally uses the OrcRawRecordMerger and maintains a constant
   * amount of memory usage, given the number of delete delta files. Therefore, this
   * implementation will be picked up when the memory pressure is high.
   */
  static class SortMergedDeleteEventRegistry implements DeleteEventRegistry {
    private OrcRawRecordMerger deleteRecords;
    private OrcRawRecordMerger.ReaderKey deleteRecordKey;
    private OrcStruct deleteRecordValue;
    private boolean isDeleteRecordAvailable = true;
    private ValidTxnList validTxnList;

    public SortMergedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit, Reader.Options readerOptions)
      throws IOException {
        final Path[] deleteDeltas = getDeleteDeltaDirsFromSplit(orcSplit);
        if (deleteDeltas.length > 0) {
          int bucket = AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf).getBucket();
          String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
          this.validTxnList = (txnString == null) ? new ValidReadTxnList() : new ValidReadTxnList(txnString);
          this.deleteRecords = new OrcRawRecordMerger(conf, true, null, false, bucket,
                                                      validTxnList, readerOptions, deleteDeltas);
          this.deleteRecordKey = new OrcRawRecordMerger.ReaderKey();
          this.deleteRecordValue = this.deleteRecords.createValue();
          // Initialize the first value in the delete reader.
          this.isDeleteRecordAvailable = this.deleteRecords.next(deleteRecordKey, deleteRecordValue);
        } else {
          this.isDeleteRecordAvailable = false;
          this.deleteRecordKey = null;
          this.deleteRecordValue = null;
          this.deleteRecords = null;
        }
    }

    @Override
    public void findDeletedRecords(VectorizedRowBatch batch, BitSet selectedBitSet)
        throws IOException {
      if (!isDeleteRecordAvailable) {
        return;
      }

      long[] originalTransaction =
          batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? null
              : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector;
      long[] bucket =
          batch.cols[OrcRecordUpdater.BUCKET].isRepeating ? null
              : ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector;
      long[] rowId =
          batch.cols[OrcRecordUpdater.ROW_ID].isRepeating ? null
              : ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector;

      // The following repeatedX values will be set, if any of the columns are repeating.
      long repeatedOriginalTransaction = (originalTransaction != null) ? -1
          : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[0];
      long repeatedBucket = (bucket != null) ? -1
          : ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector[0];
      long repeatedRowId = (rowId != null) ? -1
          : ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[0];


      // Get the first valid row in the batch still available.
      int firstValidIndex = selectedBitSet.nextSetBit(0);
      if (firstValidIndex == -1) {
        return; // Everything in the batch has already been filtered out.
      }
      RecordIdentifier firstRecordIdInBatch =
          new RecordIdentifier(
              originalTransaction != null ? originalTransaction[firstValidIndex] : repeatedOriginalTransaction,
              bucket != null ? (int) bucket[firstValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[firstValidIndex] : repeatedRowId);

      // Get the last valid row in the batch still available.
      int lastValidIndex = selectedBitSet.previousSetBit(batch.size - 1);
      RecordIdentifier lastRecordIdInBatch =
          new RecordIdentifier(
              originalTransaction != null ? originalTransaction[lastValidIndex] : repeatedOriginalTransaction,
              bucket != null ? (int) bucket[lastValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[lastValidIndex] : repeatedRowId);

      // We must iterate over all the delete records, until we find one record with
      // deleteRecord >= firstRecordInBatch or until we exhaust all the delete records.
      while (deleteRecordKey.compareRow(firstRecordIdInBatch) == -1) {
        isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        if (!isDeleteRecordAvailable) return; // exhausted all delete records, return.
      }

      // If we are here, then we have established that firstRecordInBatch <= deleteRecord.
      // Now continue marking records which have been deleted until we reach the end of the batch
      // or we exhaust all the delete records.

      int currIndex = firstValidIndex;
      RecordIdentifier currRecordIdInBatch = new RecordIdentifier();
      while (isDeleteRecordAvailable && currIndex != -1 && currIndex <= lastValidIndex) {
        currRecordIdInBatch.setValues(
            (originalTransaction != null) ? originalTransaction[currIndex] : repeatedOriginalTransaction,
            (bucket != null) ? (int) bucket[currIndex] : (int) repeatedBucket,
            (rowId != null) ? rowId[currIndex] : repeatedRowId);

        if (deleteRecordKey.compareRow(currRecordIdInBatch) == 0) {
          // When deleteRecordId == currRecordIdInBatch, this record in the batch has been deleted.
          selectedBitSet.clear(currIndex);
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else if (deleteRecordKey.compareRow(currRecordIdInBatch) == 1) {
          // When deleteRecordId > currRecordIdInBatch, we have to move on to look at the
          // next record in the batch.
          // But before that, can we short-circuit and skip the entire batch itself
          // by checking if the deleteRecordId > lastRecordInBatch?
          if (deleteRecordKey.compareRow(lastRecordIdInBatch) == 1) {
            return; // Yay! We short-circuited, skip everything remaining in the batch and return.
          }
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else {
          // We have deleteRecordId < currRecordIdInBatch, we must now move on to find
          // next the larger deleteRecordId that can possibly match anything in the batch.
          isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (this.deleteRecords != null) {
        this.deleteRecords.close();
      }
    }
  }

  /**
   * An implementation for DeleteEventRegistry that optimizes for performance by loading
   * all the delete events into memory at once from all the delete delta files.
   * It starts by reading all the delete events through a regular sort merge logic
   * into two vectors- one for original transaction id (otid), and the other for row id.
   * (In the current version, since the bucket id should be same for all the delete deltas,
   * it is not stored). The otids are likely to be repeated very often, as a single transaction
   * often deletes thousands of rows. Hence, the otid vector is compressed to only store the
   * toIndex and fromIndex ranges in the larger row id vector. Now, querying whether a
   * record id is deleted or not, is done by performing a binary search on the
   * compressed otid range. If a match is found, then a binary search is then performed on
   * the larger rowId vector between the given toIndex and fromIndex. Of course, there is rough
   * heuristic that prevents creation of an instance of this class if the memory pressure is high.
   * The SortMergedDeleteEventRegistry is then the fallback method for such scenarios.
   */
   static class ColumnizedDeleteEventRegistry implements DeleteEventRegistry {
    /**
     * A simple wrapper class to hold the (otid, rowId) pair.
     */
    static class DeleteRecordKey implements Comparable<DeleteRecordKey> {
      private long originalTransactionId;
      private long rowId;
      public DeleteRecordKey() {
        this.originalTransactionId = -1;
        this.rowId = -1;
      }
      public DeleteRecordKey(long otid, long rowId) {
        this.originalTransactionId = otid;
        this.rowId = rowId;
      }
      public void set(long otid, long rowId) {
        this.originalTransactionId = otid;
        this.rowId = rowId;
      }

      @Override
      public int compareTo(DeleteRecordKey other) {
        if (other == null) {
          return -1;
        }
        if (originalTransactionId != other.originalTransactionId) {
          return originalTransactionId < other.originalTransactionId ? -1 : 1;
        }
        if (rowId != other.rowId) {
          return rowId < other.rowId ? -1 : 1;
        }
        return 0;
      }
    }

    /**
     * This class actually reads the delete delta files in vectorized row batches.
     * For every call to next(), it returns the next smallest record id in the file if available.
     * Internally, the next() buffers a row batch and maintains an index pointer, reading the
     * next batch when the previous batch is exhausted.
     */
    static class DeleteReaderValue {
      private VectorizedRowBatch batch;
      private final RecordReader recordReader;
      private int indexPtrInBatch;
      private final int bucketForSplit; // The bucket value should be same for all the records.
      private final ValidTxnList validTxnList;

      public DeleteReaderValue(Reader deleteDeltaReader, Reader.Options readerOptions, int bucket,
          ValidTxnList validTxnList) throws IOException {
        this.recordReader  = deleteDeltaReader.rowsOptions(readerOptions);
        this.bucketForSplit = bucket;
        this.batch = deleteDeltaReader.getSchema().createRowBatch();
        if (!recordReader.nextBatch(batch)) { // Read the first batch.
          this.batch = null; // Oh! the first batch itself was null. Close the reader.
        }
        this.indexPtrInBatch = 0;
        this.validTxnList = validTxnList;
      }

      public boolean next(DeleteRecordKey deleteRecordKey) throws IOException {
        if (batch == null) {
          return false;
        }
        boolean isValidNext = false;
        while (!isValidNext) {
          if (indexPtrInBatch >= batch.size) {
            // We have exhausted our current batch, read the next batch.
            if (recordReader.nextBatch(batch)) {
              // Whenever we are reading a batch, we must ensure that all the records in the batch
              // have the same bucket id as the bucket id of the split. If not, throw exception.
              // NOTE: this assertion might not hold, once virtual bucketing is in place. However,
              // it should be simple to fix that case. Just replace check for bucket equality with
              // a check for valid bucket mapping. Until virtual bucketing is added, it means
              // either the split computation got messed up or we found some corrupted records.
              long bucketForRecord = ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector[0];
              if ((batch.size > 1 && !batch.cols[OrcRecordUpdater.BUCKET].isRepeating)
                  || (bucketForRecord != bucketForSplit)){
                throw new IOException("Corrupted records with different bucket ids "
                    + "from the containing bucket file found! Expected bucket id "
                    + bucketForSplit + ", however found the bucket id " + bucketForRecord);
              }
              indexPtrInBatch = 0; // After reading the batch, reset the pointer to beginning.
            } else {
              return false; // no more batches to read, exhausted the reader.
            }
          }
          int originalTransactionIndex =
              batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? 0 : indexPtrInBatch;
          long originalTransaction =
              ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[originalTransactionIndex];
          long rowId = ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[indexPtrInBatch];
          int currentTransactionIndex =
              batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION].isRepeating ? 0 : indexPtrInBatch;
          long currentTransaction =
              ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector[currentTransactionIndex];
          ++indexPtrInBatch;
          if (validTxnList.isTxnValid(currentTransaction)) {
            isValidNext = true;
            deleteRecordKey.set(originalTransaction, rowId);
          }
        }
        return true;
      }

      public void close() throws IOException {
        this.recordReader.close();
      }
    }

    /**
     * A CompressedOtid class stores a compressed representation of the original
     * transaction ids (otids) read from the delete delta files. Since the record ids
     * are sorted by (otid, rowId) and otids are highly likely to be repetitive, it is
     * efficient to compress them as a CompressedOtid that stores the fromIndex and
     * the toIndex. These fromIndex and toIndex reference the larger vector formed by
     * concatenating the correspondingly ordered rowIds.
     */
    private class CompressedOtid implements Comparable<CompressedOtid> {
      long originalTransactionId;
      int fromIndex; // inclusive
      int toIndex; // exclusive

      public CompressedOtid(long otid, int fromIndex, int toIndex) {
        this.originalTransactionId = otid;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
      }

      @Override
      public int compareTo(CompressedOtid other) {
        // When comparing the CompressedOtid, the one with the lesser value is smaller.
        if (originalTransactionId != other.originalTransactionId) {
          return originalTransactionId < other.originalTransactionId ? -1 : 1;
        }
        return 0;
      }
    }

    private TreeMap<DeleteRecordKey, DeleteReaderValue> sortMerger;
    private long rowIds[];
    private CompressedOtid compressedOtids[];
    private ValidTxnList validTxnList;

    public ColumnizedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
        Reader.Options readerOptions) throws IOException, DeleteEventsOverflowMemoryException {
      int bucket = AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf).getBucket();
      String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
      this.validTxnList = (txnString == null) ? new ValidReadTxnList() : new ValidReadTxnList(txnString);
      this.sortMerger = new TreeMap<DeleteRecordKey, DeleteReaderValue>();
      this.rowIds = null;
      this.compressedOtids = null;
      int maxEventsInMemory = HiveConf.getIntVar(conf, ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY);

      try {
        final Path[] deleteDeltaDirs = getDeleteDeltaDirsFromSplit(orcSplit);
        if (deleteDeltaDirs.length > 0) {
          int totalDeleteEventCount = 0;
          for (Path deleteDeltaDir : deleteDeltaDirs) {
            Path deleteDeltaFile = AcidUtils.createBucketFile(deleteDeltaDir, bucket);
            FileSystem fs = deleteDeltaFile.getFileSystem(conf);
            // NOTE: Calling last flush length below is more for future-proofing when we have
            // streaming deletes. But currently we don't support streaming deletes, and this can
            // be removed if this becomes a performance issue.
            long length = OrcAcidUtils.getLastFlushLength(fs, deleteDeltaFile);
            // NOTE: A check for existence of deleteDeltaFile is required because we may not have
            // deletes for the bucket being taken into consideration for this split processing.
            if (length != -1 && fs.exists(deleteDeltaFile)) {
              Reader deleteDeltaReader = OrcFile.createReader(deleteDeltaFile,
                  OrcFile.readerOptions(conf).maxLength(length));
              AcidStats acidStats = OrcAcidUtils.parseAcidStats(deleteDeltaReader);
              if (acidStats.deletes == 0) {
                continue; // just a safe check to ensure that we are not reading empty delete files.
              }
              totalDeleteEventCount += acidStats.deletes;
              if (totalDeleteEventCount > maxEventsInMemory) {
                // ColumnizedDeleteEventRegistry loads all the delete events from all the delete deltas
                // into memory. To prevent out-of-memory errors, this check is a rough heuristic that
                // prevents creation of an object of this class if the total number of delete events
                // exceed this value. By default, it has been set to 10 million delete events per bucket.
                LOG.info("Total number of delete events exceeds the maximum number of delete events "
                    + "that can be loaded into memory for the delete deltas in the directory at : "
                    + deleteDeltaDirs.toString() +". The max limit is currently set at "
                    + maxEventsInMemory + " and can be changed by setting the Hive config variable "
                    + ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname);
                throw new DeleteEventsOverflowMemoryException();
              }
              DeleteReaderValue deleteReaderValue = new DeleteReaderValue(deleteDeltaReader,
                  readerOptions, bucket, validTxnList);
              DeleteRecordKey deleteRecordKey = new DeleteRecordKey();
              if (deleteReaderValue.next(deleteRecordKey)) {
                sortMerger.put(deleteRecordKey, deleteReaderValue);
              } else {
                deleteReaderValue.close();
              }
            }
          }
          if (totalDeleteEventCount > 0) {
            // Initialize the rowId array when we have some delete events.
            rowIds = new long[totalDeleteEventCount];
            readAllDeleteEventsFromDeleteDeltas();
          }
        }
      } catch(IOException|DeleteEventsOverflowMemoryException e) {
        close(); // close any open readers, if there was some exception during initialization.
        throw e; // rethrow the exception so that the caller can handle.
      }
    }

    private void readAllDeleteEventsFromDeleteDeltas() throws IOException {
      if (sortMerger == null || sortMerger.isEmpty()) return; // trivial case, nothing to read.
      int distinctOtids = 0;
      long lastSeenOtid = -1;
      long otids[] = new long[rowIds.length];
      int index = 0;
      while (!sortMerger.isEmpty()) {
        // The sortMerger is a heap data structure that stores a pair of
        // (deleteRecordKey, deleteReaderValue) at each node and is ordered by deleteRecordKey.
        // The deleteReaderValue is the actual wrapper class that has the reference to the
        // underlying delta file that is being read, and its corresponding deleteRecordKey
        // is the smallest record id for that file. In each iteration of this loop, we extract(poll)
        // the minimum deleteRecordKey pair. Once we have processed that deleteRecordKey, we
        // advance the pointer for the corresponding deleteReaderValue. If the underlying file
        // itself has no more records, then we remove that pair from the heap, or else we
        // add the updated pair back to the heap.
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        DeleteRecordKey deleteRecordKey = entry.getKey();
        DeleteReaderValue deleteReaderValue = entry.getValue();
        otids[index] = deleteRecordKey.originalTransactionId;
        rowIds[index] = deleteRecordKey.rowId;
        ++index;
        if (lastSeenOtid != deleteRecordKey.originalTransactionId) {
          ++distinctOtids;
          lastSeenOtid = deleteRecordKey.originalTransactionId;
        }
        if (deleteReaderValue.next(deleteRecordKey)) {
          sortMerger.put(deleteRecordKey, deleteReaderValue);
        } else {
          deleteReaderValue.close(); // Exhausted reading all records, close the reader.
        }
      }

      // Once we have processed all the delete events and seen all the distinct otids,
      // we compress the otids into CompressedOtid data structure that records
      // the fromIndex(inclusive) and toIndex(exclusive) for each unique otid.
      this.compressedOtids = new CompressedOtid[distinctOtids];
      lastSeenOtid = otids[0];
      int fromIndex = 0, pos = 0;
      for (int i = 1; i < otids.length; ++i) {
        if (otids[i] != lastSeenOtid) {
          compressedOtids[pos] = new CompressedOtid(lastSeenOtid, fromIndex, i);
          lastSeenOtid = otids[i];
          fromIndex = i;
          ++pos;
        }
      }
      // account for the last distinct otid
      compressedOtids[pos] = new CompressedOtid(lastSeenOtid, fromIndex, otids.length);
    }

    private boolean isDeleted(long otid, long rowId) {
      if (compressedOtids == null || rowIds == null) {
        return false;
      }
      // To find if a given (otid, rowId) pair is deleted or not, we perform
      // two binary searches at most. The first binary search is on the
      // compressed otids. If a match is found, only then we do the next
      // binary search in the larger rowId vector between the given toIndex & fromIndex.

      // Check if otid is outside the range of all otids present.
      if (otid < compressedOtids[0].originalTransactionId
          || otid > compressedOtids[compressedOtids.length - 1].originalTransactionId) {
        return false;
      }
      // Create a dummy key for searching the otid in the compressed otid ranges.
      CompressedOtid key = new CompressedOtid(otid, -1, -1);
      int pos = Arrays.binarySearch(compressedOtids, key);
      if (pos >= 0) {
        // Otid with the given value found! Searching now for rowId...
        key = compressedOtids[pos]; // Retrieve the actual CompressedOtid that matched.
        // Check if rowId is outside the range of all rowIds present for this otid.
        if (rowId < rowIds[key.fromIndex]
            || rowId > rowIds[key.toIndex - 1]) {
          return false;
        }
        if (Arrays.binarySearch(rowIds, key.fromIndex, key.toIndex, rowId) >= 0) {
          return true; // rowId also found!
        }
      }
      return false;
    }

    @Override
    public void findDeletedRecords(VectorizedRowBatch batch, BitSet selectedBitSet)
        throws IOException {
      if (rowIds == null || compressedOtids == null) {
        return;
      }
      // Iterate through the batch and for each (otid, rowid) in the batch
      // check if it is deleted or not.

      long[] originalTransactionVector =
          batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? null
              : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector;
      long repeatedOriginalTransaction = (originalTransactionVector != null) ? -1
          : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[0];

      long[] rowIdVector =
          ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector;

      for (int setBitIndex = selectedBitSet.nextSetBit(0);
          setBitIndex >= 0;
          setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
        long otid = originalTransactionVector != null ? originalTransactionVector[setBitIndex]
                                                    : repeatedOriginalTransaction ;
        long rowId = rowIdVector[setBitIndex];
        if (isDeleted(otid, rowId)) {
          selectedBitSet.clear(setBitIndex);
        }
     }
    }

    @Override
    public void close() throws IOException {
      // ColumnizedDeleteEventRegistry reads all the delete events into memory during initialization
      // and it closes the delete event readers after it. If an exception gets thrown during
      // initialization, we may have to close any readers that are still left open.
      while (!sortMerger.isEmpty()) {
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        entry.getValue().close(); // close the reader for this entry
      }
    }
  }

  static class DeleteEventsOverflowMemoryException extends Exception {
    private static final long serialVersionUID = 1L;
  }
}
