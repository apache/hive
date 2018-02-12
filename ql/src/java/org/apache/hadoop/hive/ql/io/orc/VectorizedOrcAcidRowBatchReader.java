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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
/**
 * A fast vectorized batch reader class for ACID. Insert events are read directly
 * from the base files/insert_only deltas in vectorized row batches. The deleted
 * rows can then be easily indicated via the 'selected' field of the vectorized row batch.
 * Refer HIVE-14233 for more details.
 */
public class VectorizedOrcAcidRowBatchReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable,VectorizedRowBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedOrcAcidRowBatchReader.class);

  private org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader;
  private final VectorizedRowBatchCtx rbCtx;
  private VectorizedRowBatch vectorizedRowBatchBase;
  private long offset;
  private long length;
  protected float progress = 0.0f;
  protected Object[] partitionValues;
  private boolean addPartitionCols = true;
  private final ValidTxnList validTxnList;
  private final DeleteEventRegistry deleteEventRegistry;
  /**
   * {@link RecordIdentifier}/{@link VirtualColumn#ROWID} information
   */
  private final StructColumnVector recordIdColumnVector;
  private final Reader.Options readerOptions;
  private final boolean isOriginal;
  /**
   * something further in the data pipeline wants {@link VirtualColumn#ROWID}
   */
  private final boolean rowIdProjected;
  /**
   * partition/table root
   */
  private final Path rootPath;
  /**
   * for reading "original" files
   */
  private final OffsetAndBucketProperty syntheticProps;
  /**
   * To have access to {@link RecordReader#getRowNumber()} in the underlying file
   */
  private RecordReader innerReader;

  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
                                  Reporter reporter) throws IOException {
    this(inputSplit, conf,reporter, null);
  }
  @VisibleForTesting
  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
        Reporter reporter, VectorizedRowBatchCtx rbCtx) throws IOException {
    this(conf, inputSplit, reporter, rbCtx == null ? Utilities.getVectorizedRowBatchCtx(conf) : rbCtx);

    final Reader reader = OrcInputFormat.createOrcReaderForSplit(conf, (OrcSplit) inputSplit);
    // Careful with the range here now, we do not want to read the whole base file like deltas.
    innerReader = reader.rowsOptions(readerOptions.range(offset, length));
    baseReader = new org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>() {

      @Override
      public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
        return innerReader.nextBatch(value);
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
        return 0;
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return innerReader.getProgress();
      }
    };
    this.vectorizedRowBatchBase = ((RecordReaderImpl) innerReader).createRowBatch();
  }
  /**
   * LLAP IO c'tor
   */
  public VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf, Reporter reporter,
      org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader,
      VectorizedRowBatchCtx rbCtx) throws IOException {
    this(conf, inputSplit, reporter, rbCtx);
    this.baseReader = baseReader;
    this.innerReader = null;
    this.vectorizedRowBatchBase = baseReader.createValue();
  }

  private VectorizedOrcAcidRowBatchReader(JobConf conf, OrcSplit orcSplit, Reporter reporter,
      VectorizedRowBatchCtx rowBatchCtx) throws IOException {
    this.rbCtx = rowBatchCtx;
    final boolean isAcidRead = AcidUtils.isFullAcidScan(conf);
    final AcidUtils.AcidOperationalProperties acidOperationalProperties
            = AcidUtils.getAcidOperationalProperties(conf);

    // This type of VectorizedOrcAcidRowBatchReader can only be created when split-update is
    // enabled for an ACID case and the file format is ORC.
    boolean isReadNotAllowed = !isAcidRead || !acidOperationalProperties.isSplitUpdate();
    if (isReadNotAllowed) {
      OrcInputFormat.raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }

    reporter.setStatus(orcSplit.toString());
    readerOptions = OrcRawRecordMerger.createEventOptions(OrcInputFormat.createOptionsForReader(conf));

    this.offset = orcSplit.getStart();
    this.length = orcSplit.getLength();

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
    DeleteEventRegistry der;
    try {
      // See if we can load all the delete events from all the delete deltas in memory...
      der = new ColumnizedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions);
    } catch (DeleteEventsOverflowMemoryException e) {
      // If not, then create a set of hanging readers that do sort-merge to find the next smallest
      // delete event on-demand. Caps the memory consumption to (some_const * no. of readers).
      der = new SortMergedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions);
    }
    this.deleteEventRegistry = der;
    isOriginal = orcSplit.isOriginal();
    if(isOriginal) {
      recordIdColumnVector = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector(), new LongColumnVector(), new LongColumnVector());
    }
    else {
      //will swap in the Vectors from underlying row batch
      recordIdColumnVector = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, null, null, null);
    }
    rowIdProjected = areRowIdsProjected(rbCtx);
    rootPath = orcSplit.getRootDir();
    syntheticProps = computeOffsetAndBucket(orcSplit, conf, validTxnList);
  }

  /**
   * Used for generating synthetic ROW__IDs for reading "original" files
   */
  private static final class OffsetAndBucketProperty {
    private final long rowIdOffset;
    private final int bucketProperty;
    private final long syntheticTxnId;
    private OffsetAndBucketProperty(long rowIdOffset, int bucketProperty, long syntheticTxnId) {
      this.rowIdOffset = rowIdOffset;
      this.bucketProperty = bucketProperty;
      this.syntheticTxnId = syntheticTxnId;
    }
  }
  /**
   * See {@link #next(NullWritable, VectorizedRowBatch)} fist and
   * {@link OrcRawRecordMerger.OriginalReaderPair}.
   * When reading a split of an "original" file and we need to decorate data with ROW__ID.
   * This requires treating multiple files that are part of the same bucket (tranche for unbucketed
   * tables) as a single logical file to number rowids consistently.
   *
   * todo: This logic is executed per split of every "original" file.  The computed result is the
   * same for every split form the same file so this could be optimized by moving it to
   * before/during split computation and passing the info in the split.  (HIVE-17917)
   */
  private OffsetAndBucketProperty computeOffsetAndBucket(
    OrcSplit split, JobConf conf,ValidTxnList validTxnList) throws IOException {
    if(!needSyntheticRowIds(split.isOriginal(), !deleteEventRegistry.isEmpty(), rowIdProjected)) {
      if(split.isOriginal()) {
        /**
         * Even if we don't need to project ROW_IDs, we still need to check the transaction ID that
         * created the file to see if it's committed.  See more in
         * {@link #next(NullWritable, VectorizedRowBatch)}.  (In practice getAcidState() should
         * filter out base/delta files but this makes fewer dependencies)
         */
        OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
          OrcRawRecordMerger.TransactionMetaData.findTransactionIDForSynthetcRowIDs(split.getPath(),
            split.getRootDir(), conf);
        return new OffsetAndBucketProperty(-1,-1,
          syntheticTxnInfo.syntheticTransactionId);
      }
      return null;
    }
    long rowIdOffset = 0;
    OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
      OrcRawRecordMerger.TransactionMetaData.findTransactionIDForSynthetcRowIDs(split.getPath(),
        split.getRootDir(), conf);
    int bucketId = AcidUtils.parseBaseOrDeltaBucketFilename(split.getPath(), conf).getBucketId();
    int bucketProperty = BucketCodec.V1.encode(new AcidOutputFormat.Options(conf)
      .statementId(syntheticTxnInfo.statementId).bucket(bucketId));
    AcidUtils.Directory directoryState = AcidUtils.getAcidState( syntheticTxnInfo.folder, conf,
      validTxnList, false, true);
    for (HadoopShims.HdfsFileStatusWithId f : directoryState.getOriginalFiles()) {
      AcidOutputFormat.Options bucketOptions =
        AcidUtils.parseBaseOrDeltaBucketFilename(f.getFileStatus().getPath(), conf);
      if (bucketOptions.getBucketId() != bucketId) {
        continue;//HIVE-16952
      }
      if (f.getFileStatus().getPath().equals(split.getPath())) {
        //'f' is the file whence this split is
        break;
      }
      Reader reader = OrcFile.createReader(f.getFileStatus().getPath(),
        OrcFile.readerOptions(conf));
      rowIdOffset += reader.getNumberOfRows();
    }
    return new OffsetAndBucketProperty(rowIdOffset, bucketProperty,
      syntheticTxnInfo.syntheticTransactionId);
  }
  /**
   * {@link VectorizedOrcAcidRowBatchReader} is always used for vectorized reads of acid tables.
   * In some cases this cannot be used from LLAP IO elevator because
   * {@link RecordReader#getRowNumber()} is not (currently) available there but is required to
   * generate ROW__IDs for "original" files
   * @param hasDeletes - if there are any deletes that apply to this split
   * todo: HIVE-17944
   */
  static boolean canUseLlapForAcid(OrcSplit split, boolean hasDeletes, Configuration conf) {
    if(!split.isOriginal()) {
      return true;
    }
    VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    if(rbCtx == null) {
      throw new IllegalStateException("Could not create VectorizedRowBatchCtx for " + split.getPath());
    }
    return !needSyntheticRowIds(split.isOriginal(), hasDeletes, areRowIdsProjected(rbCtx));
  }

  /**
   * Does this reader need to decorate rows with ROW__IDs (for "original" reads).
   * Even if ROW__ID is not projected you still need to decorate the rows with them to see if
   * any of the delete events apply.
   */
  private static boolean needSyntheticRowIds(boolean isOriginal, boolean hasDeletes, boolean rowIdProjected) {
    return isOriginal && (hasDeletes || rowIdProjected);
  }
  private static boolean areRowIdsProjected(VectorizedRowBatchCtx rbCtx) {
    if(rbCtx.getVirtualColumnCount() == 0) {
      return false;
    }
    for(VirtualColumn vc : rbCtx.getNeededVirtualColumns()) {
      if(vc == VirtualColumn.ROWID) {
        //The query needs ROW__ID: maybe explicitly asked, maybe it's part of
        // Update/Delete statement.
        //Either way, we need to decorate "original" rows with row__id
        return true;
      }
    }
    return false;
  }
  static Path[] getDeleteDeltaDirsFromSplit(OrcSplit orcSplit) throws IOException {
    Path path = orcSplit.getPath();
    Path root;
    if (orcSplit.hasBase()) {
      if (orcSplit.isOriginal()) {
        root = orcSplit.getRootDir();
      } else {
        root = path.getParent().getParent();//todo: why not just use getRootDir()?
        assert root.equals(orcSplit.getRootDir()) : "root mismatch: baseDir=" + orcSplit.getRootDir() +
          " path.p.p=" + root;
      }
    } else {
      throw new IllegalStateException("Split w/o base w/Acid 2.0??: " + path);
    }
    return AcidUtils.deserializeDeleteDeltas(root, orcSplit.getDeltas());
  }

  /**
   * There are 2 types of schema from the {@link #baseReader} that this handles.  In the case
   * the data was written to a transactional table from the start, every row is decorated with
   * transaction related info and looks like <op, otid, writerId, rowid, ctid, <f1, ... fn>>.
   *
   * The other case is when data was written to non-transactional table and thus only has the user
   * data: <f1, ... fn>.  Then this table was then converted to a transactional table but the data
   * files are not changed until major compaction.  These are the "original" files.
   *
   * In this case we may need to decorate the outgoing data with transactional column values at
   * read time.  (It's done somewhat out of band via VectorizedRowBatchCtx - ask Teddy Choi).
   * The "otid, writerId, rowid" columns represent {@link RecordIdentifier}.  They are assigned
   * each time the table is read in a way that needs to project {@link VirtualColumn#ROWID}.
   * Major compaction will attach these values to each row permanently.
   * It's critical that these generated column values are assigned exactly the same way by each
   * read of the same row and by the Compactor.
   * See {@link org.apache.hadoop.hive.ql.txn.compactor.CompactorMR} and
   * {@link OrcRawRecordMerger.OriginalReaderPairToCompact} for the Compactor read path.
   * (Longer term should make compactor use this class)
   *
   * This only decorates original rows with metadata if something above is requesting these values
   * or if there are Delete events to apply.
   *
   * @return false where there is no more data, i.e. {@code value} is empty
   */
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
      if (!baseReader.next(null, vectorizedRowBatchBase)) {
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
    ColumnVector[] innerRecordIdColumnVector = vectorizedRowBatchBase.cols;
    if(isOriginal) {
      /*
       * If there are deletes and reading original file, we must produce synthetic ROW_IDs in order
       * to see if any deletes apply
       */
      boolean needSyntheticRowId =
          needSyntheticRowIds(true, !deleteEventRegistry.isEmpty(), rowIdProjected);
      if(needSyntheticRowId) {
        assert syntheticProps != null && syntheticProps.rowIdOffset >= 0 : "" + syntheticProps;
        assert syntheticProps != null && syntheticProps.bucketProperty >= 0 : "" + syntheticProps;
        if(innerReader == null) {
          throw new IllegalStateException(getClass().getName() + " requires " +
            org.apache.orc.RecordReader.class +
            " to handle original files that require ROW__IDs: " + rootPath);
        }
        /**
         * {@link RecordIdentifier#getTransactionId()}
         */
        recordIdColumnVector.fields[0].noNulls = true;
        recordIdColumnVector.fields[0].isRepeating = true;
        ((LongColumnVector)recordIdColumnVector.fields[0]).vector[0] = syntheticProps.syntheticTxnId;
        /**
         * This is {@link RecordIdentifier#getBucketProperty()}
         * Also see {@link BucketCodec}
         */
        recordIdColumnVector.fields[1].noNulls = true;
        recordIdColumnVector.fields[1].isRepeating = true;
        ((LongColumnVector)recordIdColumnVector.fields[1]).vector[0] = syntheticProps.bucketProperty;
        /**
         * {@link RecordIdentifier#getRowId()}
         */
        recordIdColumnVector.fields[2].noNulls = true;
        recordIdColumnVector.fields[2].isRepeating = false;
        long[] rowIdVector = ((LongColumnVector)recordIdColumnVector.fields[2]).vector;
        for(int i = 0; i < vectorizedRowBatchBase.size; i++) {
          //baseReader.getRowNumber() seems to point at the start of the batch todo: validate
          rowIdVector[i] = syntheticProps.rowIdOffset + innerReader.getRowNumber() + i;
        }
        //Now populate a structure to use to apply delete events
        innerRecordIdColumnVector = new ColumnVector[OrcRecordUpdater.FIELDS];
        innerRecordIdColumnVector[OrcRecordUpdater.ORIGINAL_TRANSACTION] = recordIdColumnVector.fields[0];
        innerRecordIdColumnVector[OrcRecordUpdater.BUCKET] = recordIdColumnVector.fields[1];
        innerRecordIdColumnVector[OrcRecordUpdater.ROW_ID] = recordIdColumnVector.fields[2];
        //these are insert events so (original txn == current) txn for all rows
        innerRecordIdColumnVector[OrcRecordUpdater.CURRENT_TRANSACTION] = recordIdColumnVector.fields[0];
      }
      if(syntheticProps.syntheticTxnId > 0) {
        //"originals" (written before table was converted to acid) is considered written by
        // txnid:0 which is always committed so there is no need to check wrt invalid transactions
        //But originals written by Load Data for example can be in base_x or delta_x_x so we must
        //check if 'x' is committed or not evn if ROW_ID is not needed in the Operator pipeline.
        if (needSyntheticRowId) {
          findRecordsWithInvalidTransactionIds(innerRecordIdColumnVector,
              vectorizedRowBatchBase.size, selectedBitSet);
        } else {
          /*since ROW_IDs are not needed we didn't create the ColumnVectors to hold them but we
          * still have to check if the data being read is committed as far as current
          * reader (transactions) is concerned.  Since here we are reading 'original' schema file,
          * all rows in it have been created by the same txn, namely 'syntheticProps.syntheticTxnId'
          */
          if (!validTxnList.isTxnValid(syntheticProps.syntheticTxnId)) {
            selectedBitSet.clear(0, vectorizedRowBatchBase.size);
          }
        }
      }
    }
    else {
      // Case 1- find rows which belong to transactions that are not valid.
      findRecordsWithInvalidTransactionIds(vectorizedRowBatchBase, selectedBitSet);
    }

    // Case 2- find rows which have been deleted.
    this.deleteEventRegistry.findDeletedRecords(innerRecordIdColumnVector,
        vectorizedRowBatchBase.size, selectedBitSet);

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

    if(isOriginal) {
     /*Just copy the payload.  {@link recordIdColumnVector} has already been populated*/
      System.arraycopy(vectorizedRowBatchBase.cols, 0, value.cols, 0,
        value.getDataColumnCount());
    }
    else {
      // Finally, link up the columnVector from the base VectorizedRowBatch to outgoing batch.
      StructColumnVector payloadStruct = (StructColumnVector) vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW];
      // Transfer columnVector objects from base batch to outgoing batch.
      System.arraycopy(payloadStruct.fields, 0, value.cols, 0, value.getDataColumnCount());
      if(rowIdProjected) {
        recordIdColumnVector.fields[0] = vectorizedRowBatchBase.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION];
        recordIdColumnVector.fields[1] = vectorizedRowBatchBase.cols[OrcRecordUpdater.BUCKET];
        recordIdColumnVector.fields[2] = vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW_ID];
      }
    }
    if(rowIdProjected) {
      rbCtx.setRecordIdColumnVector(recordIdColumnVector);
    }
    progress = baseReader.getProgress();
    return true;
  }

  private void findRecordsWithInvalidTransactionIds(VectorizedRowBatch batch, BitSet selectedBitSet) {
    findRecordsWithInvalidTransactionIds(batch.cols, batch.size, selectedBitSet);
  }

  private void findRecordsWithInvalidTransactionIds(ColumnVector[] cols, int size, BitSet selectedBitSet) {
    if (cols[OrcRecordUpdater.CURRENT_TRANSACTION].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid transaction.
      long currentTransactionIdForBatch = ((LongColumnVector)
          cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector[0];
      if (!validTxnList.isTxnValid(currentTransactionIdForBatch)) {
        selectedBitSet.clear(0, size);
      }
      return;
    }
    long[] currentTransactionVector =
        ((LongColumnVector) cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector;
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
  protected static interface DeleteEventRegistry {
    /**
     * Modifies the passed bitset to indicate which of the rows in the batch
     * have been deleted. Assumes that the batch.size is equal to bitset size.
     * @param cols
     * @param size
     * @param selectedBitSet
     * @throws IOException
     */
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet) throws IOException;

    /**
     * The close() method can be called externally to signal the implementing classes
     * to free up resources.
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * @return {@code true} if no delete events were found
     */
    boolean isEmpty();
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
    private Boolean isDeleteRecordAvailable = null;
    private ValidTxnList validTxnList;

    SortMergedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit, Reader.Options readerOptions)
      throws IOException {
        final Path[] deleteDeltas = getDeleteDeltaDirsFromSplit(orcSplit);
        if (deleteDeltas.length > 0) {
          int bucket = AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf).getBucketId();
          String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
          this.validTxnList = (txnString == null) ? new ValidReadTxnList() : new ValidReadTxnList(txnString);
          OrcRawRecordMerger.Options mergerOptions = new OrcRawRecordMerger.Options().isDeleteReader(true);
          assert !orcSplit.isOriginal() : "If this now supports Original splits, set up mergeOptions properly";
          this.deleteRecords = new OrcRawRecordMerger(conf, true, null, false, bucket,
                                                      validTxnList, readerOptions, deleteDeltas,
                                                      mergerOptions);
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
    public boolean isEmpty() {
      if(isDeleteRecordAvailable == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return !isDeleteRecordAvailable;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet)
        throws IOException {
      if (!isDeleteRecordAvailable) {
        return;
      }

      long[] originalTransaction =
          cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector;
      long[] bucket =
          cols[OrcRecordUpdater.BUCKET].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      long[] rowId =
          cols[OrcRecordUpdater.ROW_ID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      // The following repeatedX values will be set, if any of the columns are repeating.
      long repeatedOriginalTransaction = (originalTransaction != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[0];
      long repeatedBucket = (bucket != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];
      long repeatedRowId = (rowId != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector[0];


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
      int lastValidIndex = selectedBitSet.previousSetBit(size - 1);
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
   * into 3 vectors- one for original transaction id (otid), one for bucket property and one for
   * row id.  See {@link BucketCodec} for more about bucket property.
   * The otids are likely to be repeated very often, as a single transaction
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
     * A simple wrapper class to hold the (otid, bucketProperty, rowId) pair.
     */
    static class DeleteRecordKey implements Comparable<DeleteRecordKey> {
      private long originalTransactionId;
      /**
       * see {@link BucketCodec}
       */
      private int bucketProperty; 
      private long rowId;
      DeleteRecordKey() {
        this.originalTransactionId = -1;
        this.rowId = -1;
      }
      public void set(long otid, int bucketProperty, long rowId) {
        this.originalTransactionId = otid;
        this.bucketProperty = bucketProperty;
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
        if(bucketProperty != other.bucketProperty) {
          return bucketProperty < other.bucketProperty ? -1 : 1;
        }
        if (rowId != other.rowId) {
          return rowId < other.rowId ? -1 : 1;
        }
        return 0;
      }
      @Override
      public String toString() {
        return "otid: " + originalTransactionId + " bucketP:" + bucketProperty + " rowid: " + rowId;
      }
    }

    /**
     * This class actually reads the delete delta files in vectorized row batches.
     * For every call to next(), it returns the next smallest record id in the file if available.
     * Internally, the next() buffers a row batch and maintains an index pointer, reading the
     * next batch when the previous batch is exhausted.
     *
     * For unbucketed tables this will currently return all delete events.  Once we trust that
     * the N in bucketN for "base" spit is reliable, all delete events not matching N can be skipped.
     */
    static class DeleteReaderValue {
      private VectorizedRowBatch batch;
      private final RecordReader recordReader;
      private int indexPtrInBatch;
      private final int bucketForSplit; // The bucket value should be same for all the records.
      private final ValidTxnList validTxnList;
      private boolean isBucketPropertyRepeating;
      private final boolean isBucketedTable;

      DeleteReaderValue(Reader deleteDeltaReader, Reader.Options readerOptions, int bucket,
          ValidTxnList validTxnList, boolean isBucketedTable) throws IOException {
        this.recordReader  = deleteDeltaReader.rowsOptions(readerOptions);
        this.bucketForSplit = bucket;
        this.batch = deleteDeltaReader.getSchema().createRowBatch();
        if (!recordReader.nextBatch(batch)) { // Read the first batch.
          this.batch = null; // Oh! the first batch itself was null. Close the reader.
        }
        this.indexPtrInBatch = 0;
        this.validTxnList = validTxnList;
        this.isBucketedTable = isBucketedTable;
        checkBucketId();//check 1st batch
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
              checkBucketId();
              indexPtrInBatch = 0; // After reading the batch, reset the pointer to beginning.
            } else {
              return false; // no more batches to read, exhausted the reader.
            }
          }
          long currentTransaction = setCurrentDeleteKey(deleteRecordKey);
          if(!isBucketPropertyRepeating) {
            checkBucketId(deleteRecordKey.bucketProperty);
          }
          ++indexPtrInBatch;
          if (validTxnList.isTxnValid(currentTransaction)) {
            isValidNext = true;
          }
        }
        return true;
      }

      public void close() throws IOException {
        this.recordReader.close();
      }
      private long setCurrentDeleteKey(DeleteRecordKey deleteRecordKey) {
        int originalTransactionIndex =
          batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? 0 : indexPtrInBatch;
        long originalTransaction =
          ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[originalTransactionIndex];
        int bucketPropertyIndex =
          batch.cols[OrcRecordUpdater.BUCKET].isRepeating ? 0 : indexPtrInBatch;
        int bucketProperty = (int)((LongColumnVector)batch.cols[OrcRecordUpdater.BUCKET]).vector[bucketPropertyIndex];
        long rowId = ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[indexPtrInBatch];
        int currentTransactionIndex =
          batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION].isRepeating ? 0 : indexPtrInBatch;
        long currentTransaction =
          ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector[currentTransactionIndex];
        deleteRecordKey.set(originalTransaction, bucketProperty, rowId);
        return currentTransaction;
      }
      private void checkBucketId() throws IOException {
        isBucketPropertyRepeating = batch.cols[OrcRecordUpdater.BUCKET].isRepeating;
        if(isBucketPropertyRepeating) {
          int bucketPropertyFromRecord = (int)((LongColumnVector)
            batch.cols[OrcRecordUpdater.BUCKET]).vector[0];
          checkBucketId(bucketPropertyFromRecord);
        }
      }
      /**
       * Whenever we are reading a batch, we must ensure that all the records in the batch
       * have the same bucket id as the bucket id of the split. If not, throw exception.
       * NOTE: this assertion might not hold, once virtual bucketing is in place. However,
       * it should be simple to fix that case. Just replace check for bucket equality with
       * a check for valid bucket mapping. Until virtual bucketing is added, it means
       * either the split computation got messed up or we found some corrupted records.
       */
      private void checkBucketId(int bucketPropertyFromRecord) throws IOException {
        if(!isBucketedTable) {
          /**
           * in this case a file inside a delete_delta_x_y/bucketN may contain any value for
           * bucketId in {@link RecordIdentifier#getBucketProperty()}
           */
          return;
        }
        int bucketIdFromRecord = BucketCodec.determineVersion(bucketPropertyFromRecord)
          .decodeWriterId(bucketPropertyFromRecord);
        if(bucketIdFromRecord != bucketForSplit) {
          DeleteRecordKey dummy = new DeleteRecordKey();
          long curTxnId = setCurrentDeleteKey(dummy);
          throw new IOException("Corrupted records with different bucket ids "
            + "from the containing bucket file found! Expected bucket id "
            + bucketForSplit + ", however found the bucket id " + bucketIdFromRecord +
            " from " + dummy + " curTxnId: " + curTxnId);
        }
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
    private final class CompressedOtid implements Comparable<CompressedOtid> {
      final long originalTransactionId;
      final int bucketProperty;
      final int fromIndex; // inclusive
      final int toIndex; // exclusive

      CompressedOtid(long otid, int bucketProperty, int fromIndex, int toIndex) {
        this.originalTransactionId = otid;
        this.bucketProperty = bucketProperty;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
      }

      @Override
      public int compareTo(CompressedOtid other) {
        // When comparing the CompressedOtid, the one with the lesser value is smaller.
        if (originalTransactionId != other.originalTransactionId) {
          return originalTransactionId < other.originalTransactionId ? -1 : 1;
        }
        if(bucketProperty != other.bucketProperty) {
          return bucketProperty < other.bucketProperty ? -1 : 1;
        }
        return 0;
      }
    }

    /**
     * Food for thought:
     * this is a bit problematic - in order to load ColumnizedDeleteEventRegistry we still open
     * all delete deltas at once - possibly causing OOM same as for {@link SortMergedDeleteEventRegistry}
     * which uses {@link OrcRawRecordMerger}.  Why not load all delete_delta sequentially.  Each
     * dd is sorted by {@link RecordIdentifier} so we could create a BTree like structure where the
     * 1st level is an array of originalTransactionId where each entry points at an array
     * of bucketIds where each entry points at an array of rowIds.  We could probably use ArrayList
     * to manage insertion as the structure is built (LinkedList?).  This should reduce memory
     * footprint (as far as OrcReader to a single reader) - probably bad for LLAP IO
     */
    private TreeMap<DeleteRecordKey, DeleteReaderValue> sortMerger;
    private long rowIds[];
    private CompressedOtid compressedOtids[];
    private ValidTxnList validTxnList;
    private Boolean isEmpty = null;

    ColumnizedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
        Reader.Options readerOptions) throws IOException, DeleteEventsOverflowMemoryException {
      int bucket = AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf).getBucketId();
      String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
      this.validTxnList = (txnString == null) ? new ValidReadTxnList() : new ValidReadTxnList(txnString);
      this.sortMerger = new TreeMap<DeleteRecordKey, DeleteReaderValue>();
      this.rowIds = null;
      this.compressedOtids = null;
      int maxEventsInMemory = HiveConf.getIntVar(conf, ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY);
      final boolean isBucketedTable  = conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0) > 0;

      try {
        final Path[] deleteDeltaDirs = getDeleteDeltaDirsFromSplit(orcSplit);
        if (deleteDeltaDirs.length > 0) {
          int totalDeleteEventCount = 0;
          for (Path deleteDeltaDir : deleteDeltaDirs) {
            FileSystem fs = deleteDeltaDir.getFileSystem(conf);
            for(Path deleteDeltaFile : OrcRawRecordMerger.getDeltaFiles(deleteDeltaDir, bucket, conf,
              new OrcRawRecordMerger.Options().isCompacting(false), isBucketedTable)) {
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
                  readerOptions, bucket, validTxnList, isBucketedTable);
              DeleteRecordKey deleteRecordKey = new DeleteRecordKey();
              if (deleteReaderValue.next(deleteRecordKey)) {
                sortMerger.put(deleteRecordKey, deleteReaderValue);
              } else {
                deleteReaderValue.close();
              }
            }
          }
          }
          if (totalDeleteEventCount > 0) {
            // Initialize the rowId array when we have some delete events.
            rowIds = new long[totalDeleteEventCount];
            readAllDeleteEventsFromDeleteDeltas();
          }
        }
        isEmpty = compressedOtids == null || rowIds == null;
      } catch(IOException|DeleteEventsOverflowMemoryException e) {
        close(); // close any open readers, if there was some exception during initialization.
        throw e; // rethrow the exception so that the caller can handle.
      }
    }

    /**
     * This is not done quite right.  The intent of {@link CompressedOtid} is a hedge against
     * "delete from T" that generates a huge number of delete events possibly even 2G - max array
     * size.  (assuming no one txn inserts > 2G rows (in a bucket)).  As implemented, the algorithm
     * first loads all data into one array otid[] and rowIds[] which defeats the purpose.
     * In practice we should be filtering delete evens by min/max ROW_ID from the split.  The later
     * is also not yet implemented: HIVE-16812.
     */
    private void readAllDeleteEventsFromDeleteDeltas() throws IOException {
      if (sortMerger == null || sortMerger.isEmpty()) return; // trivial case, nothing to read.
      int distinctOtids = 0;
      long lastSeenOtid = -1;
      int lastSeenBucketProperty = -1;
      long otids[] = new long[rowIds.length];
      int[] bucketProperties = new int [rowIds.length];
      
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
        bucketProperties[index] = deleteRecordKey.bucketProperty;
        rowIds[index] = deleteRecordKey.rowId;
        ++index;
        if (lastSeenOtid != deleteRecordKey.originalTransactionId ||
          lastSeenBucketProperty != deleteRecordKey.bucketProperty) {
          ++distinctOtids;
          lastSeenOtid = deleteRecordKey.originalTransactionId;
          lastSeenBucketProperty = deleteRecordKey.bucketProperty;
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
      lastSeenBucketProperty = bucketProperties[0];
      int fromIndex = 0, pos = 0;
      for (int i = 1; i < otids.length; ++i) {
        if (otids[i] != lastSeenOtid || lastSeenBucketProperty != bucketProperties[i]) {
          compressedOtids[pos] = 
            new CompressedOtid(lastSeenOtid, lastSeenBucketProperty, fromIndex, i);
          lastSeenOtid = otids[i];
          lastSeenBucketProperty = bucketProperties[i];
          fromIndex = i;
          ++pos;
        }
      }
      // account for the last distinct otid
      compressedOtids[pos] =
        new CompressedOtid(lastSeenOtid, lastSeenBucketProperty, fromIndex, otids.length);
    }

    private boolean isDeleted(long otid, int bucketProperty, long rowId) {
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
      // Create a dummy key for searching the otid/bucket in the compressed otid ranges.
      CompressedOtid key = new CompressedOtid(otid, bucketProperty, -1, -1);
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
    public boolean isEmpty() {
      if(isEmpty == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return isEmpty;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet)
        throws IOException {
      if (rowIds == null || compressedOtids == null) {
        return;
      }
      // Iterate through the batch and for each (otid, rowid) in the batch
      // check if it is deleted or not.

      long[] originalTransactionVector =
          cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector;
      long repeatedOriginalTransaction = (originalTransactionVector != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[0];

      long[] bucketProperties =
        cols[OrcRecordUpdater.BUCKET].isRepeating ? null
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      int repeatedBucketProperty = (bucketProperties != null) ? -1
        : (int)((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];

      long[] rowIdVector =
          ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      for (int setBitIndex = selectedBitSet.nextSetBit(0);
          setBitIndex >= 0;
          setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
        long otid = originalTransactionVector != null ? originalTransactionVector[setBitIndex]
                                                    : repeatedOriginalTransaction ;
        int bucketProperty = bucketProperties != null ? (int)bucketProperties[setBitIndex]
          : repeatedBucketProperty;
        long rowId = rowIdVector[setBitIndex];
        if (isDeleted(otid, bucketProperty, rowId)) {
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
