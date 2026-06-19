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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.orc.OrcUtils;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.common.util.Ref;

import com.google.common.annotations.VisibleForTesting;

/**
 * Merges a base and a list of delta files together into a single stream of
 * events.
 */
public class OrcRawRecordMerger implements AcidInputFormat.RawReader<OrcStruct>{

  private static final Logger LOG = LoggerFactory.getLogger(OrcRawRecordMerger.class);

  private final boolean collapse;
  private final RecordReader baseReader;
  private final ObjectInspector objectInspector;
  private final long offset;
  private final long length;
  private final ValidWriteIdList validWriteIdList;
  private final int columns;
  protected final ReaderKey prevKey = new ReaderKey();
  // this is the key less than the lowest key we need to process
  private final RecordIdentifier minKey;
  // this is the last key we need to process
  private final RecordIdentifier maxKey;
  // an extra value so that we can return it while reading ahead
  private OrcStruct extraValue;
  /**
   * A RecordIdentifier extended with the current write id. This is the
   * key of our merge sort with the originalWriteId, bucket, and rowId
   * ascending and the currentWriteId, statementId descending. This means that if the
   * reader is collapsing events to just the last update, just the first
   * instance of each record is required.
   */
  @VisibleForTesting
  public final static class ReaderKey extends RecordIdentifier{
    private long currentWriteId;
    private boolean isDeleteEvent = false;

    ReaderKey() {
      this(-1, -1, -1, -1);
    }

    public ReaderKey(long originalWriteId, int bucket, long rowId, long currentWriteId) {
      super(originalWriteId, bucket, rowId);
      this.currentWriteId = currentWriteId;
    }

    @Override
    public void set(RecordIdentifier other) {
      super.set(other);
      currentWriteId = ((ReaderKey) other).currentWriteId;
      isDeleteEvent = ((ReaderKey) other).isDeleteEvent;
    }

    public void setValues(long originalWriteId,
                          int bucket,
                          long rowId,
                          long currentWriteId,
                          boolean  isDelete) {
      setValues(originalWriteId, bucket, rowId);
      this.currentWriteId = currentWriteId;
      this.isDeleteEvent = isDelete;
    }

    public void setDeleteEvent(boolean deleteEvent) {
      isDeleteEvent = deleteEvent;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) &&
                 currentWriteId == ((ReaderKey) other).currentWriteId;
    }
    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int)(currentWriteId ^ (currentWriteId >>> 32));
      return result;
    }


    @Override
    public int compareTo(RecordIdentifier other) {
      int sup = compareToInternal(other);
      if (sup == 0) {
        if (other.getClass() == ReaderKey.class) {
          ReaderKey oth = (ReaderKey) other;
          if (currentWriteId != oth.currentWriteId) {
            return currentWriteId < oth.currentWriteId ? +1 : -1;
          }
          if(isDeleteEvent != oth.isDeleteEvent) {
            //this is to break a tie if insert + delete of a given row is done within the same
            //txn (so that currentWriteId is the same for both events) and we want the
            //delete event to sort 1st since it needs to be sent up so that
            // OrcInputFormat.getReader(InputSplit inputSplit, Options options) can skip it.
            return isDeleteEvent ? -1 : +1;
          }
        } else {
          return -1;
        }
      }
      return sup;
    }

    /**
     * This means 1 txn modified the same row more than once
     */
    private boolean isSameRow(ReaderKey other) {
      return compareRow(other) == 0 && currentWriteId == other.currentWriteId;
    }

    long getCurrentWriteId() {
      return currentWriteId;
    }

    /**
     * Compare rows without considering the currentWriteId.
     * @param other the value to compare to
     * @return -1, 0, +1
     */
    int compareRow(RecordIdentifier other) {
      return compareToInternal(other);
    }

    @Override
    public String toString() {
      return "{originalWriteId: " + getWriteId() + ", " +
          bucketToString(getBucketProperty()) + ", row: " + getRowId() +
          ", currentWriteId " + currentWriteId + "}";
    }

    public boolean isDeleteEvent() {
      return isDeleteEvent;
    }
  }
  interface ReaderPair {
    OrcStruct nextRecord();
    int getColumns();
    RecordReader getRecordReader();
    Reader getReader();
    RecordIdentifier getMinKey();
    RecordIdentifier getMaxKey();
    ReaderKey getKey();
    void next(OrcStruct next) throws IOException;
  }
  /**
   * Used when base_x/bucket_N is missing - makes control flow a bit easier
   */
  private class EmptyReaderPair implements ReaderPair {
    @Override public OrcStruct nextRecord() {
      return null;
    }
    @Override public int getColumns() {
      return 0;
    }
    @Override public RecordReader getRecordReader() {
      return null;
    }
    @Override public Reader getReader() {
      return null;
    }
    @Override public RecordIdentifier getMinKey() {
      return null;
    }
    @Override public RecordIdentifier getMaxKey() {
      return null;
    }
    @Override public ReaderKey getKey() {
      return null;
    }
    @Override public void next(OrcStruct next) throws IOException {
    }
  }
  /**
   * A reader and the next record from that reader. The code reads ahead so that
   * we can return the lowest ReaderKey from each of the readers. Thus, the
   * next available row is nextRecord and only following records are still in
   * the reader.
   */
  @VisibleForTesting
  final static class ReaderPairAcid implements ReaderPair {
    private OrcStruct nextRecord;
    private final Reader reader;
    private final RecordReader recordReader;
    private final ReaderKey key;
    private final RecordIdentifier minKey;
    private final RecordIdentifier maxKey;

    /**
     * Create a reader that reads from the first key larger than minKey to any
     * keys equal to maxKey.
     * @param key the key to read into
     * @param reader the ORC file reader
     * @param minKey only return keys larger than minKey if it is non-null
     * @param maxKey only return keys less than or equal to maxKey if it is
     *               non-null
     * @param options options to provide to read the rows.
     * @param conf
     * @throws IOException
     */
    @VisibleForTesting
    ReaderPairAcid(ReaderKey key, Reader reader,
      RecordIdentifier minKey, RecordIdentifier maxKey,
      ReaderImpl.Options options, final Configuration conf) throws IOException {
      this.reader = reader;
      this.key = key;
      // TODO use stripe statistics to jump over stripes
      recordReader = reader.rowsOptions(options, conf);
      this.minKey = minKey;
      this.maxKey = maxKey;
      // advance the reader until we reach the minimum key
      do {
        next(nextRecord());
      } while (nextRecord() != null &&
        (minKey != null && key.compareRow(getMinKey()) <= 0));
    }
    @Override
    public String toString() {
      return "[key=" + key + ", nextRecord=" + nextRecord + ", reader=" + reader + "]";
    }
    @Override public final OrcStruct nextRecord() {
      return nextRecord;
    }
    @Override
    public final int getColumns() {
      return getReader().getTypes().get(OrcRecordUpdater.ROW + 1).getSubtypesCount();
    }

    @Override public RecordReader getRecordReader() {
      return recordReader;
    }
    @Override public Reader getReader() { return reader; }
    @Override public RecordIdentifier getMinKey() {
      return minKey;
    }
    @Override public RecordIdentifier getMaxKey() {
      return maxKey;
    }
    @Override public ReaderKey getKey() {
      return key;
    }

    @Override
    public void next(OrcStruct next) throws IOException {
      if (getRecordReader().hasNext()) {
        nextRecord = (OrcStruct) getRecordReader().next(next);
        // set the key
        getKey().setValues(OrcRecordUpdater.getOriginalTransaction(nextRecord()),
            OrcRecordUpdater.getBucket(nextRecord()),
            OrcRecordUpdater.getRowId(nextRecord()),
            OrcRecordUpdater.getCurrentTransaction(nextRecord()),
            OrcRecordUpdater.getOperation(nextRecord()) == OrcRecordUpdater.DELETE_OPERATION);
        // if this record is larger than maxKey, we need to stop
        if (getMaxKey() != null && getKey().compareRow(getMaxKey()) > 0) {
          LOG.debug("key " + getKey() + " > maxkey " + getMaxKey());
          nextRecord = null;
          getRecordReader().close();
        }
      } else {
        nextRecord = null;
        getRecordReader().close();
      }
    }
  }

  /**
   * A reader that pretends an original base file is a new versioned base file.
   * It wraps the underlying reader's row with an ACID event object and
   * makes the relevant translations.
   * 
   * Running multiple Insert statements on the same partition (of non acid table) creates files
   * like so: 00000_0, 00000_0_copy1, 00000_0_copy2, etc.  So the OriginalReaderPair must treat all
   * of these files as part of a single logical bucket file.
   *
   * Also, for unbucketed (non acid) tables, there are no guarantees where data files may be placed.
   * For example, CTAS+Tez+Union creates subdirs
   * {@link AbstractFileMergeOperator#UNION_SUDBIR_PREFIX}_1/,
   * {@link AbstractFileMergeOperator#UNION_SUDBIR_PREFIX}_2/, etc for each leg of the Union.  Thus
   * the data file need not be an immediate child of partition dir.  All files for a given writerId
   * are treated as one logical unit to assign {@link RecordIdentifier}s to them consistently.
   * 
   * For Compaction, where each split includes the whole bucket, this means reading over all the
   * files in order to assign ROW__ID.rowid in one sequence for the entire logical bucket.
   * For unbucketed tables, a Compaction split is all files written by a given writerId.
   *
   * For a read after the table is marked transactional but before it's rewritten into a base/
   * by compaction, each of the original files may be split into many pieces.  For each split we
   * must make sure to include only the relevant part of each delta file.
   * {@link OrcRawRecordMerger#minKey} and {@link OrcRawRecordMerger#maxKey} are computed for each
   * split of the original file and used to filter rows from all the deltas.  The ROW__ID.rowid for
   * the rows of the 'original' file of course, must be assigned from the beginning of logical
   * bucket.  The last split of the logical bucket, i.e. the split that has the end of last file,
   * should include all insert events from deltas (last sentence is obsolete for Acid 2: HIVE-17320)
   */
  private static abstract class OriginalReaderPair implements ReaderPair {
    OrcStruct nextRecord;
    private final ReaderKey key;
    final int bucketId;
    final int bucketProperty;
    /**
     * Write Id to use when generating synthetic ROW_IDs
     */
    final long writeId;
    /**
     * @param statementId - this should be from delta_x_y_stmtId file name.  Imagine 2 load data
     *                    statements in 1 txn.  The stmtId will be embedded in
     *                    {@link RecordIdentifier#bucketId} via {@link BucketCodec} below
     */
    OriginalReaderPair(ReaderKey key, int bucketId, Configuration conf, Options mergeOptions,
      int statementId) throws IOException {
      this.key = key;
      this.bucketId = bucketId;
      assert bucketId >= 0 : "don't support non-bucketed tables yet";
      this.bucketProperty = encodeBucketId(conf, bucketId, statementId);
      writeId = mergeOptions.getWriteId();
    }
    @Override public final OrcStruct nextRecord() {
      return nextRecord;
    }
    @Override
    public int getColumns() {
      return getReader().getTypes().get(0).getSubtypesCount();
    }
    @Override
    public final ReaderKey getKey() { return key; }
    /**
     * The cumulative number of rows in all files of the logical bucket that precede the file
     * represented by {@link #getRecordReader()}
     */
    abstract long getRowIdOffset();

    final boolean nextFromCurrentFile(OrcStruct next) throws IOException {
      if (getRecordReader().hasNext()) {
        //RecordReader.getRowNumber() produces a file-global row number even with PPD
        long nextRowId = getRecordReader().getRowNumber() + getRowIdOffset();
        // have to do initialization here, because the super's constructor
        // calls next and thus we need to initialize before our constructor
        // runs
        if (next == null) {
          nextRecord = new OrcStruct(OrcRecordUpdater.FIELDS);
          IntWritable operation =
              new IntWritable(OrcRecordUpdater.INSERT_OPERATION);
          nextRecord().setFieldValue(OrcRecordUpdater.OPERATION, operation);
          nextRecord().setFieldValue(OrcRecordUpdater.CURRENT_WRITEID,
              new LongWritable(writeId));
          nextRecord().setFieldValue(OrcRecordUpdater.ORIGINAL_WRITEID,
              new LongWritable(writeId));
          nextRecord().setFieldValue(OrcRecordUpdater.BUCKET,
              new IntWritable(bucketProperty));
          nextRecord().setFieldValue(OrcRecordUpdater.ROW_ID,
              new LongWritable(nextRowId));
          nextRecord().setFieldValue(OrcRecordUpdater.ROW,
              getRecordReader().next(null));
        } else {
          nextRecord = next;
          ((IntWritable) next.getFieldValue(OrcRecordUpdater.OPERATION))
              .set(OrcRecordUpdater.INSERT_OPERATION);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.ORIGINAL_WRITEID))
              .set(writeId);
          ((IntWritable) next.getFieldValue(OrcRecordUpdater.BUCKET))
              .set(bucketProperty);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.CURRENT_WRITEID))
              .set(writeId);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.ROW_ID))
              .set(nextRowId);
          nextRecord().setFieldValue(OrcRecordUpdater.ROW,
              getRecordReader().next(OrcRecordUpdater.getRow(next)));
        }
        key.setValues(writeId, bucketProperty, nextRowId, writeId, false);
        if (getMaxKey() != null && key.compareRow(getMaxKey()) > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("key " + key + " > maxkey " + getMaxKey());
          }
          return false;//reached End Of Split
        }
        return true;
      }
      return false;//reached EndOfFile
    }
  }
  static int encodeBucketId(Configuration conf, int bucketId, int statementId) {
    return BucketCodec.V1.encode(new AcidOutputFormat.Options(conf).bucket(bucketId)
        .statementId(statementId));
  }
  /**
   * This handles normal read (as opposed to Compaction) of a {@link AcidUtils.AcidBaseFileType#ORIGINAL_BASE}
   * file.  These may be a result of Load Data or it may be a file that was written to the table
   * before it was converted to acid.
   */
  @VisibleForTesting
  final static class OriginalReaderPairToRead extends OriginalReaderPair {
    private final long rowIdOffset;
    private final Reader reader;
    private final RecordReader recordReader;
    private final RecordIdentifier minKey;
    private final RecordIdentifier maxKey;
    OriginalReaderPairToRead(ReaderKey key, Reader reader, int bucketId,
                             final RecordIdentifier minKey, final RecordIdentifier maxKey,
                             Reader.Options options, Options mergerOptions, Configuration conf,
                             ValidWriteIdList validWriteIdList, int statementId) throws IOException {
      super(key, bucketId, conf, mergerOptions, statementId);
      this.reader = reader;
      assert !mergerOptions.isCompacting();
      assert mergerOptions.getRootPath() != null : "Since we have original files";

      RecordIdentifier newMinKey = minKey;
      RecordIdentifier newMaxKey = maxKey;
      recordReader = reader.rowsOptions(options, conf);
      /**
       * Logically each bucket consists of 0000_0, 0000_0_copy_1... 0000_0_copy_N. etc  We don't
       * know N a priori so if this is true, then the current split is from 0000_0_copy_N file.
       * It's needed to correctly set maxKey.  In particular, set maxKey==null if this split
       * is the tail of the last file for this logical bucket to include all deltas written after
       * non-acid to acid table conversion (todo: HIVE-17320).
       * Also, see comments at {@link OriginalReaderPair} about unbucketed tables.
       */
      boolean isLastFileForThisBucket = true;
      boolean haveSeenCurrentFile = false;
      long rowIdOffsetTmp = 0;
      {
        /**
         * Note that for reading base_x/ or delta_x_x/ with non-acid schema,
         * {@link Options#getRootPath()} is set to base_x/ or delta_x_x/ which causes all it's
         * contents to be in {@link AcidDirectory#getOriginalFiles()}
         */
        //the split is from something other than the 1st file of the logical bucket - compute offset
        AcidDirectory directoryState = AcidUtils.getAcidState(null, mergerOptions.getRootPath(), conf,
            validWriteIdList, Ref.from(false), true);
        for (HadoopShims.HdfsFileStatusWithId f : directoryState.getOriginalFiles()) {
          int bucketIdFromPath = AcidUtils.parseBucketId(f.getFileStatus().getPath());
          if (bucketIdFromPath != bucketId) {
            continue;//todo: HIVE-16952
          }
          if (haveSeenCurrentFile) {
            //if here we already saw current file and now found another file for the same bucket
            //so the current file is not the last file of the logical bucket
            isLastFileForThisBucket = false;
            break;
          }
          if (f.getFileStatus().getPath().equals(mergerOptions.getBucketPath())) {
            /**
             * found the file whence the current split is from so we're done
             * counting {@link rowIdOffset}
             */
            haveSeenCurrentFile = true;
            isLastFileForThisBucket = true;
            continue;
          }
          try (Reader copyReader = OrcFile.createReader(f.getFileStatus().getPath(), OrcFile.readerOptions(conf))) {
            rowIdOffsetTmp += copyReader.getNumberOfRows();
          }
        }
        this.rowIdOffset = rowIdOffsetTmp;
        if (rowIdOffset > 0) {
          //rowIdOffset could be 0 if all files before current one are empty
          /**
           * Since we already done {@link OrcRawRecordMerger#discoverOriginalKeyBounds(Reader, int, Reader.Options, Configuration, Options)}
           * need to fix min/max key since these are used by
           * {@link #next(OrcStruct)} which uses {@link #rowIdOffset} to generate rowId for
           * the key.  Clear?  */
          if (minKey != null) {
            minKey.setRowId(minKey.getRowId() + rowIdOffset);
          } else {
            /**
             *  If this is not the 1st file, set minKey 1 less than the start of current file
             * (Would not need to set minKey if we knew that there are no delta files)
             * {@link #advanceToMinKey()} needs this */
            newMinKey = new RecordIdentifier(writeId, bucketProperty,rowIdOffset - 1);
          }
          if (maxKey != null) {
            maxKey.setRowId(maxKey.getRowId() + rowIdOffset);
          }
        }
      }
      if (!isLastFileForThisBucket && maxKey == null) {
          /*
           * If this is the last file for this bucket, maxKey == null means the split is the tail
           * of the file so we want to leave it blank to make sure any insert events in delta
           * files are included; Conversely, if it's not the last file, set the maxKey so that
           * events from deltas that don't modify anything in the current split are excluded*/
        newMaxKey = new RecordIdentifier(writeId, bucketProperty,
          rowIdOffset + reader.getNumberOfRows() - 1);
      }
      this.minKey = newMinKey;
      this.maxKey = newMaxKey;

      // advance the reader until we reach the minimum key
      do {
        next(nextRecord());
      } while (nextRecord() != null &&
        (getMinKey() != null && this.getKey().compareRow(getMinKey()) <= 0));
    }
    @Override public RecordReader getRecordReader() {
      return recordReader;
    }
    @Override public Reader getReader() { return reader; }
    @Override public RecordIdentifier getMinKey() { return minKey; }
    @Override public RecordIdentifier getMaxKey() {
      return maxKey;
    }
    @Override public long getRowIdOffset() { return rowIdOffset; }

    @Override
    public void next(OrcStruct next) throws IOException {
      if(!nextFromCurrentFile(next)) {
        //only have 1 file so done
        nextRecord = null;
        getRecordReader().close();
      }
    }
  }
  @VisibleForTesting
  final static class OriginalReaderPairToCompact extends OriginalReaderPair {
    /**
     * See {@link AcidDirectory#getOriginalFiles()}.  This list has a fixed sort order.
     * It includes all original files (for all buckets).  
     */
    private final List<HadoopShims.HdfsFileStatusWithId> originalFiles;
    /**
     * index into {@link #originalFiles}
     */
    private int nextFileIndex = 0;
    private Reader reader;
    private RecordReader recordReader = null;
    private final Configuration conf;
    private final Reader.Options options;
    private long rowIdOffset = 0;

    OriginalReaderPairToCompact(ReaderKey key, int bucketId,
                       Reader.Options options, Options mergerOptions, Configuration conf,
                       ValidWriteIdList validWriteIdList, int statementId) throws IOException {
      super(key, bucketId, conf, mergerOptions, statementId);
      assert mergerOptions.isCompacting() : "Should only be used for Compaction";
      this.conf = conf;
      this.options = options;
      assert mergerOptions.getRootPath() != null : "Since we have original files";
      assert this.bucketId >= 0 : "don't support non-bucketed tables yet";
      //when compacting each split needs to process the whole logical bucket
      assert options.getOffset() == 0;
      assert options.getMaxOffset() == Long.MAX_VALUE;
      AcidDirectory directoryState = AcidUtils.getAcidState(null, mergerOptions.getRootPath(), conf,
          validWriteIdList, Ref.from(false), true);
      /**
       * Note that for reading base_x/ or delta_x_x/ with non-acid schema,
       * {@link Options#getRootPath()} is set to base_x/ or delta_x_x/ which causes all it's
       * contents to be in {@link AcidDirectory#getOriginalFiles()}
       */
      originalFiles = directoryState.getOriginalFiles();
      assert originalFiles.size() > 0;
      //in case of Compaction, this is the 1st file of the current bucket
      this.reader = advanceToNextFile();
      if (reader == null) {
        //Compactor generated a split for a bucket that has no data?
        throw new IllegalStateException("No 'original' files found for bucketId=" + this.bucketId +
          " in " + mergerOptions.getRootPath());
      }
      recordReader = getReader().rowsOptions(options, conf);
      next(nextRecord());//load 1st row
    }
    @Override public RecordReader getRecordReader() {
      return recordReader;
    }
    @Override public Reader getReader() { return reader; }
    @Override public RecordIdentifier getMinKey() {
      return null;
    }
    @Override public RecordIdentifier getMaxKey() {
      return null;
    }
    @Override public long getRowIdOffset() { return rowIdOffset; }

    @Override
    public void next(OrcStruct next) throws IOException {
      while(true) {
        if(nextFromCurrentFile(next)) {
          return;
        } else {
          if (originalFiles.size() <= nextFileIndex) {
            //no more original files to read
            nextRecord = null;
            recordReader.close();
            return;
          } else {
            rowIdOffset += reader.getNumberOfRows();
            recordReader.close();
            reader = advanceToNextFile();
            if(reader == null) {
              nextRecord = null;
              return;
            }
            recordReader = reader.rowsOptions(options, conf);
          }
        }
      }
    }
    /**
     * Finds the next file of the logical bucket
     * @return {@code null} if there are no more files
     */
    private Reader advanceToNextFile() throws IOException {
      while(nextFileIndex < originalFiles.size()) {
        int bucketIdFromPath = AcidUtils.parseBucketId(originalFiles.get(nextFileIndex).getFileStatus().getPath());
        if (bucketIdFromPath == bucketId) {
          break;
        }
        //the the bucket we care about here
        nextFileIndex++;
      }
      if(originalFiles.size() <= nextFileIndex) {
        return null;//no more files for current bucket
      }
      return OrcFile.createReader(originalFiles.get(nextFileIndex++).getFileStatus().
        getPath(), OrcFile.readerOptions(conf));
    }
  }

  /**
   * The process here reads several (base + some deltas) files each of which is sorted on 
   * {@link ReaderKey} ascending.  The output of this Reader should a global order across these
   * files.  The root of this tree is always the next 'file' to read from.
   */
  private final TreeMap<ReaderKey, ReaderPair> readers = new TreeMap<>();

  // The reader that currently has the lowest key.
  private ReaderPair primary;

  // The key of the next lowest reader.
  private ReaderKey secondaryKey = null;
  static final class KeyInterval {
    private final RecordIdentifier minKey;
    private final RecordIdentifier maxKey;
    KeyInterval(RecordIdentifier minKey, RecordIdentifier maxKey) {
      this.minKey = minKey;
      this.maxKey = maxKey;
    }
    RecordIdentifier getMinKey() {
      return minKey;
    }
    RecordIdentifier getMaxKey() {
      return maxKey;
    };
    @Override
    public String toString() {
      return "KeyInterval[" + minKey + "," + maxKey + "]";
    }
    @Override
    public boolean equals(Object other) {
      if(!(other instanceof KeyInterval)) {
        return false;
      }
      KeyInterval otherInterval = (KeyInterval)other;
      return Objects.equals(minKey, otherInterval.getMinKey()) &&
          Objects.equals(maxKey, otherInterval.getMaxKey());
    }
    @Override
    public int hashCode() {
      return Objects.hash(minKey, maxKey);
    }

    public boolean isIntersects(KeyInterval other) {
      return (minKey == null || other.maxKey == null || minKey.compareTo(other.maxKey) <= 0) &&
          (maxKey == null || other.minKey == null || maxKey.compareTo(other.minKey) >= 0);
    }
  }
  /**
   * Find the key range for original bucket files.
   * For unbucketed tables the insert event data is still written to bucket_N file except that
   * N is just a writer ID - it still matches {@link RecordIdentifier#getBucketProperty()}.  For
   * 'original' files (ubucketed) the same applies.  A file 000000_0 encodes a taskId/wirterId and
   * at read time we synthesize {@link RecordIdentifier#getBucketProperty()} to match the file name
   * and so the same bucketProperty is used here to create minKey/maxKey, i.e. these keys are valid
   * to filter data from delete_delta files even for unbucketed tables.
   * @param reader the reader
   * @param bucket the bucket number we are reading
   * @param options the options for reading with
   * @throws IOException
   */
  private KeyInterval discoverOriginalKeyBounds(Reader reader, int bucket,
                                         Reader.Options options,
                                         Configuration conf, Options mergerOptions) throws IOException {
    long rowLength = 0;
    long rowOffset = 0;
    long offset = options.getOffset();//this would usually be at block boundary
    long maxOffset = options.getMaxOffset();//this would usually be at block boundary
    boolean isTail = true;
    RecordIdentifier minKey = null;
    RecordIdentifier maxKey = null;
    TransactionMetaData tfp = TransactionMetaData.findWriteIDForSynthetcRowIDs(
      mergerOptions.getBucketPath(), mergerOptions.getRootPath(), conf);
    int bucketProperty = encodeBucketId(conf, bucket, tfp.statementId);
   /**
    * options.getOffset() and getMaxOffset() would usually be at block boundary which doesn't
    * necessarily match stripe boundary.  So we want to come up with minKey to be one before the 1st
    * row of the first stripe that starts after getOffset() and maxKey to be the last row of the
    * stripe that contains getMaxOffset().  This breaks if getOffset() and getMaxOffset() are inside
    * the sames tripe - in this case we have minKey & isTail=false but rowLength is never set.
    * (HIVE-16953)
    */
    for(StripeInformation stripe: reader.getStripes()) {
      if (offset > stripe.getOffset()) {
        rowOffset += stripe.getNumberOfRows();
      } else if (maxOffset > stripe.getOffset()) {
        rowLength += stripe.getNumberOfRows();
      } else {
        isTail = false;
        break;
      }
    }
    if (rowOffset > 0) {
      minKey = new RecordIdentifier(tfp.syntheticWriteId, bucketProperty, rowOffset - 1);
    }
    if (!isTail) {
      maxKey = new RecordIdentifier(tfp.syntheticWriteId, bucketProperty, rowOffset + rowLength - 1);
    }
    return new KeyInterval(minKey, maxKey);
  }

  /**
   * Find the key range for the split (of the base) based on the 'hive.acid.key.index' metadata.
   * These keys are used to filter delta files since both are sorted by key.
   * If 'hive.acid.key.index' is missing from the ORC file, return null keys (which forces a full read).
   * @param reader the reader
   * @param options the options for reading with
   */
  private KeyInterval discoverKeyBounds(Reader reader, Reader.Options options) {
    final RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(reader);
    if (keyIndex == null) {
      LOG.warn("Missing '{}' metadata in ORC file, can't compute min/max keys",
          OrcRecordUpdater.ACID_KEY_INDEX_NAME);
      return new KeyInterval(null, null);
    }

    long offset = options.getOffset();
    long maxOffset = options.getMaxOffset();
    int firstStripe = 0;
    int stripeCount = 0;
    boolean isTail = true;
    RecordIdentifier minKey = null;
    RecordIdentifier maxKey = null;
    
    List<StripeInformation> stripes = reader.getStripes();
    for(StripeInformation stripe: stripes) {
      if (offset > stripe.getOffset()) {
        firstStripe += 1;
      } else if (maxOffset > stripe.getOffset()) {
        stripeCount += 1;
      } else {
        isTail = false;
        break;
      }
    }
    if (firstStripe != 0) {
      minKey = keyIndex[firstStripe - 1];
    }
    if (!isTail) {
      maxKey = keyIndex[firstStripe + stripeCount - 1];
    }
    return new KeyInterval(minKey, maxKey);
  }

  /**
   * Convert from the row include/sarg/columnNames to the event equivalent
   * for the underlying file.
   * @param options options for the row reader
   * @param rowSchema schema of the row, excluding ACID columns
   * @return a cloned options object that is modified for the event reader
   */
  static Reader.Options createEventOptions(Reader.Options options, TypeDescription rowSchema) {
    Reader.Options result = options.clone();
    result.include(options.getInclude());

    // slide the column names down by 6 for the name array
    if (options.getColumnNames() != null) {
      String[] orig = options.getColumnNames();
      String[] cols = new String[orig.length + OrcRecordUpdater.FIELDS];
      for(int i=0; i < orig.length; ++i) {
        cols[i + OrcRecordUpdater.FIELDS] = orig[i];
      }
      result.searchArgument(options.getSearchArgument(), cols);
    }

    // schema evolution will insert the acid columns to row schema for ACID read
    result.schema(rowSchema);

    return result;
  }

  /**
   * {@link OrcRawRecordMerger} Acid reader is used slightly differently in various contexts.
   * This makes the "context" explicit.
   */
  static class Options implements Cloneable {
    private int copyIndex = 0;
    private boolean isCompacting = false;
    private Path bucketPath;
    private Path rootPath;
    private Path baseDir;
    private boolean isMajorCompaction = false;
    private boolean isDeleteReader = false;
    private long writeId = 0;
    Options copyIndex(int copyIndex) {
      assert copyIndex >= 0;
      this.copyIndex = copyIndex;
      return this;
    }
    Options isCompacting(boolean isCompacting) {
      this.isCompacting = isCompacting;
      assert !isDeleteReader;
      return this;
    }
    Options bucketPath(Path bucketPath) {
      this.bucketPath = bucketPath;
      return this;
    }
    Options rootPath(Path rootPath) {
      this.rootPath = rootPath;
      return this;
    }
    Options isMajorCompaction(boolean isMajor) {
      this.isMajorCompaction = isMajor;
      assert !isDeleteReader;
      return this;
    }
    Options isDeleteReader(boolean isDeleteReader) {
      this.isDeleteReader = isDeleteReader;
      assert !isCompacting;
      return this;
    }
    Options writeId(long writeId) {
      this.writeId = writeId;
      return this;
    }
    Options baseDir(Path baseDir) {
      this.baseDir = baseDir;
      return this;
    }
    /**
     * 0 means it's the original file, without {@link Utilities#COPY_KEYWORD} suffix
     */
    int getCopyIndex() {
      return copyIndex;
    }
    boolean isCompacting() {
      return isCompacting;
    }
    /**
     * Full path to the data file
     */
    Path getBucketPath() {
      return bucketPath;
    }
    /**
     * Partition folder (Table folder if not partitioned)
     */
    Path getRootPath()  { return rootPath; }
    /**
     * @return true if major compaction, false if minor
     */
    boolean isMajorCompaction() {
      return isMajorCompaction && isCompacting;
    }
    boolean isMinorCompaction() {
      return !isMajorCompaction && isCompacting;
    }
    /**
     * true if this is only processing delete deltas to load in-memory table for
     * vectorized reader
     */
    boolean isDeleteReader() {
      return isDeleteReader;
    }
    /**
     * for reading "original" files - i.e. not native acid schema.  Default value of 0 is
     * appropriate for files that existed in a table before it was made transactional.  0 is the
     * primordial transaction.  For non-native files resulting from Load Data command, they
     * are located and base_x or delta_x_x and then writeId == x.
     */
    long getWriteId() {
      return writeId;
    }

    /**
     * In case of isMajorCompaction() this is the base dir from the Compactor, i.e. either a base_x
     * or {@link #rootPath} if it's the 1st major compaction after non-acid2acid conversion
     */
    Path getBaseDir() {
      return baseDir;
    }
    /**
     * shallow clone
     */
    public Options clone() {
      try {
        return (Options) super.clone();
      }
      catch(CloneNotSupportedException ex) {
        throw new AssertionError();
      }
    }
  }

  OrcRawRecordMerger(Configuration conf,
      boolean collapseEvents,
      Reader reader,
      boolean isOriginal,
      int bucket,
      ValidWriteIdList validWriteIdList,
      Reader.Options options,
      Path[] deltaDirectory,
      Options mergerOptions) throws IOException {
    this(conf, collapseEvents, reader, isOriginal, bucket, validWriteIdList, options, deltaDirectory, mergerOptions,
        null);
  }

  /**
   * Create a reader that merge sorts the ACID events together.  This handles
   * 1. 'normal' reads on behalf of a query (non vectorized)
   * 2. Compaction reads (major/minor)
   * 3. Delete event reads - to create a sorted view of all delete events for vectorized read
   *
   * This makes the logic in the constructor confusing and needs to be refactored.  Liberal use of
   * asserts below is primarily for documentation purposes.
   *
   * @param conf the configuration
   * @param collapseEvents should the events on the same row be collapsed
   * @param isOriginal if reading filws w/o acid schema - {@link AcidUtils.AcidBaseFileType#ORIGINAL_BASE}
   * @param bucket the bucket/writer id of the file we are reading
   * @param options the options to read with
   * @param deltaDirectory the list of delta directories to include
   * @throws IOException
   */
  OrcRawRecordMerger(Configuration conf,
                     boolean collapseEvents,
                     Reader reader,
                     boolean isOriginal,
                     int bucket,
                     ValidWriteIdList validWriteIdList,
                     Reader.Options options,
                     Path[] deltaDirectory,
                     Options mergerOptions,
                     Map<String, Integer> deltasToAttemptId) throws IOException {
    this.collapse = collapseEvents;
    this.offset = options.getOffset();
    this.length = options.getLength();
    this.validWriteIdList = validWriteIdList;
    /**
     * @since Hive 3.0
     * With split update (HIVE-14035) we have base/, delta/ and delete_delta/ - the latter only
     * has Delete events and the others only have Insert events.  Thus {@link #baseReader} is
     * a split of a file in base/ or delta/.
     *
     * For Compaction, each split (for now) is a logical bucket, i.e. all files from base/ + delta(s)/
     * for a given bucket ID and delete_delta(s)/
     *
     * For bucketed tables, the data files are named bucket_N and all rows in this file are such
     * that {@link org.apache.hadoop.hive.ql.io.BucketCodec#decodeWriterId(int)} of
     * {@link RecordIdentifier#getBucketProperty()} is N.  This is currently true for all types of
     * files but may not be true for for delete_delta/ files in the future.
     *
     * For un-bucketed tables, the system is designed so that it works when there is no relationship
     * between delete_delta file name (bucket_N) and the value of {@link RecordIdentifier#getBucketProperty()}.
     * (Later we this maybe optimized to take advantage of situations where it is known that
     * bucket_N matches bucketProperty().)  This implies that for a given {@link baseReader} all
     * files in delete_delta/ have to be opened ({@link ReaderPair} created).  Insert events are
     * still written such that N in file name (writerId) matches what's in bucketProperty().
     *
     * Compactor for un-bucketed tables works exactly the same as for bucketed ones though it
     * should be optimized (see HIVE-17206).  In particular, each split is a set of files
     * created by a writer with the same writerId, i.e. all bucket_N files across base/ &
     * deleta/ for the same N. Unlike bucketed tables, there is no relationship between
     * any values in user columns to file name.
     * The maximum N is determined by the number of writers the system chose for the the "largest"
     * write into a given partition.
     *
     * In both cases, Compactor should be changed so that Minor compaction is run very often and
     * only compacts delete_delta/.  Major compaction can do what it does now.
     */
    boolean isBucketed = conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0) > 0;

    TypeDescription typeDescr =
        OrcInputFormat.getDesiredRowTypeDescr(conf, true, Integer.MAX_VALUE);

    objectInspector = OrcRecordUpdater.createEventObjectInspector
        (OrcStruct.createObjectInspector(0, OrcUtils.getOrcTypes(typeDescr)));
    assert !(mergerOptions.isCompacting() && reader != null) : "don't need a reader for compaction";

    // modify the options to reflect the event instead of the base row
    Reader.Options eventOptions = createEventOptions(options, typeDescr);
    //suppose it's the first Major compaction so we only have deltas
    boolean isMajorNoBase = mergerOptions.isCompacting() && mergerOptions.isMajorCompaction()
      && mergerOptions.getBaseDir() == null;
    if((mergerOptions.isCompacting() && mergerOptions.isMinorCompaction()) ||
      mergerOptions.isDeleteReader() || isMajorNoBase) {
      //for minor compaction, there is no progress report and we don't filter deltas
      baseReader = null;
      minKey = maxKey = null;
      assert reader == null : "unexpected input reader during minor compaction: " +
        mergerOptions.getRootPath();
    } else {
      KeyInterval keyInterval;
      if (mergerOptions.isCompacting()) {
        assert mergerOptions.isMajorCompaction();
        //compaction doesn't filter deltas but *may* have a reader for 'base'
        keyInterval = new KeyInterval(null, null);
      } else {
        // find the min/max based on the offset and length (and more for 'original')
        if (isOriginal) {
          //note that this KeyInterval may be adjusted later due to copy_N files
          keyInterval = discoverOriginalKeyBounds(reader, bucket, options, conf, mergerOptions);
        } else {
          keyInterval = discoverKeyBounds(reader, options);
        }
      }
      LOG.info("min key = " + keyInterval.getMinKey() + ", max key = " + keyInterval.getMaxKey());
      // use the min/max instead of the byte range
      ReaderPair pair = null;
      ReaderKey baseKey = new ReaderKey();
      if (isOriginal) {
        options = options.clone();
        if(mergerOptions.isCompacting()) {
          assert mergerOptions.isMajorCompaction();
          Options readerPairOptions = mergerOptions;
          if(mergerOptions.getBaseDir().getName().startsWith(AcidUtils.BASE_PREFIX)) {
            readerPairOptions = modifyForNonAcidSchemaRead(mergerOptions,
                AcidUtils.ParsedBaseLight.parseBase(mergerOptions.getBaseDir()).getWriteId(),
                mergerOptions.getBaseDir());
          }
          pair = new OriginalReaderPairToCompact(baseKey, bucket, options, readerPairOptions,
            conf, validWriteIdList,
            0);//0 since base_x doesn't have a suffix (neither does pre acid write)
        } else {
          assert mergerOptions.getBucketPath() != null : " since this is not compaction: "
            + mergerOptions.getRootPath();
          //if here it's a non-acid schema file - check if from before table was marked transactional
          //or in base_x/delta_x_x from Load Data
          Options readerPairOptions = mergerOptions;
          TransactionMetaData tfp = TransactionMetaData.findWriteIDForSynthetcRowIDs(
            mergerOptions.getBucketPath(), mergerOptions.getRootPath(), conf);
          if(tfp.syntheticWriteId > 0) {
            readerPairOptions = modifyForNonAcidSchemaRead(mergerOptions,
              tfp.syntheticWriteId, tfp.folder);
          }
          pair = new OriginalReaderPairToRead(baseKey, reader, bucket, keyInterval.getMinKey(),
            keyInterval.getMaxKey(), options,  readerPairOptions, conf, validWriteIdList, tfp.statementId);
        }
      } else {
        if(mergerOptions.isCompacting()) {
          assert mergerOptions.isMajorCompaction() : "expected major compaction: "
            + mergerOptions.getBaseDir() + ":" + bucket;
          assert mergerOptions.getBaseDir() != null : "no baseDir?: " + mergerOptions.getRootPath();
          //we are compacting and it's acid schema so create a reader for the 1st bucket file that is not empty
          FileSystem fs = mergerOptions.getBaseDir().getFileSystem(conf);
          Integer attemptId = null;
          if (deltasToAttemptId != null) {
            attemptId = deltasToAttemptId.get(mergerOptions.getBaseDir().getName());
          }
          Path bucketPath = AcidUtils.createBucketFile(mergerOptions.getBaseDir(), bucket, attemptId);
          if(fs.exists(bucketPath) && fs.getFileStatus(bucketPath).getLen() > 0) {
            //doing major compaction - it's possible where full compliment of bucket files is not
            //required (on Tez) that base_x/ doesn't have a file for 'bucket'
            reader = OrcFile.createReader(bucketPath, OrcFile.readerOptions(conf));
            pair = new ReaderPairAcid(baseKey, reader, keyInterval.getMinKey(), keyInterval.getMaxKey(),
              eventOptions, conf);
          }
          else {
            pair = new EmptyReaderPair();
            LOG.info("No non-empty " + bucketPath + " was found for Major compaction");
          }
        }
        else {
          assert reader != null : "no reader? " + mergerOptions.getRootPath();
          pair = new ReaderPairAcid(baseKey, reader, keyInterval.getMinKey(), keyInterval.getMaxKey(),
            eventOptions, conf);
        }
      }
      minKey = pair.getMinKey();
      maxKey = pair.getMaxKey();
      LOG.info("updated min key = " + keyInterval.getMinKey() + ", max key = " + keyInterval.getMaxKey());
      // if there is at least one record, put it in the map
      if (pair.nextRecord() != null) {
        ensurePutReader(baseKey, pair);
        baseKey = null;
      }
      baseReader = pair.getRecordReader();
    }
    /*now process the delta files.  For normal read these should only be delete deltas.  For
    * Compaction these may be any delta_x_y/.  The files inside any delta_x_y/ may be in Acid
    * format (i.e. with Acid metadata columns) or 'original'.*/
    if (deltaDirectory != null && deltaDirectory.length > 0) {
      /*For reads, whatever SARG maybe applicable to base it's not applicable to delete_delta since it has no
      * user columns.  For Compaction there is never a SARG.
      * */
      Reader.Options deltaEventOptions = eventOptions.clone()
        .searchArgument(null, null).range(0, Long.MAX_VALUE);
      for(Path delta: deltaDirectory) {
        if(!mergerOptions.isCompacting() && !AcidUtils.isDeleteDelta(delta)) {
          //all inserts should be in baseReader for normal read so this should always be delete delta if not compacting
          throw new IllegalStateException(delta + " is not delete delta and is not compacting.");
        }
        ReaderKey key = new ReaderKey();
        //todo: only need to know isRawFormat if compacting for acid V2 and V2 should normally run
        //in vectorized mode - i.e. this is not a significant perf overhead vs ParsedDeltaLight
        AcidUtils.ParsedDelta deltaDir = AcidUtils.parsedDelta(delta, delta.getFileSystem(conf));
        if(deltaDir.isRawFormat()) {
          assert !deltaDir.isDeleteDelta() : delta.toString();
          assert mergerOptions.isCompacting() : "during regular read anything which is not a" +
            " delete_delta is treated like base: " + delta;
          Options rawCompactOptions = modifyForNonAcidSchemaRead(mergerOptions, deltaDir.getMinWriteId(), delta);

          //this will also handle copy_N files if any
          ReaderPair deltaPair =  new OriginalReaderPairToCompact(key, bucket, options,
              rawCompactOptions, conf, validWriteIdList, deltaDir.getStatementId());
          if (deltaPair.nextRecord() != null) {
            ensurePutReader(key, deltaPair);
            key = new ReaderKey();
          }
          continue;
        }

        Integer attemptId = null;
        if (deltasToAttemptId != null) {
          attemptId = deltasToAttemptId.get(delta.getName());
        }

        for (Path deltaFile : getDeltaFiles(delta, bucket, mergerOptions, attemptId)) {
          FileSystem fs = deltaFile.getFileSystem(conf);
          if(!fs.exists(deltaFile)) {
            /**
             * it's possible that the file for a specific {@link bucket} doesn't exist in any given
             * delta since since no rows hashed to it (and not configured to create empty buckets)
             */
            continue;
          }
          LOG.debug("Looking at delta file {}", deltaFile);
          if(deltaDir.isDeleteDelta()) {
            //if here it maybe compaction or regular read or Delete event sorter
            //in the later 2 cases we should do:
            //HIVE-17320: we should compute a SARG to push down min/max key to delete_delta
            Reader deltaReader = OrcFile.createReader(deltaFile, OrcFile.readerOptions(conf));
            ReaderPair deltaPair = new ReaderPairAcid(key, deltaReader, minKey, maxKey,
                deltaEventOptions, conf);
            if (deltaPair.nextRecord() != null) {
              ensurePutReader(key, deltaPair);
              key = new ReaderKey();
            }
            continue;
          }
          //if here then we must be compacting
          assert mergerOptions.isCompacting() : "not compacting and not delete delta : " + delta;
          /* side files are only created by streaming ingest.  If this is a compaction, we may
          * have an insert delta/ here with side files there because the original writer died.*/
          long length = AcidUtils.getLogicalLength(fs, fs.getFileStatus(deltaFile));
          assert length >= 0;
          Reader deltaReader = OrcFile.createReader(deltaFile, OrcFile.readerOptions(conf).maxLength(length));
          //must get statementId from file name since Acid 1.0 doesn't write it into bucketProperty
          ReaderPairAcid deltaPair = new ReaderPairAcid(key, deltaReader, minKey, maxKey, deltaEventOptions, conf);
          if (deltaPair.nextRecord() != null) {
            ensurePutReader(key, deltaPair);
            key = new ReaderKey();
          }
        }
      }
    }

    // get the first record
    LOG.debug("Final reader map {}", readers);
    Map.Entry<ReaderKey, ReaderPair> entry = readers.pollFirstEntry();
    if (entry == null) {
      columns = 0;
      primary = null;
    } else {
      primary = entry.getValue();
      if (readers.isEmpty()) {
        secondaryKey = null;
      } else {
        secondaryKey = readers.firstKey();
      }
      // get the number of columns in the user's rows
      columns = primary.getColumns();
    }
  }

  private void ensurePutReader(ReaderKey key, ReaderPair deltaPair) throws IOException {
    ReaderPair oldPair = readers.put(key, deltaPair);
    if (oldPair == null) return;
    String error = "Two readers for " + key + ": new " + deltaPair + ", old " + oldPair;
    LOG.error(error);
    throw new IOException(error);
  }

  /**
   * For use with Load Data statement which places {@link AcidUtils.AcidBaseFileType#ORIGINAL_BASE}
   * type files into a base_x/ or delta_x_x.  The data in these are then assigned ROW_IDs at read
   * time and made permanent at compaction time.  This is identical to how 'original' files (i.e.
   * those that existed in the table before it was converted to an Acid table) except that the
   * write ID to use in the ROW_ID should be that of the transaction that ran the Load Data.
   */
  static final class TransactionMetaData {
    final long syntheticWriteId;
    /**
     * folder which determines the write id to use in synthetic ROW_IDs
     */
    final Path folder;
    final int statementId;
    TransactionMetaData(long syntheticWriteId, Path folder) {
      this(syntheticWriteId, folder, 0);
    }
    TransactionMetaData(long syntheticWriteId, Path folder, int statementId) {
      this.syntheticWriteId = syntheticWriteId;
      this.folder = folder;
      this.statementId = statementId;
    }
    static TransactionMetaData findWriteIDForSynthetcRowIDs(Path splitPath, Path rootPath,
      Configuration conf) throws IOException {
      Path parent = splitPath.getParent();
      if(rootPath.equals(parent)) {
        //the 'isOriginal' file is at the root of the partition (or table) thus it is
        //from a pre-acid conversion write and belongs to primordial writeid:0.
        return new TransactionMetaData(0, parent);
      }
      while(parent != null && !rootPath.equals(parent)) {
        boolean isBase = parent.getName().startsWith(AcidUtils.BASE_PREFIX);
        boolean isDelta = parent.getName().startsWith(AcidUtils.DELTA_PREFIX)
            || parent.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX);
        if(isBase || isDelta) {
          if(isBase) {
            return new TransactionMetaData(AcidUtils.ParsedBaseLight.parseBase(parent).getWriteId(),
                parent);
          }
          else {
            AcidUtils.ParsedDeltaLight pd = AcidUtils.ParsedDeltaLight.parse(parent);
            return new TransactionMetaData(pd.getMinWriteId(), parent, pd.getStatementId());
          }
        }
        parent = parent.getParent();
      }
      if(parent == null) {
        //spit is marked isOriginal but it's not an immediate child of a partition nor is it in a
        //base/ or delta/ - this should never happen
        throw new IllegalStateException("Cannot determine write id for original file "
          + splitPath + " in " + rootPath);
      }
      //"warehouse/t/HIVE_UNION_SUBDIR_15/000000_0" is a meaningful path for nonAcid2acid
      // converted table
      return new TransactionMetaData(0, rootPath);
    }
  }
  /**
   * This is done to read non-acid schema files ("original") located in base_x/ or delta_x_x/ which
   * happens as a result of Load Data statement.  Setting {@code rootPath} to base_x/ or delta_x_x
   * causes {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} in subsequent
   * {@link OriginalReaderPair} object to return the files in this dir
   * in {@link AcidDirectory#getOriginalFiles()}
   * @return modified clone of {@code baseOptions}
   */
  private Options modifyForNonAcidSchemaRead(Options baseOptions, long writeId, Path rootPath) {
    return baseOptions.clone().writeId(writeId).rootPath(rootPath);
  }
  /**
   * This determines the set of {@link ReaderPairAcid} to create for a given delta/.
   * For unbucketed tables {@code bucket} can be thought of as a write tranche.
   */
  static Path[] getDeltaFiles(Path deltaDirectory, int bucket, Options mergerOptions, Integer attemptId) {
    assert (!mergerOptions.isCompacting &&
        deltaDirectory.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)
    ) || mergerOptions.isCompacting : "Unexpected delta: " + deltaDirectory +
        "(isCompacting=" + mergerOptions.isCompacting() + ")";
    return new Path[] {AcidUtils.createBucketFile(deltaDirectory, bucket, attemptId)};
  }
  
  @VisibleForTesting
  RecordIdentifier getMinKey() {
    return minKey;
  }

  @VisibleForTesting
  RecordIdentifier getMaxKey() {
    return maxKey;
  }

  @VisibleForTesting
  ReaderPair getCurrentReader() {
    return primary;
  }

  @VisibleForTesting
  Map<ReaderKey, ReaderPair> getOtherReaders() {
    return readers;
  }

  @Override
  public boolean next(RecordIdentifier recordIdentifier,
                      OrcStruct prev) throws IOException {
    boolean keysSame = true;
    while (keysSame && primary != null) {

      // The primary's nextRecord is the next value to return
      OrcStruct current = primary.nextRecord();
      recordIdentifier.set(primary.getKey());

      // Advance the primary reader to the next record
      primary.next(extraValue);

      // Save the current record as the new extraValue for next time so that
      // we minimize allocations
      extraValue = current;

      // now that the primary reader has advanced, we need to see if we
      // continue to read it or move to the secondary.
      if (primary.nextRecord() == null ||
          primary.getKey().compareTo(secondaryKey) > 0) {

        // if the primary isn't done, push it back into the readers
        if (primary.nextRecord() != null) {
          readers.put(primary.getKey(), primary);
        }

        // update primary and secondaryKey
        Map.Entry<ReaderKey, ReaderPair> entry = readers.pollFirstEntry();
        if (entry != null) {
          primary = entry.getValue();
          if (readers.isEmpty()) {
            secondaryKey = null;
          } else {
            secondaryKey = readers.firstKey();
          }
        } else {
          primary = null;
        }
      }

      // if this transaction isn't ok, skip over it
      if (!validWriteIdList.isWriteIdValid(
          ((ReaderKey) recordIdentifier).getCurrentWriteId())) {
        continue;
      }

      /*for multi-statement txns, you may have multiple events for the same
      * row in the same (current) transaction.  We want to collapse these to just the last one
      * regardless whether we are minor compacting.  Consider INSERT/UPDATE/UPDATE of the
      * same row in the same txn.  There is no benefit passing along anything except the last
      * event.  If we did want to pass it along, we'd have to include statementId in the row
      * returned so that compaction could write it out or make minor minor compaction understand
      * how to write out delta files in delta_xxx_yyy_stid format.  There doesn't seem to be any
      * value in this.
      *
      * todo: this could be simplified since in Acid2 even if you update the same row 2 times in 1
      * txn, it will have different ROW__IDs, i.e. there is no such thing as multiple versions of
      * the same physical row.  Leave it for now since this Acid reader should go away altogether
      * and org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader will be used.*/
      boolean isSameRow = prevKey.isSameRow((ReaderKey)recordIdentifier);
      // if we are collapsing, figure out if this is a new row
      if (collapse || isSameRow) {
        // Note: for collapse == false, this just sets keysSame.
        keysSame = (collapse && prevKey.compareRow(recordIdentifier) == 0) || (isSameRow);
        if (keysSame) {
          keysSame = collapse(recordIdentifier);
        } else {
          prevKey.set(recordIdentifier);
        }
      } else {
        keysSame = false;
      }

      // set the output record by fiddling with the pointers so that we can
      // avoid a copy.
      prev.linkFields(current);
    }
    return !keysSame;
  }

  protected boolean collapse(RecordIdentifier recordIdentifier) {
    return true;
  }

  @Override
  public OrcRawRecordMerger.ReaderKey createKey() {
    return new ReaderKey();
  }

  @Override
  public OrcStruct createValue() {
    return new OrcStruct(OrcRecordUpdater.FIELDS);
  }

  @Override
  public long getPos() throws IOException {
    return offset + (long)(getProgress() * length);
  }

  @Override
  public void close() throws IOException {
    if (primary != null) {
      primary.getRecordReader().close();
    }
    for(ReaderPair pair: readers.values()) {
      pair.getRecordReader().close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    //this is not likely to do the right thing for Compaction of "original" files when there are copyN files
    return baseReader == null ? 1 : baseReader.getProgress();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }

  @Override
  public boolean isDelete(OrcStruct value) {
    return OrcRecordUpdater.getOperation(value) == OrcRecordUpdater.DELETE_OPERATION;
  }

  /**
   * Get the number of columns in the underlying rows.
   * @return 0 if there are no base and no deltas.
   */
  public int getColumns() {
    return columns;
  }
}
