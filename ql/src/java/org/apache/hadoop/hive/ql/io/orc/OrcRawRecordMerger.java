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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Merges a base and a list of delta files together into a single stream of
 * events.
 */
public class OrcRawRecordMerger implements AcidInputFormat.RawReader<OrcStruct>{

  private static final Log LOG = LogFactory.getLog(OrcRawRecordMerger.class);

  private final Configuration conf;
  private final boolean collapse;
  private final RecordReader baseReader;
  private final long offset;
  private final long length;
  private final ValidTxnList validTxnList;
  private final int columns;
  private ReaderKey prevKey = new ReaderKey();
  // this is the key less than the lowest key we need to process
  private RecordIdentifier minKey;
  // this is the last key we need to process
  private RecordIdentifier maxKey;
  // an extra value so that we can return it while reading ahead
  private OrcStruct extraValue;

  /**
   * A RecordIdentifier extended with the current transaction id. This is the
   * key of our merge sort with the originalTransaction, bucket, and rowId
   * ascending and the currentTransaction descending. This means that if the
   * reader is collapsing events to just the last update, just the first
   * instance of each record is required.
   */
  final static class ReaderKey extends RecordIdentifier{
    private long currentTransactionId;

    public ReaderKey() {
      this(-1, -1, -1, -1);
    }

    public ReaderKey(long originalTransaction, int bucket, long rowId,
                     long currentTransactionId) {
      super(originalTransaction, bucket, rowId);
      this.currentTransactionId = currentTransactionId;
    }

    @Override
    public void set(RecordIdentifier other) {
      super.set(other);
      currentTransactionId = ((ReaderKey) other).currentTransactionId;
    }

    public void setValues(long originalTransactionId,
                          int bucket,
                          long rowId,
                          long currentTransactionId) {
      setValues(originalTransactionId, bucket, rowId);
      this.currentTransactionId = currentTransactionId;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) &&
          currentTransactionId == ((ReaderKey) other).currentTransactionId;
    }

    @Override
    public int compareTo(RecordIdentifier other) {
      int sup = compareToInternal(other);
      if (sup == 0) {
        if (other.getClass() == ReaderKey.class) {
          ReaderKey oth = (ReaderKey) other;
          if (currentTransactionId != oth.currentTransactionId) {
            return currentTransactionId < oth.currentTransactionId ? +1 : -1;
          }
        } else {
          return -1;
        }
      }
      return sup;
    }

    public long getCurrentTransactionId() {
      return currentTransactionId;
    }

    /**
     * Compare rows without considering the currentTransactionId.
     * @param other the value to compare to
     * @return -1, 0, +1
     */
    public int compareRow(RecordIdentifier other) {
      return compareToInternal(other);
    }

    @Override
    public String toString() {
      return "{originalTxn: " + getTransactionId() + ", bucket: " +
          getBucketId() + ", row: " + getRowId() + ", currentTxn: " +
          currentTransactionId + "}";
    }
  }

  /**
   * A reader and the next record from that reader. The code reads ahead so that
   * we can return the lowest ReaderKey from each of the readers. Thus, the
   * next available row is nextRecord and only following records are still in
   * the reader.
   */
  static class ReaderPair {
    OrcStruct nextRecord;
    final Reader reader;
    final RecordReader recordReader;
    final ReaderKey key;
    final RecordIdentifier maxKey;
    final int bucket;

    /**
     * Create a reader that reads from the first key larger than minKey to any
     * keys equal to maxKey.
     * @param key the key to read into
     * @param reader the ORC file reader
     * @param bucket the bucket number for the file
     * @param minKey only return keys larger than minKey if it is non-null
     * @param maxKey only return keys less than or equal to maxKey if it is
     *               non-null
     * @param options options to provide to read the rows.
     * @throws IOException
     */
    ReaderPair(ReaderKey key, Reader reader, int bucket,
               RecordIdentifier minKey, RecordIdentifier maxKey,
               ReaderImpl.Options options) throws IOException {
      this.reader = reader;
      this.key = key;
      this.maxKey = maxKey;
      this.bucket = bucket;
      // TODO use stripe statistics to jump over stripes
      recordReader = reader.rowsOptions(options);
      // advance the reader until we reach the minimum key
      do {
        next(nextRecord);
      } while (nextRecord != null &&
          (minKey != null && key.compareRow(minKey) <= 0));
    }

    void next(OrcStruct next) throws IOException {
      if (recordReader.hasNext()) {
        nextRecord = (OrcStruct) recordReader.next(next);
        // set the key
        key.setValues(OrcRecordUpdater.getOriginalTransaction(nextRecord),
            OrcRecordUpdater.getBucket(nextRecord),
            OrcRecordUpdater.getRowId(nextRecord),
            OrcRecordUpdater.getCurrentTransaction(nextRecord));

        // if this record is larger than maxKey, we need to stop
        if (maxKey != null && key.compareRow(maxKey) > 0) {
          LOG.debug("key " + key + " > maxkey " + maxKey);
          nextRecord = null;
          recordReader.close();
        }
      } else {
        nextRecord = null;
        recordReader.close();
      }
    }

    int getColumns() {
      return reader.getTypes().get(OrcRecordUpdater.ROW + 1).getSubtypesCount();
    }
  }

  /**
   * A reader that pretends an original base file is a new version base file.
   * It wraps the underlying reader's row with an ACID event object and
   * makes the relevant translations.
   */
  static final class OriginalReaderPair extends ReaderPair {
    OriginalReaderPair(ReaderKey key, Reader reader, int bucket,
                       RecordIdentifier minKey, RecordIdentifier maxKey,
                       Reader.Options options) throws IOException {
      super(key, reader, bucket, minKey, maxKey, options);
    }

    @Override
    void next(OrcStruct next) throws IOException {
      if (recordReader.hasNext()) {
        long nextRowId = recordReader.getRowNumber();
        // have to do initialization here, because the super's constructor
        // calls next and thus we need to initialize before our constructor
        // runs
        if (next == null) {
          nextRecord = new OrcStruct(OrcRecordUpdater.FIELDS);
          IntWritable operation =
              new IntWritable(OrcRecordUpdater.INSERT_OPERATION);
          nextRecord.setFieldValue(OrcRecordUpdater.OPERATION, operation);
          nextRecord.setFieldValue(OrcRecordUpdater.CURRENT_TRANSACTION,
              new LongWritable(0));
          nextRecord.setFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION,
              new LongWritable(0));
          nextRecord.setFieldValue(OrcRecordUpdater.BUCKET,
              new IntWritable(bucket));
          nextRecord.setFieldValue(OrcRecordUpdater.ROW_ID,
              new LongWritable(nextRowId));
          nextRecord.setFieldValue(OrcRecordUpdater.ROW,
              recordReader.next(null));
        } else {
          nextRecord = next;
          ((IntWritable) next.getFieldValue(OrcRecordUpdater.OPERATION))
              .set(OrcRecordUpdater.INSERT_OPERATION);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION))
              .set(0);
          ((IntWritable) next.getFieldValue(OrcRecordUpdater.BUCKET))
              .set(bucket);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.CURRENT_TRANSACTION))
              .set(0);
          ((LongWritable) next.getFieldValue(OrcRecordUpdater.ROW_ID))
              .set(0);
          nextRecord.setFieldValue(OrcRecordUpdater.ROW,
              recordReader.next(OrcRecordUpdater.getRow(next)));
        }
        key.setValues(0L, bucket, nextRowId, 0L);
        if (maxKey != null && key.compareRow(maxKey) > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("key " + key + " > maxkey " + maxKey);
          }
          nextRecord = null;
          recordReader.close();
        }
      } else {
        nextRecord = null;
        recordReader.close();
      }
    }

    @Override
    int getColumns() {
      return reader.getTypes().get(0).getSubtypesCount();
    }
  }

  private final TreeMap<ReaderKey, ReaderPair> readers =
      new TreeMap<ReaderKey, ReaderPair>();

  // The reader that currently has the lowest key.
  private ReaderPair primary;

  // The key of the next lowest reader.
  private ReaderKey secondaryKey = null;

  /**
   * Find the key range for original bucket files.
   * @param reader the reader
   * @param bucket the bucket number we are reading
   * @param options the options for reading with
   * @throws IOException
   */
  private void discoverOriginalKeyBounds(Reader reader, int bucket,
                                         Reader.Options options
                                         ) throws IOException {
    long rowLength = 0;
    long rowOffset = 0;
    long offset = options.getOffset();
    long maxOffset = options.getMaxOffset();
    boolean isTail = true;
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
      minKey = new RecordIdentifier(0, bucket, rowOffset - 1);
    }
    if (!isTail) {
      maxKey = new RecordIdentifier(0, bucket, rowOffset + rowLength - 1);
    }
  }

  /**
   * Find the key range for bucket files.
   * @param reader the reader
   * @param options the options for reading with
   * @throws IOException
   */
  private void discoverKeyBounds(Reader reader,
                                 Reader.Options options) throws IOException {
    RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(reader);
    long offset = options.getOffset();
    long maxOffset = options.getMaxOffset();
    int firstStripe = 0;
    int stripeCount = 0;
    boolean isTail = true;
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
  }

  /**
   * Convert from the row include/sarg/columnNames to the event equivalent
   * for the underlying file.
   * @param options options for the row reader
   * @return a cloned options object that is modified for the event reader
   */
  static Reader.Options createEventOptions(Reader.Options options) {
    Reader.Options result = options.clone();
    result.range(options.getOffset(), Long.MAX_VALUE);
    // slide the columns down by 6 for the include array
    if (options.getInclude() != null) {
      boolean[] orig = options.getInclude();
      // we always need the base row
      orig[0] = true;
      boolean[] include = new boolean[orig.length + OrcRecordUpdater.FIELDS];
      Arrays.fill(include, 0, OrcRecordUpdater.FIELDS, true);
      for(int i= 0; i < orig.length; ++i) {
        include[i + OrcRecordUpdater.FIELDS] = orig[i];
      }
      result.include(include);
    }

    // slide the column names down by 6 for the name array
    if (options.getColumnNames() != null) {
      String[] orig = options.getColumnNames();
      String[] cols = new String[orig.length + OrcRecordUpdater.FIELDS];
      for(int i=0; i < orig.length; ++i) {
        cols[i + OrcRecordUpdater.FIELDS] = orig[i];
      }
      result.searchArgument(options.getSearchArgument(), cols);
    }
    return result;
  }

  /**
   * Create a reader that merge sorts the ACID events together.
   * @param conf the configuration
   * @param collapseEvents should the events on the same row be collapsed
   * @param isOriginal is the base file a pre-acid file
   * @param bucket the bucket we are reading
   * @param options the options to read with
   * @param deltaDirectory the list of delta directories to include
   * @throws IOException
   */
  OrcRawRecordMerger(Configuration conf,
                     boolean collapseEvents,
                     Reader reader,
                     boolean isOriginal,
                     int bucket,
                     ValidTxnList validTxnList,
                     Reader.Options options,
                     Path[] deltaDirectory) throws IOException {
    this.conf = conf;
    this.collapse = collapseEvents;
    this.offset = options.getOffset();
    this.length = options.getLength();
    this.validTxnList = validTxnList;
    // modify the optins to reflect the event instead of the base row
    Reader.Options eventOptions = createEventOptions(options);
    if (reader == null) {
      baseReader = null;
    } else {

      // find the min/max based on the offset and length
      if (isOriginal) {
        discoverOriginalKeyBounds(reader, bucket, options);
      } else {
        discoverKeyBounds(reader, options);
      }
      LOG.info("min key = " + minKey + ", max key = " + maxKey);
      // use the min/max instead of the byte range
      ReaderPair pair;
      ReaderKey key = new ReaderKey();
      if (isOriginal) {
        options = options.clone();
        options.range(options.getOffset(), Long.MAX_VALUE);
        pair = new OriginalReaderPair(key, reader, bucket, minKey, maxKey,
                                      options);
      } else {
        pair = new ReaderPair(key, reader, bucket, minKey, maxKey,
                              eventOptions);
      }

      // if there is at least one record, put it in the map
      if (pair.nextRecord != null) {
        readers.put(key, pair);
      }
      baseReader = pair.recordReader;
    }

    // we always want to read all of the deltas
    eventOptions.range(0, Long.MAX_VALUE);
    if (deltaDirectory != null) {
      for(Path delta: deltaDirectory) {
        ReaderKey key = new ReaderKey();
        Path deltaFile = AcidUtils.createBucketFile(delta, bucket);
        FileSystem fs = deltaFile.getFileSystem(conf);
        long length = getLastFlushLength(fs, deltaFile);
        if (fs.exists(deltaFile) && length != -1) {
          Reader deltaReader = OrcFile.createReader(deltaFile,
              OrcFile.readerOptions(conf).maxLength(length));
          ReaderPair deltaPair = new ReaderPair(key, deltaReader, bucket, minKey,
            maxKey, eventOptions);
          if (deltaPair.nextRecord != null) {
            readers.put(key, deltaPair);
          }
        }
      }
    }

    // get the first record
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

  /**
   * Read the side file to get the last flush length.
   * @param fs the file system to use
   * @param deltaFile the path of the delta file
   * @return the maximum size of the file to use
   * @throws IOException
   */
  private static long getLastFlushLength(FileSystem fs,
                                         Path deltaFile) throws IOException {
    Path lengths = OrcRecordUpdater.getSideFile(deltaFile);
    long result = Long.MAX_VALUE;
    try {
      FSDataInputStream stream = fs.open(lengths);
      result = -1;
      while (stream.available() > 0) {
        result = stream.readLong();
      }
      stream.close();
      return result;
    } catch (IOException ioe) {
      return result;
    }
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
      OrcStruct current = primary.nextRecord;
      recordIdentifier.set(primary.key);

      // Advance the primary reader to the next record
      primary.next(extraValue);

      // Save the current record as the new extraValue for next time so that
      // we minimize allocations
      extraValue = current;

      // now that the primary reader has advanced, we need to see if we
      // continue to read it or move to the secondary.
      if (primary.nextRecord == null ||
          primary.key.compareTo(secondaryKey) > 0) {

        // if the primary isn't done, push it back into the readers
        if (primary.nextRecord != null) {
          readers.put(primary.key, primary);
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
      if (!validTxnList.isTxnCommitted(
          ((ReaderKey) recordIdentifier).getCurrentTransactionId())) {
        continue;
      }

      // if we are collapsing, figure out if this is a new row
      if (collapse) {
        keysSame = prevKey.compareRow(recordIdentifier) == 0;
        if (!keysSame) {
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

  @Override
  public RecordIdentifier createKey() {
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
    for(ReaderPair pair: readers.values()) {
      pair.recordReader.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return baseReader == null ? 1 : baseReader.getProgress();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    // Read the configuration parameters
    String columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

    // Parse the configuration parameters
    ArrayList<String> columnNames = new ArrayList<String>();
    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      Collections.addAll(columnNames, columnNameProperty.split(","));
    }
    if (columnTypeProperty == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(":");
        }
        sb.append("string");
      }
      columnTypeProperty = sb.toString();
    }

    ArrayList<TypeInfo> fieldTypes =
        TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    StructTypeInfo rowType = new StructTypeInfo();
    rowType.setAllStructFieldNames(columnNames);
    rowType.setAllStructFieldTypeInfos(fieldTypes);
    return OrcRecordUpdater.createEventSchema
        (OrcStruct.createObjectInspector(rowType));
  }

  /**
   * Get the number of columns in the underlying rows.
   * @return 0 if there are no base and no deltas.
   */
  public int getColumns() {
    return columns;
  }
}
