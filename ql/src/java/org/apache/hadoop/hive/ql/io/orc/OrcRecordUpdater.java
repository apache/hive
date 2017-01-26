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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A RecordUpdater where the files are stored as ORC.
 */
public class OrcRecordUpdater implements RecordUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(OrcRecordUpdater.class);

  public static final String ACID_KEY_INDEX_NAME = "hive.acid.key.index";
  public static final String ACID_FORMAT = "_orc_acid_version";
  public static final int ORC_ACID_VERSION = 0;


  final static int INSERT_OPERATION = 0;
  final static int UPDATE_OPERATION = 1;
  final static int DELETE_OPERATION = 2;

  final static int OPERATION = 0;
  final static int ORIGINAL_TRANSACTION = 1;
  final static int BUCKET = 2;
  final static int ROW_ID = 3;
  final static int CURRENT_TRANSACTION = 4;
  final static int ROW = 5;
  final static int FIELDS = 6;

  final static int DELTA_BUFFER_SIZE = 16 * 1024;
  final static long DELTA_STRIPE_SIZE = 16 * 1024 * 1024;

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final AcidOutputFormat.Options options;
  private final AcidUtils.AcidOperationalProperties acidOperationalProperties;
  private final Path path;
  private Path deleteEventPath;
  private final FileSystem fs;
  private OrcFile.WriterOptions writerOptions;
  private Writer writer = null;
  private boolean writerClosed = false;
  private Writer deleteEventWriter = null;
  private final FSDataOutputStream flushLengths;
  private final OrcStruct item;
  private final IntWritable operation = new IntWritable();
  private final LongWritable currentTransaction = new LongWritable(-1);
  private final LongWritable originalTransaction = new LongWritable(-1);
  private final IntWritable bucket = new IntWritable();
  private final LongWritable rowId = new LongWritable();
  private long insertedRows = 0;
  private long rowIdOffset = 0;
  // This records how many rows have been inserted or deleted.  It is separate from insertedRows
  // because that is monotonically increasing to give new unique row ids.
  private long rowCountDelta = 0;
  private final KeyIndexBuilder indexBuilder = new KeyIndexBuilder();
  private KeyIndexBuilder deleteEventIndexBuilder;
  private StructField recIdField = null; // field to look for the record identifier in
  private StructField rowIdField = null; // field inside recId to look for row id in
  private StructField originalTxnField = null;  // field inside recId to look for original txn in
  private StructField bucketField = null; // field inside recId to look for bucket in
  private StructObjectInspector rowInspector; // OI for the original row
  private StructObjectInspector recIdInspector; // OI for the record identifier struct
  private LongObjectInspector rowIdInspector; // OI for the long row id inside the recordIdentifier
  private LongObjectInspector origTxnInspector; // OI for the original txn inside the record
  // identifer

  static int getOperation(OrcStruct struct) {
    return ((IntWritable) struct.getFieldValue(OPERATION)).get();
  }

  static long getCurrentTransaction(OrcStruct struct) {
    return ((LongWritable) struct.getFieldValue(CURRENT_TRANSACTION)).get();
  }

  static long getOriginalTransaction(OrcStruct struct) {
    return ((LongWritable) struct.getFieldValue(ORIGINAL_TRANSACTION)).get();
  }

  static int getBucket(OrcStruct struct) {
    return ((IntWritable) struct.getFieldValue(BUCKET)).get();
  }

  static long getRowId(OrcStruct struct) {
    return ((LongWritable) struct.getFieldValue(ROW_ID)).get();
  }

  static OrcStruct getRow(OrcStruct struct) {
    if (struct == null) {
      return null;
    } else {
      return (OrcStruct) struct.getFieldValue(ROW);
    }
  }

  /**
   * An extension to AcidOutputFormat that allows users to add additional
   * options.
   */
  public static class OrcOptions extends AcidOutputFormat.Options {
    OrcFile.WriterOptions orcOptions = null;

    public OrcOptions(Configuration conf) {
      super(conf);
    }

    public OrcOptions orcOptions(OrcFile.WriterOptions opts) {
      this.orcOptions = opts;
      return this;
    }

    public OrcFile.WriterOptions getOrcOptions() {
      return orcOptions;
    }
  }

  /**
   * Create an object inspector for the ACID event based on the object inspector
   * for the underlying row.
   * @param rowInspector the row's object inspector
   * @return an object inspector for the event stream
   */
  static StructObjectInspector createEventSchema(ObjectInspector rowInspector) {
    List<StructField> fields = new ArrayList<StructField>();
    fields.add(new OrcStruct.Field("operation",
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, OPERATION));
    fields.add(new OrcStruct.Field("originalTransaction",
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        ORIGINAL_TRANSACTION));
    fields.add(new OrcStruct.Field("bucket",
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, BUCKET));
    fields.add(new OrcStruct.Field("rowId",
        PrimitiveObjectInspectorFactory.writableLongObjectInspector, ROW_ID));
    fields.add(new OrcStruct.Field("currentTransaction",
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        CURRENT_TRANSACTION));
    fields.add(new OrcStruct.Field("row", rowInspector, ROW));
    return new OrcStruct.OrcStructInspector(fields);
  }

  OrcRecordUpdater(Path path,
                   AcidOutputFormat.Options options) throws IOException {
    this.options = options;
    // Initialize acidOperationalProperties based on table properties, and
    // if they are not available, see if we can find it in the job configuration.
    // We have to look at these two places instead of just the conf, because Streaming Ingest
    // uses table properties, while normal Hive SQL inserts/updates/deletes will place this
    // value in the configuration object.
    if (options.getTableProperties() != null) {
      this.acidOperationalProperties =
          AcidUtils.getAcidOperationalProperties(options.getTableProperties());
    } else {
      this.acidOperationalProperties =
          AcidUtils.getAcidOperationalProperties(options.getConfiguration());
    }
    this.bucket.set(options.getBucket());
    this.path = AcidUtils.createFilename(path, options);
    this.deleteEventWriter = null;
    this.deleteEventPath = null;
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = path.getFileSystem(options.getConfiguration());
    }
    this.fs = fs;
    Path formatFile = new Path(path, ACID_FORMAT);
    if(!fs.exists(formatFile)) {
      try (FSDataOutputStream strm = fs.create(formatFile, false)) {
        strm.writeInt(ORC_ACID_VERSION);
      } catch (IOException ioe) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to create " + path + "/" + ACID_FORMAT + " with " +
            ioe);
        }
      }
    }
    if (options.getMinimumTransactionId() != options.getMaximumTransactionId()
        && !options.isWritingBase()){
      flushLengths = fs.create(OrcAcidUtils.getSideFile(this.path), true, 8,
          options.getReporter());
    } else {
      flushLengths = null;
    }
    this.writerOptions = null;
    // If writing delta dirs, we need to make a clone of original options, to avoid polluting it for
    // the base writer
    if (options.isWritingBase()) {
      if (options instanceof OrcOptions) {
        writerOptions = ((OrcOptions) options).getOrcOptions();
      }
      if (writerOptions == null) {
        writerOptions = OrcFile.writerOptions(options.getTableProperties(),
            options.getConfiguration());
      }
    } else {  // delta writer
      AcidOutputFormat.Options optionsCloneForDelta = options.clone();

      if (optionsCloneForDelta instanceof OrcOptions) {
        writerOptions = ((OrcOptions) optionsCloneForDelta).getOrcOptions();
      }
      if (writerOptions == null) {
        writerOptions = OrcFile.writerOptions(optionsCloneForDelta.getTableProperties(),
            optionsCloneForDelta.getConfiguration());
      }
      if (this.acidOperationalProperties.isSplitUpdate()) {
        // If this is a split-update, we initialize a delete delta file path in anticipation that
        // they would write update/delete events to that separate file.
        // This writes to a file in directory which starts with "delete_delta_..."
        // The actual initialization of a writer only happens if any delete events are written.
        this.deleteEventPath = AcidUtils.createFilename(path,
            optionsCloneForDelta.writingDeleteDelta(true));
      }

      // get buffer size and stripe size for base writer
      int baseBufferSizeValue = writerOptions.getBufferSize();
      long baseStripeSizeValue = writerOptions.getStripeSize();

      // overwrite buffer size and stripe size for delta writer, based on BASE_DELTA_RATIO
      int ratio = (int) OrcConf.BASE_DELTA_RATIO.getLong(options.getConfiguration());
      writerOptions.bufferSize(baseBufferSizeValue / ratio);
      writerOptions.stripeSize(baseStripeSizeValue / ratio);
      writerOptions.blockPadding(false);
    }
    writerOptions.fileSystem(fs).callback(indexBuilder);
    rowInspector = (StructObjectInspector)options.getInspector();
    writerOptions.inspector(createEventSchema(findRecId(options.getInspector(),
        options.getRecordIdColumn())));
    item = new OrcStruct(FIELDS);
    item.setFieldValue(OPERATION, operation);
    item.setFieldValue(CURRENT_TRANSACTION, currentTransaction);
    item.setFieldValue(ORIGINAL_TRANSACTION, originalTransaction);
    item.setFieldValue(BUCKET, bucket);
    item.setFieldValue(ROW_ID, rowId);
  }

  @Override
  public String toString() {
    return getClass().getName() + "[" + path +"]";
  }
  /**
   * To handle multiple INSERT... statements in a single transaction, we want to make sure
   * to generate unique {@code rowId} for all inserted rows of the transaction.
   * @return largest rowId created by previous statements (maybe 0)
   * @throws IOException
   */
  private long findRowIdOffsetForInsert() throws IOException {
    /*
    * 1. need to know bucket we are writing to
    * 2. need to know which delta dir it's in
    * Then,
    * 1. find the same bucket file in previous (insert) delta dir for this txn
    *    (Note: in case of split_update, we can ignore the delete_delta dirs)
    * 2. read the footer and get AcidStats which has insert count
     * 2.1 if AcidStats.inserts>0 add to the insert count.
     *  else go to previous delta file
     *  For example, consider insert/update/insert case...*/
    if(options.getStatementId() <= 0) {
      return 0;//there is only 1 statement in this transaction (so far)
    }
    long totalInserts = 0;
    for(int pastStmt = options.getStatementId() - 1; pastStmt >= 0; pastStmt--) {
      Path matchingBucket = AcidUtils.createFilename(options.getFinalDestination(), options.clone().statementId(pastStmt));
      if(!fs.exists(matchingBucket)) {
        continue;
      }
      Reader reader = OrcFile.createReader(matchingBucket, OrcFile.readerOptions(options.getConfiguration()));
      //no close() on Reader?!
      AcidStats acidStats = OrcAcidUtils.parseAcidStats(reader);
      if(acidStats.inserts > 0) {
        totalInserts += acidStats.inserts;
      }
    }
    return totalInserts;
  }
  // Find the record identifier column (if there) and return a possibly new ObjectInspector that
  // will strain out the record id for the underlying writer.
  private ObjectInspector findRecId(ObjectInspector inspector, int rowIdColNum) {
    if (!(inspector instanceof StructObjectInspector)) {
      throw new RuntimeException("Serious problem, expected a StructObjectInspector, but got a " +
          inspector.getClass().getName());
    }
    if (rowIdColNum < 0) {
      return inspector;
    } else {
      RecIdStrippingObjectInspector newInspector =
          new RecIdStrippingObjectInspector(inspector, rowIdColNum);
      recIdField = newInspector.getRecId();
      List<? extends StructField> fields =
          ((StructObjectInspector) recIdField.getFieldObjectInspector()).getAllStructFieldRefs();
      // Go by position, not field name, as field names aren't guaranteed.  The order of fields
      // in RecordIdentifier is transactionId, bucketId, rowId
      originalTxnField = fields.get(0);
      origTxnInspector = (LongObjectInspector)originalTxnField.getFieldObjectInspector();
      bucketField = fields.get(1);
      rowIdField = fields.get(2);
      rowIdInspector = (LongObjectInspector)rowIdField.getFieldObjectInspector();


      recIdInspector = (StructObjectInspector) recIdField.getFieldObjectInspector();
      return newInspector;
    }
  }

  private void addSimpleEvent(int operation, long currentTransaction, long rowId, Object row)
      throws IOException {
    this.operation.set(operation);
    this.currentTransaction.set(currentTransaction);
    // If this is an insert, originalTransaction should be set to this transaction.  If not,
    // it will be reset by the following if anyway.
    long originalTransaction = currentTransaction;
    if (operation == DELETE_OPERATION || operation == UPDATE_OPERATION) {
      Object rowIdValue = rowInspector.getStructFieldData(row, recIdField);
      originalTransaction = origTxnInspector.get(
          recIdInspector.getStructFieldData(rowIdValue, originalTxnField));
      rowId = rowIdInspector.get(recIdInspector.getStructFieldData(rowIdValue, rowIdField));
    }
    else if(operation == INSERT_OPERATION) {
      rowId += rowIdOffset;
    }
    this.rowId.set(rowId);
    this.originalTransaction.set(originalTransaction);
    item.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(operation));
    item.setFieldValue(OrcRecordUpdater.ROW, (operation == DELETE_OPERATION ? null : row));
    indexBuilder.addKey(operation, originalTransaction, bucket.get(), rowId);
    if (writer == null) {
      writer = OrcFile.createWriter(path, writerOptions);
    }
    writer.addRow(item);
  }

  private void addSplitUpdateEvent(int operation, long currentTransaction, long rowId, Object row)
      throws IOException {
    if (operation == INSERT_OPERATION) {
      // Just insert the record in the usual way, i.e., default to the simple behavior.
      addSimpleEvent(operation, currentTransaction, rowId, row);
      return;
    }
    this.operation.set(operation);
    this.currentTransaction.set(currentTransaction);
    Object rowValue = rowInspector.getStructFieldData(row, recIdField);
    long originalTransaction = origTxnInspector.get(
            recIdInspector.getStructFieldData(rowValue, originalTxnField));
    rowId = rowIdInspector.get(
            recIdInspector.getStructFieldData(rowValue, rowIdField));

    if (operation == DELETE_OPERATION || operation == UPDATE_OPERATION) {
      // Initialize a deleteEventWriter if not yet done. (Lazy initialization)
      if (deleteEventWriter == null) {
        // Initialize an indexBuilder for deleteEvents.
        deleteEventIndexBuilder = new KeyIndexBuilder();
        // Change the indexBuilder callback too for the deleteEvent file, the remaining writer
        // options remain the same.

        // TODO: When we change the callback, we are essentially mutating the writerOptions.
        // This works but perhaps is not a good thing. The proper way to do this would be
        // to clone the writerOptions, however it requires that the parent OrcFile.writerOptions
        // implements a clone() method (which it does not for now). HIVE-14514 is currently an open
        // JIRA to fix this.

        this.deleteEventWriter = OrcFile.createWriter(deleteEventPath,
                                                      writerOptions.callback(deleteEventIndexBuilder));
      }

      // A delete/update generates a delete event for the original row.
      this.rowId.set(rowId);
      this.originalTransaction.set(originalTransaction);
      item.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(DELETE_OPERATION));
      item.setFieldValue(OrcRecordUpdater.ROW, null); // ROW is null for delete events.
      deleteEventIndexBuilder.addKey(DELETE_OPERATION, originalTransaction, bucket.get(), rowId);
      deleteEventWriter.addRow(item);
    }

    if (operation == UPDATE_OPERATION) {
      // A new row is also inserted in the usual delta file for an update event.
      addSimpleEvent(INSERT_OPERATION, currentTransaction, insertedRows++, row);
    }
  }

  @Override
  public void insert(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
      //this method is almost no-op in hcatalog.streaming case since statementId == 0 is
      //always true in that case
      rowIdOffset = findRowIdOffsetForInsert();
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(INSERT_OPERATION, currentTransaction, insertedRows++, row);
    } else {
      addSimpleEvent(INSERT_OPERATION, currentTransaction, insertedRows++, row);
    }
    rowCountDelta++;
  }

  @Override
  public void update(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
      rowIdOffset = findRowIdOffsetForInsert();
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(UPDATE_OPERATION, currentTransaction, -1L, row);
    } else {
      addSimpleEvent(UPDATE_OPERATION, currentTransaction, -1L, row);
    }
  }

  @Override
  public void delete(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(DELETE_OPERATION, currentTransaction, -1L, row);
    } else {
      addSimpleEvent(DELETE_OPERATION, currentTransaction, -1L, row);
    }
    rowCountDelta--;
  }

  @Override
  public void flush() throws IOException {
    // We only support flushes on files with multiple transactions, because
    // flushes create significant overhead in HDFS. Record updaters with a
    // single transaction should be closed rather than flushed.
    if (flushLengths == null) {
      throw new IllegalStateException("Attempting to flush a RecordUpdater on "
         + path + " with a single transaction.");
    }
    if (writer == null) {
      writer = OrcFile.createWriter(path, writerOptions);
    }
    long len = writer.writeIntermediateFooter();
    flushLengths.writeLong(len);
    OrcInputFormat.SHIMS.hflush(flushLengths);
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort) {
      if (flushLengths == null) {
        fs.delete(path, false);
      }
    } else if (!writerClosed) {
      if (acidOperationalProperties.isSplitUpdate()) {
        // When split-update is enabled, we can choose not to write
        // any delta files when there are no inserts. In such cases only the delete_deltas
        // would be written & they are closed separately below.
        if (writer != null && indexBuilder.acidStats.inserts > 0) {
          writer.close(); // normal close, when there are inserts.
        }
      } else {
        if (writer == null) {
          writer = OrcFile.createWriter(path, writerOptions);
        }
        writer.close(); // normal close.
      }
      if (deleteEventWriter != null) {
        if (deleteEventIndexBuilder.acidStats.deletes > 0) {
          // Only need to write out & close the delete_delta if there have been any.
          deleteEventWriter.close();
        } else {
          // Just remove delete_delta, if there have been no delete events.
          fs.delete(deleteEventPath, false);
        }
      }
    }
    if (flushLengths != null) {
      flushLengths.close();
      fs.delete(OrcAcidUtils.getSideFile(path), false);
    }
    writer = null;
    deleteEventWriter = null;
    writerClosed = true;
  }

  @Override
  public SerDeStats getStats() {
    SerDeStats stats = new SerDeStats();
    stats.setRowCount(rowCountDelta);
    // Don't worry about setting raw data size diff.  I have no idea how to calculate that
    // without finding the row we are updating or deleting, which would be a mess.
    return stats;
  }

  private static final Charset utf8 = Charset.forName("UTF-8");
  private static final CharsetDecoder utf8Decoder = utf8.newDecoder();

  static RecordIdentifier[] parseKeyIndex(Reader reader) {
    String[] stripes;
    try {
      ByteBuffer val =
          reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)
              .duplicate();
      stripes = utf8Decoder.decode(val).toString().split(";");
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("Bad string encoding for " +
          OrcRecordUpdater.ACID_KEY_INDEX_NAME, e);
    }
    RecordIdentifier[] result = new RecordIdentifier[stripes.length];
    for(int i=0; i < stripes.length; ++i) {
      if (stripes[i].length() != 0) {
        String[] parts = stripes[i].split(",");
        result[i] = new RecordIdentifier();
        result[i].setValues(Long.parseLong(parts[0]),
            Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
      }
    }
    return result;
  }

  static class KeyIndexBuilder implements OrcFile.WriterCallback {
    StringBuilder lastKey = new StringBuilder();
    long lastTransaction;
    int lastBucket;
    long lastRowId;
    AcidStats acidStats = new AcidStats();

    @Override
    public void preStripeWrite(OrcFile.WriterContext context
    ) throws IOException {
      lastKey.append(lastTransaction);
      lastKey.append(',');
      lastKey.append(lastBucket);
      lastKey.append(',');
      lastKey.append(lastRowId);
      lastKey.append(';');
    }

    @Override
    public void preFooterWrite(OrcFile.WriterContext context
                               ) throws IOException {
      context.getWriter().addUserMetadata(ACID_KEY_INDEX_NAME,
          UTF8.encode(lastKey.toString()));
      context.getWriter().addUserMetadata(OrcAcidUtils.ACID_STATS,
          UTF8.encode(acidStats.serialize()));
    }

    void addKey(int op, long transaction, int bucket, long rowId) {
      switch (op) {
        case INSERT_OPERATION:
          acidStats.inserts += 1;
          break;
        case UPDATE_OPERATION:
          acidStats.updates += 1;
          break;
        case DELETE_OPERATION:
          acidStats.deletes += 1;
          break;
        default:
          throw new IllegalArgumentException("Unknown operation " + op);
      }
      lastTransaction = transaction;
      lastBucket = bucket;
      lastRowId = rowId;
    }
  }

  /**
   * An ObjectInspector that will strip out the record identifier so that the underlying writer
   * doesn't see it.
   */
  private static class RecIdStrippingObjectInspector extends StructObjectInspector {
    private StructObjectInspector wrapped;
    List<StructField> fields;
    StructField recId;

    RecIdStrippingObjectInspector(ObjectInspector oi, int rowIdColNum) {
      if (!(oi instanceof StructObjectInspector)) {
        throw new RuntimeException("Serious problem, expected a StructObjectInspector, " +
            "but got a " + oi.getClass().getName());
      }
      wrapped = (StructObjectInspector)oi;
      List<? extends StructField> wrappedFields = wrapped.getAllStructFieldRefs();
      fields = new ArrayList<StructField>(wrapped.getAllStructFieldRefs().size());
      for (int i = 0; i < wrappedFields.size(); i++) {
        if (i == rowIdColNum) {
          recId = wrappedFields.get(i);
        } else {
          fields.add(wrappedFields.get(i));
        }
      }
    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
      return wrapped.getStructFieldRef(fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      // For performance don't check that that the fieldRef isn't recId everytime,
      // just assume that the caller used getAllStructFieldRefs and thus doesn't have that fieldRef
      return wrapped.getStructFieldData(data, fieldRef);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      return wrapped.getStructFieldsDataAsList(data);
    }

    @Override
    public String getTypeName() {
      return wrapped.getTypeName();
    }

    @Override
    public Category getCategory() {
      return wrapped.getCategory();
    }

    StructField getRecId() {
      return recId;
    }
  }
}
