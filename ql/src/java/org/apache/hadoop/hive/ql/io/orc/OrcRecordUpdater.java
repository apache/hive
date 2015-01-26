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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * A RecordUpdater where the files are stored as ORC.
 */
public class OrcRecordUpdater implements RecordUpdater {

  private static final Log LOG = LogFactory.getLog(OrcRecordUpdater.class);

  public static final String ACID_KEY_INDEX_NAME = "hive.acid.key.index";
  public static final String ACID_FORMAT = "_orc_acid_version";
  public static final String ACID_STATS = "hive.acid.stats";
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
  private final Path path;
  private final FileSystem fs;
  private Writer writer;
  private final FSDataOutputStream flushLengths;
  private final OrcStruct item;
  private final IntWritable operation = new IntWritable();
  private final LongWritable currentTransaction = new LongWritable(-1);
  private final LongWritable originalTransaction = new LongWritable(-1);
  private final IntWritable bucket = new IntWritable();
  private final LongWritable rowId = new LongWritable();
  private long insertedRows = 0;
  // This records how many rows have been inserted or deleted.  It is separate from insertedRows
  // because that is monotonically increasing to give new unique row ids.
  private long rowCountDelta = 0;
  private final KeyIndexBuilder indexBuilder = new KeyIndexBuilder();
  private StructField recIdField = null; // field to look for the record identifier in
  private StructField rowIdField = null; // field inside recId to look for row id in
  private StructField originalTxnField = null;  // field inside recId to look for original txn in
  private StructObjectInspector rowInspector; // OI for the original row
  private StructObjectInspector recIdInspector; // OI for the record identifier struct
  private LongObjectInspector rowIdInspector; // OI for the long row id inside the recordIdentifier
  private LongObjectInspector origTxnInspector; // OI for the original txn inside the record
  // identifer

  static class AcidStats {
    long inserts;
    long updates;
    long deletes;

    AcidStats() {
      // nothing
    }

    AcidStats(String serialized) {
      String[] parts = serialized.split(",");
      inserts = Long.parseLong(parts[0]);
      updates = Long.parseLong(parts[1]);
      deletes = Long.parseLong(parts[2]);
    }

    String serialize() {
      StringBuilder builder = new StringBuilder();
      builder.append(inserts);
      builder.append(",");
      builder.append(updates);
      builder.append(",");
      builder.append(deletes);
      return builder.toString();
    }
  }

  static Path getSideFile(Path main) {
    return new Path(main + AcidUtils.DELTA_SIDE_FILE_SUFFIX);
  }

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
    this.bucket.set(options.getBucket());
    this.path = AcidUtils.createFilename(path, options);
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = path.getFileSystem(options.getConfiguration());
    }
    this.fs = fs;
    try {
      FSDataOutputStream strm = fs.create(new Path(path, ACID_FORMAT), false);
      strm.writeInt(ORC_ACID_VERSION);
      strm.close();
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to create " + path + "/" + ACID_FORMAT + " with " +
            ioe);
      }
    }
    if (options.getMinimumTransactionId() != options.getMaximumTransactionId()
        && !options.isWritingBase()){
      flushLengths = fs.create(getSideFile(this.path), true, 8,
          options.getReporter());
    } else {
      flushLengths = null;
    }
    OrcFile.WriterOptions writerOptions = null;
    if (options instanceof OrcOptions) {
      writerOptions = ((OrcOptions) options).getOrcOptions();
    }
    if (writerOptions == null) {
      writerOptions = OrcFile.writerOptions(options.getConfiguration());
    }
    writerOptions.fileSystem(fs).callback(indexBuilder);
    if (!options.isWritingBase()) {
      writerOptions.blockPadding(false);
      writerOptions.bufferSize(DELTA_BUFFER_SIZE);
      writerOptions.stripeSize(DELTA_STRIPE_SIZE);
    }
    rowInspector = (StructObjectInspector)options.getInspector();
    writerOptions.inspector(createEventSchema(findRecId(options.getInspector(),
        options.getRecordIdColumn())));
    this.writer = OrcFile.createWriter(this.path, writerOptions);
    item = new OrcStruct(FIELDS);
    item.setFieldValue(OPERATION, operation);
    item.setFieldValue(CURRENT_TRANSACTION, currentTransaction);
    item.setFieldValue(ORIGINAL_TRANSACTION, originalTransaction);
    item.setFieldValue(BUCKET, bucket);
    item.setFieldValue(ROW_ID, rowId);
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
      rowIdField = fields.get(2);
      rowIdInspector = (LongObjectInspector)rowIdField.getFieldObjectInspector();


      recIdInspector = (StructObjectInspector) recIdField.getFieldObjectInspector();
      return newInspector;
    }
  }

  private void addEvent(int operation, long currentTransaction, long rowId, Object row)
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
    this.rowId.set(rowId);
    this.originalTransaction.set(originalTransaction);
    item.setFieldValue(OrcRecordUpdater.ROW, (operation == DELETE_OPERATION ? null : row));
    indexBuilder.addKey(operation, originalTransaction, bucket.get(), rowId);
    writer.addRow(item);
  }

  @Override
  public void insert(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
    }
    addEvent(INSERT_OPERATION, currentTransaction, insertedRows++, row);
    rowCountDelta++;
  }

  @Override
  public void update(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
    }
    addEvent(UPDATE_OPERATION, currentTransaction, -1L, row);
  }

  @Override
  public void delete(long currentTransaction, Object row) throws IOException {
    if (this.currentTransaction.get() != currentTransaction) {
      insertedRows = 0;
    }
    addEvent(DELETE_OPERATION, currentTransaction, -1, row);
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
    } else {
      if (writer != null) writer.close();
    }
    if (flushLengths != null) {
      flushLengths.close();
      fs.delete(getSideFile(path), false);
    }
    writer = null;
  }

  @Override
  public SerDeStats getStats() {
    SerDeStats stats = new SerDeStats();
    stats.setRowCount(rowCountDelta);
    // Don't worry about setting raw data size diff.  I have no idea how to calculate that
    // without finding the row we are updating or deleting, which would be a mess.
    return stats;
  }

  @VisibleForTesting
  Writer getWriter() {
    return writer;
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
      context.getWriter().addUserMetadata(ACID_STATS,
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
