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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.WriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * A RecordUpdater where the files are stored as ORC.
 * A note on various record structures: the {@code row} coming in (as in {@link #insert(long, Object)}
 * for example), is a struct like &lt;RecordIdentifier, f1, ... fn&gt; but what is written to the file
 * * is &lt;op, owid, writerId, rowid, cwid, &lt;f1, ... fn&gt;&gt; (see {@link #createEventObjectInspector(ObjectInspector)})
 * So there are OIs here to make the translation.
 */
public class OrcRecordUpdater implements RecordUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(OrcRecordUpdater.class);

  static final String ACID_KEY_INDEX_NAME = "hive.acid.key.index";

  final static int INSERT_OPERATION = 0;
  final static int UPDATE_OPERATION = 1;
  final static int DELETE_OPERATION = 2;
  //column indexes of corresponding data in storage layer
  final static int OPERATION = 0;
  final static int ORIGINAL_WRITEID = 1;
  final static int BUCKET = 2;
  final static int ROW_ID = 3;
  final static int CURRENT_WRITEID = 4;
  public static final int ROW = 5;
  static final String OPERATION_FIELD_NAME = "operation";
  static final String ORIGINAL_WRITEID_FIELD_NAME = "originalTransaction";
  static final String BUCKET_FIELD_NAME = "bucket";
  static final String ROW_ID_FIELD_NAME = "rowId";
  static final String CURRENT_WRITEID_FIELD_NAME = "currentTransaction";
  static final String ROW_FIELD_NAME = "row";
  public static final Collection ALL_ACID_ROW_NAMES = Arrays.asList(
      OrcRecordUpdater.BUCKET_FIELD_NAME,
      OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME,
      OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME,
      OrcRecordUpdater.OPERATION_FIELD_NAME,
      OrcRecordUpdater.ROW_FIELD_NAME,
      OrcRecordUpdater.ROW_ID_FIELD_NAME);

  /**
   * total number of fields (above)
   */
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
  private OrcFile.WriterOptions deleteWriterOptions;
  private Writer writer = null;
  private boolean writerClosed = false;
  private Writer deleteEventWriter = null;
  private final FSDataOutputStream flushLengths;
  private final OrcStruct item;
  private final IntWritable operation = new IntWritable();
  private final LongWritable currentWriteId = new LongWritable(-1);
  private final LongWritable originalWriteId = new LongWritable(-1);
  private final IntWritable bucket = new IntWritable();
  private final LongWritable rowId = new LongWritable();
  private long insertedRows = 0;
  // This records how many rows have been inserted or deleted.  It is separate from insertedRows
  // because that is monotonically increasing to give new unique row ids.
  private long rowCountDelta = 0;
  private long insertCount = 0;
  private long updateCount = 0;
  private long deleteCount = 0;
  // used only for insert events, this is the number of rows held in memory before flush() is invoked
  private long bufferedRows = 0;
  private final KeyIndexBuilder indexBuilder = new KeyIndexBuilder();
  private KeyIndexBuilder deleteEventIndexBuilder;
  private StructField recIdField = null; // field to look for the record identifier in
  private StructField rowIdField = null; // field inside recId to look for row id in
  private StructField originalWriteIdField = null;  // field inside recId to look for original write id in
  private StructField bucketField = null; // field inside recId to look for bucket in
  private StructObjectInspector rowInspector; // OI for the original row
  private StructObjectInspector recIdInspector; // OI for the record identifier struct
  private LongObjectInspector rowIdInspector; // OI for the long row id inside the recordIdentifier
  private LongObjectInspector origWriteIdInspector; // OI for the original write id inside the record
  // identifer
  private IntObjectInspector bucketInspector;

  static int getOperation(OrcStruct struct) {
    return ((IntWritable) struct.getFieldValue(OPERATION)).get();
  }

  static long getCurrentTransaction(OrcStruct struct) {
    return ((LongWritable) struct.getFieldValue(CURRENT_WRITEID)).get();
  }

  static long getOriginalTransaction(OrcStruct struct) {
    return ((LongWritable) struct.getFieldValue(ORIGINAL_WRITEID)).get();
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
   *
   * todo: since this is only used for testing could we not control the writer some other way?
   * to simplify {@link #OrcRecordUpdater(Path, AcidOutputFormat.Options)}
   */
   final static class OrcOptions extends AcidOutputFormat.Options {
    OrcFile.WriterOptions orcOptions = null;

    OrcOptions(Configuration conf) {
      super(conf);
    }

    OrcOptions orcOptions(OrcFile.WriterOptions opts) {
      this.orcOptions = opts;
      return this;
    }

    OrcFile.WriterOptions getOrcOptions() {
      return orcOptions;
    }
  }

  /**
   * Create an object inspector for the ACID event based on the object inspector
   * for the underlying row.
   * @param rowInspector the row's object inspector
   * @return an object inspector for the event stream
   */
  static StructObjectInspector createEventObjectInspector(ObjectInspector rowInspector) {
    List<StructField> fields = new ArrayList<StructField>();
    fields.add(new OrcStruct.Field(OPERATION_FIELD_NAME,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, OPERATION));
    fields.add(new OrcStruct.Field(ORIGINAL_WRITEID_FIELD_NAME,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector, ORIGINAL_WRITEID));
    fields.add(new OrcStruct.Field(BUCKET_FIELD_NAME,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, BUCKET));
    fields.add(new OrcStruct.Field(ROW_ID_FIELD_NAME,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector, ROW_ID));
    fields.add(new OrcStruct.Field(CURRENT_WRITEID_FIELD_NAME,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector, CURRENT_WRITEID));
    fields.add(new OrcStruct.Field(ROW_FIELD_NAME, rowInspector, ROW));
    return new OrcStruct.OrcStructInspector(fields);
  }

  private static TypeDescription createEventSchemaFromTableProperties(Properties tableProps) {
    TypeDescription rowSchema = getTypeDescriptionFromTableProperties(tableProps);
    if (rowSchema == null) {
      return null;
    }

    return SchemaEvolution.createEventSchema(rowSchema);
  }

  private static TypeDescription getTypeDescriptionFromTableProperties(Properties tableProperties) {
    TypeDescription schema = null;
    if (tableProperties != null) {
      final String columnNameProperty = tableProperties.getProperty(IOConstants.COLUMNS);
      final String columnTypeProperty = tableProperties.getProperty(IOConstants.COLUMNS_TYPES);
      if (!Strings.isNullOrEmpty(columnNameProperty) && !Strings.isNullOrEmpty(columnTypeProperty)) {
        List<String> columnNames =
          columnNameProperty.length() == 0 ? new ArrayList<String>() : Arrays.asList(columnNameProperty.split(","));
        List<TypeInfo> columnTypes = columnTypeProperty.length() == 0 ? new ArrayList<TypeInfo>() : TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);

        schema = TypeDescription.createStruct();
        for (int i = 0; i < columnNames.size(); i++) {
          schema.addField(columnNames.get(i), OrcInputFormat.convertTypeInfo(columnTypes.get(i)));
        }
      }
    }

    LOG.debug("ORC schema = {}", schema);

    return schema;
  }

  /**
   * @param partitionRoot - partition root (or table root if not partitioned)
   */
  OrcRecordUpdater(Path partitionRoot,
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
    assert this.acidOperationalProperties.isSplitUpdate() : "HIVE-17089?!";
    BucketCodec bucketCodec = BucketCodec.V1;
    if(options.getConfiguration() != null) {
      //so that we can test "old" files
      Configuration hc = options.getConfiguration();
      if(hc.getBoolean(HiveConf.ConfVars.HIVE_IN_TEST.name(), false) ||
        hc.getBoolean(HiveConf.ConfVars.HIVE_IN_TEZ_TEST.name(), false)) {
        bucketCodec = BucketCodec.getCodec(
          hc.getInt(HiveConf.ConfVars.TESTMODE_BUCKET_CODEC_VERSION.name(),
            BucketCodec.V1.getVersion()));
      }
    }
    this.bucket.set(bucketCodec.encode(options)); // encode might chang bucketId/stmtId, use the new one for file name generation
    if (options.getStatementId() != -1) { // -1 is treated specially in createFilename during compaction
      options.statementId(bucketCodec.decodeStatementId(this.bucket.get()));
    }
    options.bucket(bucketCodec.decodeWriterId(this.bucket.get()));
    this.path = AcidUtils.createFilename(partitionRoot, options);
    this.deleteEventWriter = null;
    this.deleteEventPath = null;
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = partitionRoot.getFileSystem(options.getConfiguration());
    }
    this.fs = fs;
    if (options.getMinimumWriteId() != options.getMaximumWriteId() && !options.isWritingBase()) {
      //throw if file already exists as that should never happen
      flushLengths = fs.create(OrcAcidUtils.getSideFile(this.path), false, 8,
          options.getReporter());
      flushLengths.writeLong(0);
      OrcInputFormat.SHIMS.hflush(flushLengths);
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
        AcidOutputFormat.Options deleteOptions = options.clone().writingDeleteDelta(true);
        // If this is a split-update, we initialize a delete delta file path in anticipation that
        // they would write update/delete events to that separate file.
        // This writes to a file in directory which starts with "delete_delta_..."
        // The actual initialization of a writer only happens if any delete events are written
        //to avoid empty files.
        this.deleteEventPath = AcidUtils.createFilename(partitionRoot, deleteOptions);
        this.deleteWriterOptions = writerOptions
                .clone()
                .inspector(createEventObjectInspector(findRecId(options.getInspector(),
                        options.getRecordIdColumn())))
                .setSchema(createEventSchemaFromTableProperties(options.getTableProperties()));
      }

      // get buffer size and stripe size for base writer
      int baseBufferSizeValue = writerOptions.getBufferSize();
      long baseStripeSizeValue = writerOptions.getStripeSize();

      // overwrite buffer size and stripe size for delta writer, based on BASE_DELTA_RATIO
      int ratio = (int) OrcConf.BASE_DELTA_RATIO.getLong(options.getConfiguration());
      writerOptions.bufferSize(baseBufferSizeValue / ratio);
      writerOptions.stripeSize(baseStripeSizeValue / ratio);
      writerOptions.blockPadding(false);
      if (HiveConf.getBoolVar(optionsCloneForDelta.getConfiguration(),
              HiveConf.ConfVars.HIVE_ORC_DELTA_STREAMING_OPTIMIZATIONS_ENABLED) || options.isTemporary()) {
        writerOptions.encodingStrategy(org.apache.orc.OrcFile.EncodingStrategy.SPEED);
        writerOptions.rowIndexStride(0);
        writerOptions.getConfiguration().set(OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute(), "-1.0");
      }
    }
    if(!HiveConf.getBoolVar(options.getConfiguration(), HiveConf.ConfVars.HIVE_TEST_MODE_ACID_KEY_IDX_SKIP)) {
      writerOptions.fileSystem(fs).callback(indexBuilder);
    }
    rowInspector = (StructObjectInspector)options.getInspector();
    writerOptions.inspector(createEventObjectInspector(findRecId(options.getInspector(),
        options.getRecordIdColumn())));
    writerOptions.setSchema(createEventSchemaFromTableProperties(options.getTableProperties()));
    item = new OrcStruct(FIELDS);
    item.setFieldValue(OPERATION, operation);
    item.setFieldValue(CURRENT_WRITEID, currentWriteId);
    item.setFieldValue(ORIGINAL_WRITEID, originalWriteId);
    item.setFieldValue(BUCKET, bucket);
    item.setFieldValue(ROW_ID, rowId);
  }
  @Override
  public String toString() {
    return getClass().getName() + "[" + path +"]";
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
      // in RecordIdentifier is writeId, bucketId, rowId
      originalWriteIdField = fields.get(0);
      origWriteIdInspector = (LongObjectInspector)originalWriteIdField.getFieldObjectInspector();
      bucketField = fields.get(1);
      bucketInspector = (IntObjectInspector) bucketField.getFieldObjectInspector();
      rowIdField = fields.get(2);
      rowIdInspector = (LongObjectInspector)rowIdField.getFieldObjectInspector();


      recIdInspector = (StructObjectInspector) recIdField.getFieldObjectInspector();
      return newInspector;
    }
  }

  /**
   * The INSERT event always uses {@link #bucket} that this {@link RecordUpdater} was created with
   * thus even for unbucketed tables, the N in bucket_N file name matches writerId/bucketId even for
   * late split
   */
  private void addSimpleEvent(int operation, long currentWriteId, long rowId, Object row)
      throws IOException {
    this.operation.set(operation);
    this.currentWriteId.set(currentWriteId);
    Integer currentBucket = null;
    // If this is an insert, originalWriteId should be set to this transaction.  If not,
    // it will be reset by the following if anyway.
    long originalWriteId = currentWriteId;
    if (operation == DELETE_OPERATION || operation == UPDATE_OPERATION) {
      Object rowIdValue = rowInspector.getStructFieldData(row, recIdField);
      originalWriteId = origWriteIdInspector.get(
          recIdInspector.getStructFieldData(rowIdValue, originalWriteIdField));
      rowId = rowIdInspector.get(recIdInspector.getStructFieldData(rowIdValue, rowIdField));
      currentBucket = setBucket(bucketInspector.get(
        recIdInspector.getStructFieldData(rowIdValue, bucketField)), operation);
    }
    this.rowId.set(rowId);
    this.originalWriteId.set(originalWriteId);
    item.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(operation));
    item.setFieldValue(OrcRecordUpdater.ROW, (operation == DELETE_OPERATION ? null : row));
    indexBuilder.addKey(operation, originalWriteId, bucket.get(), rowId);
    initWriter();
    writer.addRow(item);
    restoreBucket(currentBucket, operation);
  }

  private void addSplitUpdateEvent(int operation, long currentWriteId, long rowId, Object row)
      throws IOException {
    if (operation == INSERT_OPERATION) {
      // Just insert the record in the usual way, i.e., default to the simple behavior.
      addSimpleEvent(operation, currentWriteId, rowId, row);
      return;
    }
    this.operation.set(operation);
    this.currentWriteId.set(currentWriteId);
    Object rowValue = rowInspector.getStructFieldData(row, recIdField);
    long originalWriteId = origWriteIdInspector.get(
            recIdInspector.getStructFieldData(rowValue, originalWriteIdField));
    rowId = rowIdInspector.get(
            recIdInspector.getStructFieldData(rowValue, rowIdField));
    Integer currentBucket = null;

    if (operation == DELETE_OPERATION || operation == UPDATE_OPERATION) {
      /**
       * make sure bucketProperty in the delete event is from the {@link row} rather than whatever
       * {@link this#bucket} is.  For bucketed tables, the 2 must agree on bucketId encoded in it
       * not for necessarily the whole value.  For unbucketed tables there is no relationship.
       */
      currentBucket = setBucket(bucketInspector.get(
        recIdInspector.getStructFieldData(rowValue, bucketField)), operation);
      // Initialize a deleteEventWriter if not yet done. (Lazy initialization)
      if (deleteEventWriter == null) {
        // Initialize an indexBuilder for deleteEvents. (HIVE-17284)
        deleteEventIndexBuilder = new KeyIndexBuilder();
        this.deleteEventWriter = OrcFile.createWriter(deleteEventPath,
            deleteWriterOptions.callback(deleteEventIndexBuilder));
        AcidUtils.OrcAcidVersion.setAcidVersionInDataFile(deleteEventWriter);
        if (options.isWriteVersionFile()) {
          AcidUtils.OrcAcidVersion.writeVersionFile(this.deleteEventPath.getParent(), fs);
        }
      }

      // A delete/update generates a delete event for the original row.
      this.rowId.set(rowId);
      this.originalWriteId.set(originalWriteId);
      item.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(DELETE_OPERATION));
      item.setFieldValue(OrcRecordUpdater.ROW, null); // ROW is null for delete events.
      deleteEventIndexBuilder.addKey(DELETE_OPERATION, originalWriteId, bucket.get(), rowId);
      deleteEventWriter.addRow(item);
      restoreBucket(currentBucket, operation);
    }

    if (operation == UPDATE_OPERATION) {
      // A new row is also inserted in the usual delta file for an update event.
      addSimpleEvent(INSERT_OPERATION, currentWriteId, insertedRows++, row);
    }
  }

  @Override
  public void insert(long currentWriteId, Object row) throws IOException {
    if (this.currentWriteId.get() != currentWriteId) {
      insertedRows = 0;
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(INSERT_OPERATION, currentWriteId, insertedRows++, row);
    } else {
      addSimpleEvent(INSERT_OPERATION, currentWriteId, insertedRows++, row);
    }
    rowCountDelta++;
    insertCount++;
    bufferedRows++;
  }

  @Override
  public void update(long currentWriteId, Object row) throws IOException {
    if (this.currentWriteId.get() != currentWriteId) {
      insertedRows = 0;
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(UPDATE_OPERATION, currentWriteId, -1L, row);
    } else {
      addSimpleEvent(UPDATE_OPERATION, currentWriteId, -1L, row);
    }
    updateCount++;
  }

  @Override
  public void delete(long currentWriteId, Object row) throws IOException {
    if (this.currentWriteId.get() != currentWriteId) {
      insertedRows = 0;
    }
    if (acidOperationalProperties.isSplitUpdate()) {
      addSplitUpdateEvent(DELETE_OPERATION, currentWriteId, -1L, row);
    } else {
      addSimpleEvent(DELETE_OPERATION, currentWriteId, -1L, row);
    }
    deleteCount++;
    rowCountDelta--;
  }

  @Override
  public void flush() throws IOException {
    initWriter();
    // streaming ingest writer with single transaction batch size, in which case the transaction is
    // either committed or aborted. In either cases we don't need flush length file but we need to
    // flush intermediate footer to reduce memory pressure. Also with HIVE-19206, streaming writer does
    // automatic memory management which would require flush of open files without actually closing it.
    if (flushLengths == null) {
      // transaction batch size = 1 case
      writer.writeIntermediateFooter();
    } else {
      // transaction batch size > 1 case
      long len = writer.writeIntermediateFooter();
      flushLengths.writeLong(len);
      OrcInputFormat.SHIMS.hflush(flushLengths);
    }
    bufferedRows = 0;
    //multiple transactions only happen for streaming ingest which only allows inserts
    assert deleteEventWriter == null : "unexpected delete writer for " + path;
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort) {
      if (flushLengths == null) {
        LOG.debug("Close on abort for path: {}.. Deleting..", path);
        fs.delete(path, false);
      }
    } else if (!writerClosed) {
      if (acidOperationalProperties.isSplitUpdate()) {
        // When split-update is enabled, we can choose not to write
        // any delta files when there are no inserts. In such cases only the delete_deltas
        // would be written & they are closed separately below.
        if (indexBuilder.acidStats.inserts > 0) {
          if (writer != null) {
            LOG.debug("Closing writer for path: {} acid stats: {}", path, indexBuilder.acidStats);
            writer.close(); // normal close, when there are inserts.
          }
        } else {
          if (options.isWritingBase()) {
            // With insert overwrite we need the empty file to delete the previous content of the table
            LOG.debug("Empty file has been created for overwrite: {}", path);
            OrcFile.createWriter(path, writerOptions).close();
          } else {
            LOG.debug("No insert events in path: {}.. Deleting..", path);
            fs.delete(path, false);
          }
        }
      } else {
        //so that we create empty bucket files when needed (but see HIVE-17138)
        LOG.debug("Initializing writer before close (to create empty buckets) for path: {}", path);
        initWriter();
        writer.close(); // normal close.
      }
      if (deleteEventWriter != null) {
        if (deleteEventIndexBuilder.acidStats.deletes > 0) {
          LOG.debug("Closing delete event writer for path: {} acid stats: {}", path, indexBuilder.acidStats);
          // Only need to write out & close the delete_delta if there have been any.
          deleteEventWriter.close();
        } else {
          LOG.debug("No delete events in path: {}.. Deleting..", path);
          // Just remove delete_delta, if there have been no delete events.
          fs.delete(deleteEventPath, false);
        }
      }
    }
    if (flushLengths != null) {
      LOG.debug("Closing and deleting flush length file for path: {}", path);
      flushLengths.close();
      fs.delete(OrcAcidUtils.getSideFile(path), false);
    }
    writer = null;
    deleteEventWriter = null;
    writerClosed = true;
  }
  private void initWriter() throws IOException {
    if (writer == null) {
      writer = OrcFile.createWriter(path, writerOptions);
      AcidUtils.OrcAcidVersion.setAcidVersionInDataFile(writer);
      if (options.isWriteVersionFile()) {
        try {
          AcidUtils.OrcAcidVersion.writeVersionFile(path.getParent(), fs);
        } catch (Exception e) {
          LOG.trace("Ignore; might have been created by another concurrent writer, writing to a"
                        + " different bucket within this delta/base directory", e);
        }
      }
    }
  }

  @Override
  public SerDeStats getStats() {
    SerDeStats stats = new SerDeStats();
    stats.setRowCount(rowCountDelta);
    stats.setInsertCount(insertCount);
    stats.setUpdateCount(updateCount);
    stats.setDeleteCount(deleteCount);
    // Don't worry about setting raw data size diff.  I have no idea how to calculate that
    // without finding the row we are updating or deleting, which would be a mess.
    return stats;
  }

  @Override
  public long getBufferedRowCount() {
    return bufferedRows;
  }

  static RecordIdentifier[] parseKeyIndex(Reader reader) {
    if (!reader.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)) {
      return null;
    }

    ByteBuffer val =
        reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)
            .duplicate();
    return parseKeyIndex(val);
  }

  static RecordIdentifier[] parseKeyIndex(OrcTail orcTail) {
    for(OrcProto.UserMetadataItem item: orcTail.getFooter().getMetadataList()) {
      if (item.hasName() && item.getName().equals(OrcRecordUpdater.ACID_KEY_INDEX_NAME)) {
        return parseKeyIndex(item.getValue().asReadOnlyByteBuffer().duplicate());
      }
    }
    return null;
  }

  private static RecordIdentifier[] parseKeyIndex(ByteBuffer val) {
    String[] stripes;
    try {
      CharsetDecoder utf8Decoder = UTF8.newDecoder();
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
    StringBuilder lastKey = new StringBuilder();//list of last keys for each stripe
    long lastTransaction;
    int lastBucket;
    long lastRowId;
    AcidStats acidStats = new AcidStats();
    /**
     *  {@link #preStripeWrite(OrcFile.WriterContext)} is normally called by the
     *  {@link org.apache.orc.MemoryManager} except on close().
     *  {@link org.apache.orc.impl.WriterImpl#close()} calls preFooterWrite() before it calls
     *  {@link WriterImpl#flushStripe()} which causes the {@link #ACID_KEY_INDEX_NAME} index to
     *  have the last entry missing.  It should be also fixed in ORC but that requires upgrading
     *  the ORC jars to have effect.
     *
     *  This is used to decide if we need to make preStripeWrite() call here.
     */
    protected long numKeysCurrentStripe = 0;

    @Override
    public void preStripeWrite(OrcFile.WriterContext context
    ) throws IOException {
      lastKey.append(lastTransaction);
      lastKey.append(',');
      lastKey.append(lastBucket);
      lastKey.append(',');
      lastKey.append(lastRowId);
      lastKey.append(';');
      numKeysCurrentStripe = 0;
    }

    @Override
    public void preFooterWrite(OrcFile.WriterContext context
                               ) throws IOException {
      if(numKeysCurrentStripe > 0) {
        preStripeWrite(context);
      }
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
      numKeysCurrentStripe++;
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
  private void restoreBucket(Integer currentBucket, int operation) {
    if(currentBucket != null) {
      setBucket(currentBucket, operation);
    }
  }
  private int setBucket(int bucketProperty, int operation) {
    assert operation == UPDATE_OPERATION || operation == DELETE_OPERATION;
    int currentBucketProperty = bucket.get();
    bucket.set(bucketProperty);
    return currentBucketProperty;
  }

  @Override
  public Path getUpdatedFilePath() {
    return path;
  }

  public Path getDeleteFilePath() {
    return deleteEventPath;
  }
}
