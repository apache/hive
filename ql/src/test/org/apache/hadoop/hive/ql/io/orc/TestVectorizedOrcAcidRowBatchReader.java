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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader.ColumnizedDeleteEventRegistry;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader.SortMergedDeleteEventRegistry;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * This class tests the VectorizedOrcAcidRowBatchReader by creating an actual split and a set
 * of delete delta files. The split is on an insert delta and there are multiple delete deltas
 * with interleaving list of record ids that get deleted. Correctness is tested by validating
 * that the correct set of record ids are returned in sorted order for valid transactions only.
 */
public class TestVectorizedOrcAcidRowBatchReader {

  private static final long NUM_ROWID_PER_OWID = 15000L;
  private static final long NUM_OWID = 10L;
  private JobConf conf;
  private FileSystem fs;
  private Path root;
  private ObjectInspector inspector;
  private ObjectInspector originalInspector;
  private ObjectInspector bigRowInspector;
  private ObjectInspector bigOriginalRowInspector;

  public static class DummyRow {
    LongWritable field;
    RecordIdentifier ROW__ID;

    DummyRow(long val) {
      field = new LongWritable(val);
      ROW__ID = null;
    }

    DummyRow(long val, long rowId, long origTxn, int bucket) {
      field = new LongWritable(val);
      bucket = BucketCodec.V1.encode(new AcidOutputFormat.Options(null).bucket(bucket));
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

    static String getColumnNamesProperty() {
      return "field";
    }
    static String getColumnTypesProperty() {
      return "bigint";
    }

  }

  /**
   * Dummy row for original files.
   */
  public static class DummyOriginalRow {
    LongWritable field;

    DummyOriginalRow(long val) {
      field = new LongWritable(val);
    }

    static String getColumnNamesProperty() {
      return "field";
    }
    static String getColumnTypesProperty() {
      return "bigint";
    }
  }

  /**
   * A larger Dummy row that can be used to write multiple stripes.
   */
  public static class BigRow {
    BytesWritable field;
    RecordIdentifier rowId;

    BigRow(byte[] val) {
      field = new BytesWritable(val);
    }

    BigRow(byte[] val, long rowId, long origTxn, int bucket) {
      field = new BytesWritable(val);
      bucket = BucketCodec.V1.encode(new AcidOutputFormat.Options(null).bucket(bucket));
      this.rowId = new RecordIdentifier(origTxn, bucket, rowId);
    }

    static String getColumnNamesProperty() {
      return "field";
    }
    static String getColumnTypesProperty() {
      return "binary";
    }
  }

  /**
   * A larger Dummy row for original files that can be used to write multiple stripes.
   */
  public static class BigOriginalRow {
    BytesWritable field;

    BigOriginalRow(byte[] val) {
      field = new BytesWritable(val);
    }

    static String getColumnNamesProperty() {
      return "field";
    }
    static String getColumnTypesProperty() {
      return "binary";
    }
  }

  @Before
  public void setup() throws Exception {
    conf = new JobConf();
    conf.set(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    conf.setBoolean(HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN.varname, true);
    conf.set(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "default");
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, DummyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, DummyRow.getColumnTypesProperty());
    conf.setBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, true);
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "BI");
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(conf, 1);

    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    root = new Path(workDir, "TestVectorizedOrcAcidRowBatch.testDump");
    fs = root.getFileSystem(conf);
    root = fs.makeQualified(root);
    fs.delete(root, true);
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (DummyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

      originalInspector = ObjectInspectorFactory.getReflectionObjectInspector(DummyOriginalRow.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

      bigRowInspector = ObjectInspectorFactory.getReflectionObjectInspector(BigRow.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      bigOriginalRowInspector = ObjectInspectorFactory.getReflectionObjectInspector(BigOriginalRow.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
  }
  @Test
  public void testDeleteEventFilteringOff() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventFiltering();
  }
  @Test
  public void testDeleteEventFilteringOn() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventFiltering();
  }

  /**
   * Tests that we can figure out min/max ROW__ID for each split and then use
   * that to only load delete events between min/max.
   * This test doesn't actually check what is read - that is done more E2E
   * unit tests.
   * @throws Exception
   */
  private void testDeleteEventFiltering() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);
    int bucket = 0;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumWriteId(1)
        .maximumWriteId(1)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);

    int bucketProperty = BucketCodec.V1.encode(options);

    //create 3 insert deltas so that we have 3 splits
    RecordUpdater updater = new OrcRecordUpdater(root, options);

    //In the first delta add 2000 recs to simulate recs in multiple stripes.
    int numRows = 2000;
    for (int i = 1; i <= numRows; i++) {
      updater.insert(options.getMinimumWriteId(),
              new DummyRow(i, i-1, options.getMinimumWriteId(), bucket));
    }
    updater.close(false);

    options.minimumWriteId(2)
        .maximumWriteId(2);
    updater = new OrcRecordUpdater(root, options);
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(4, 0, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(5, 1, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(6, 2, options.getMinimumWriteId(), bucket));
    updater.close(false);

    options.minimumWriteId(3)
        .maximumWriteId(3);
    updater = new OrcRecordUpdater(root, options);
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(7, 0, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(8, 1, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(9, 2, options.getMinimumWriteId(), bucket));
    updater.close(false);

    //delete 1 row from each of the insert deltas
    options.minimumWriteId(4)
        .maximumWriteId(4);
    updater = new OrcRecordUpdater(root, options);
    updater.delete(options.getMinimumWriteId(),
        new DummyRow(-1, 0, 1, bucket));
    updater.delete(options.getMinimumWriteId(),
        new DummyRow(-1, 1, 2, bucket));
    updater.delete(options.getMinimumWriteId(),
        new DummyRow(-1, 2, 3, bucket));
    updater.close(false);

    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    //HWM is not important - just make sure deltas created above are read as
    // if committed
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY,
        "tbl:5:" + Long.MAX_VALUE + "::");

    //now we have 3 delete events total, but for each split we should only
    // load 1 into DeleteRegistry (if filtering is on)
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();

    assertEquals(3, splits.size());
    assertEquals(root.toUri().toString() + File.separator +
            "delta_0000001_0000001_0000/bucket_00000",
        splits.get(0).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());

    assertEquals(root.toUri().toString() + File.separator +
            "delta_0000002_0000002_0000/bucket_00000",
        splits.get(1).getPath().toUri().toString());
    assertFalse(splits.get(1).isOriginal());

    assertEquals(root.toUri().toString() + File.separator +
            "delta_0000003_0000003_0000/bucket_00000",
        splits.get(2).getPath().toUri().toString());
    assertFalse(splits.get(2).isOriginal());

    VectorizedOrcAcidRowBatchReader vectorizedReader =
        new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL,
            new VectorizedRowBatchCtx());
    ColumnizedDeleteEventRegistry deleteEventRegistry =
        (ColumnizedDeleteEventRegistry) vectorizedReader
            .getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 1", filterOn ? 1 : 3,
        deleteEventRegistry.size());
    OrcRawRecordMerger.KeyInterval keyInterval =
        vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
          new RecordIdentifier(1, bucketProperty, 0),
          new RecordIdentifier(1, bucketProperty, numRows - 1)),
          keyInterval);
    }
    else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }

    vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(1), conf,
        Reporter.NULL, new VectorizedRowBatchCtx());
    deleteEventRegistry = (ColumnizedDeleteEventRegistry) vectorizedReader
        .getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 2", filterOn ? 1 : 3,
        deleteEventRegistry.size());
    keyInterval = vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
          new RecordIdentifier(2, bucketProperty, 0),
          new RecordIdentifier(2, bucketProperty, 2)),
          keyInterval);
    }
    else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }

    vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(2), conf,
        Reporter.NULL, new VectorizedRowBatchCtx());
    deleteEventRegistry = (ColumnizedDeleteEventRegistry) vectorizedReader
        .getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 3", filterOn ? 1 : 3,
        deleteEventRegistry.size());
    keyInterval = vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
          new RecordIdentifier(3, bucketProperty, 0),
          new RecordIdentifier(3, bucketProperty, 2)), keyInterval);
    }
    else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }
  }

  @Test
  public void testDeleteEventFilteringOff2() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventFiltering2();
  }
  @Test
  public void testDeleteEventFilteringOn2() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventFiltering2();
  }
  @Test
  public void testDeleteEventFilteringOnWithoutIdx2() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TEST_MODE_ACID_KEY_IDX_SKIP, true);
    testDeleteEventFiltering2();
  }
  @Test
  public void testDeleteEventFilteringOnWithoutIdx3() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TEST_MODE_ACID_KEY_IDX_SKIP, true);
    conf.set("orc.stripe.size", "1000");
    testDeleteEventFiltering();
  }

  private void testDeleteEventFiltering2() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);
    boolean skipKeyIdx =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TEST_MODE_ACID_KEY_IDX_SKIP);
    int bucket = 1;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(true)
        .minimumWriteId(10000002)
        .maximumWriteId(10000002)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);

    int bucketProperty = BucketCodec.V1.encode(options);

    //create data that looks like a compacted base that includes some data
    //from 'original' files and some from native Acid write
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(0, new DummyRow(1, 0, 0, bucket));
    updater.insert(0, new DummyRow(1, 1, 0, bucket));
    updater.insert(0, new DummyRow(2, 2, 0, bucket));
    updater.insert(10000001, new DummyRow(3, 0, 10000001, bucket));
    updater.close(false);

    //delete 3rd row
    options.writingBase(false).minimumWriteId(10000004)
        .maximumWriteId(10000004);
    updater = new OrcRecordUpdater(root, options);
    updater.delete(options.getMinimumWriteId(),
        new DummyRow(-1, 0, 0, bucket));
    //hypothetically this matches something in (nonexistent here)
    // delta_10000003_10000003
    updater.delete(options.getMinimumWriteId(),
        new DummyRow(-1, 5, 10000003, bucket));
    updater.close(false);

    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    //HWM is not important - just make sure deltas created above are read as
    // if committed
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY,
        "tbl:10000005:" + Long.MAX_VALUE + "::");

    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();

    assertEquals(1, splits.size());
    assertEquals(root.toUri().toString() + File.separator +
            "base_10000002/bucket_00001",
        splits.get(0).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());


    VectorizedOrcAcidRowBatchReader vectorizedReader =
        new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL,
            new VectorizedRowBatchCtx());
    ColumnizedDeleteEventRegistry deleteEventRegistry =
        (ColumnizedDeleteEventRegistry) vectorizedReader
            .getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 1", filterOn ? 1 : 2,
        deleteEventRegistry.size());
    OrcRawRecordMerger.KeyInterval keyInterval =
        vectorizedReader.getKeyInterval();
    SearchArgument sarg = vectorizedReader.getDeleteEventSarg();
    if(filterOn) {
      if (skipKeyIdx) {
        // If key index is not present, the min max key interval uses stripe stats instead
        assertEquals(new OrcRawRecordMerger.KeyInterval(
                new RecordIdentifier(0, bucketProperty, 0),
                new RecordIdentifier(10000001, bucketProperty, 2)),
            keyInterval);
      } else {
        assertEquals(new OrcRawRecordMerger.KeyInterval(
                new RecordIdentifier(0, bucketProperty, 0),
                new RecordIdentifier(10000001, bucketProperty, 0)),
            keyInterval);
      }
      //key point is that in leaf-5 is (rowId <= 2) even though maxKey has
      //rowId 0.  more in VectorizedOrcAcidRowBatchReader.findMinMaxKeys
      assertEquals( "leaf-0 = (LESS_THAN originalTransaction 0)," +
          " leaf-1 = (LESS_THAN bucket 536936448)," +
          " leaf-2 = (LESS_THAN rowId 0)," +
          " leaf-3 = (LESS_THAN_EQUALS originalTransaction 10000001)," +
          " leaf-4 = (LESS_THAN_EQUALS bucket 536936448)," +
          " leaf-5 = (LESS_THAN_EQUALS rowId 2)," +
          " expr = (and (not leaf-0) (not leaf-1) " +
          "(not leaf-2) leaf-3 leaf-4 leaf-5)",
          ((SearchArgumentImpl) sarg).toOldString());
    }
    else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
      assertNull(sarg);
    }

  }

  @Test
  public void testDeleteEventFilteringOff3() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventFiltering3();
  }

  @Test
  public void testDeleteEventFilteringOn3() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventFiltering3();
  }

  @Test
  public void testWithoutStatsDeleteEventFilteringOn3() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    OrcConf.ROW_INDEX_STRIDE.setLong(conf, 0);
    testDeleteEventFiltering3();
  }

  private void testDeleteEventFiltering3() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);
    boolean columnStatsPresent = OrcConf.ROW_INDEX_STRIDE.getLong(conf) != 0;

    // To create small stripes
    OrcConf.STRIPE_SIZE.setLong(conf, 1);
    // Need to use a bigger row than DummyRow for the writer to flush the stripes
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, BigRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, BigRow.getColumnTypesProperty());

    // Use OrcRecordUpdater.OrcOptions to set the batch size.
    OrcRecordUpdater.OrcOptions orcOptions = new OrcRecordUpdater.OrcOptions(conf);
    orcOptions.orcOptions(OrcFile.writerOptions(conf).batchSize(1));

    int bucket = 1;

    AcidOutputFormat.Options options = orcOptions.filesystem(fs)
        .bucket(bucket)
        .writingBase(true)
        .minimumWriteId(10000002)
        .maximumWriteId(10000002)
        .inspector(bigRowInspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);

    int bucketProperty = BucketCodec.V1.encode(options);

    // Create 3 stripes with 1 row each
    byte[] data = new byte[1000];
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(10000002, new BigRow(data, 0, 0, bucket));
    updater.insert(10000002, new BigRow(data, 1, 0, bucket));
    updater.insert(10000002, new BigRow(data, 2, 0, bucket));
    updater.close(false);

    String acidFile = "base_10000002/bucket_00001";
    Path acidFilePath = new Path(root, acidFile);

    Reader reader = OrcFile.createReader(acidFilePath, OrcFile.readerOptions(conf));

    List<StripeInformation> stripes = reader.getStripes();

    // Make sure 3 stripes are created
    assertEquals(3, stripes.size());

    long fileLength = fs.getFileStatus(acidFilePath).getLen();

    // 1. Splits within a stripe
    // A split that's completely within the 2nd stripe
    StripeInformation stripe = stripes.get(1);
    OrcSplit split = new OrcSplit(acidFilePath, null,
        stripe.getOffset() + 50,
        stripe.getLength() - 100,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    validateKeyInterval(split, new RecordIdentifier(1, 1, 1),
        new RecordIdentifier(0, 0, 0), filterOn);

    // A split that's completely within the last stripe
    stripe = stripes.get(2);
    split = new OrcSplit(acidFilePath, null,
        stripe.getOffset() + 50,
        stripe.getLength() - 100,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    validateKeyInterval(split, new RecordIdentifier(1, 1, 1),
        new RecordIdentifier(0, 0, 0), filterOn);

    // 2. Splits starting at a stripe boundary
    // A split that starts where the 1st stripe starts and ends before the 1st stripe ends
    stripe = stripes.get(0);
    split = new OrcSplit(acidFilePath, null,
        stripe.getOffset(),
        stripe.getLength() - 50,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    // The key interval for the 1st stripe
    if (columnStatsPresent) {
      validateKeyInterval(split, new RecordIdentifier(10000002, bucketProperty, 0),
          new RecordIdentifier(10000002, bucketProperty, 0), filterOn);
    } else {
      validateKeyInterval(split, null, new RecordIdentifier(10000002, bucketProperty, 0), filterOn);
    }

    // A split that starts where the 2nd stripe starts and ends after the 2nd stripe ends
    stripe = stripes.get(1);
    split = new OrcSplit(acidFilePath, null,
        stripe.getOffset(),
        stripe.getLength() + 50,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    // The key interval for the last 2 stripes
    validateKeyInterval(split, new RecordIdentifier(10000002, bucketProperty, 1),
        new RecordIdentifier(10000002, bucketProperty, 2), filterOn);

    // 3. Splits ending at a stripe boundary
    // A split that starts before the last stripe starts and ends at the last stripe boundary
    stripe = stripes.get(2);
    split = new OrcSplit(acidFilePath, null,
        stripe.getOffset() - 50,
        stripe.getLength() + 50,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    // The key interval for the last stripe
    validateKeyInterval(split, new RecordIdentifier(10000002, bucketProperty, 2),
        new RecordIdentifier(10000002, bucketProperty, 2), filterOn);

    // A split that starts after the 1st stripe starts and ends where the last stripe ends
    split = new OrcSplit(acidFilePath, null,
        stripes.get(0).getOffset() + 50,
        reader.getContentLength() - 50,
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    // The key interval for the last 2 stripes
    validateKeyInterval(split, new RecordIdentifier(10000002, bucketProperty, 1),
        new RecordIdentifier(10000002, bucketProperty, 2), filterOn);

    // A split that starts where the 1st stripe starts and ends where the last stripe ends
    split = new OrcSplit(acidFilePath, null,
        stripes.get(0).getOffset(),
        reader.getContentLength(),
        new String[] {"localhost"}, null, false, true, getDeltaMetaDataWithBucketFile(1),
        fileLength, fileLength, root, null);

    // The key interval for all 3 stripes
    if (columnStatsPresent) {
      validateKeyInterval(split, new RecordIdentifier(10000002, bucketProperty, 0),
          new RecordIdentifier(10000002, bucketProperty, 2), filterOn);
    } else {
      validateKeyInterval(split, null, new RecordIdentifier(10000002, bucketProperty, 2), filterOn);
    }
  }

  private void validateKeyInterval(OrcSplit split, RecordIdentifier lowKey, RecordIdentifier highKey, boolean filterOn)
      throws Exception {
    VectorizedOrcAcidRowBatchReader vectorizedReader =
        new VectorizedOrcAcidRowBatchReader(split, conf, Reporter.NULL, new VectorizedRowBatchCtx());

    OrcRawRecordMerger.KeyInterval keyInterval =
        vectorizedReader.getKeyInterval();
    SearchArgument sarg = vectorizedReader.getDeleteEventSarg();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(lowKey, highKey), keyInterval);
    } else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
      assertNull(sarg);
    }
  }

  @Test
  public void testDeleteEventOriginalFilteringOn() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventOriginalFiltering();
  }

  @Test
  public void testDeleteEventOriginalFilteringOff() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventOriginalFiltering();
  }

  public void testDeleteEventOriginalFiltering() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);

    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, false);

    // Create 3 original files with 3 rows each
    Properties properties = new Properties();
    properties.setProperty("columns", DummyOriginalRow.getColumnNamesProperty());
    properties.setProperty("columns.types", DummyOriginalRow.getColumnTypesProperty());

    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(properties, conf);
    writerOptions.inspector(originalInspector);

    Path testFilePath = new Path(root, "000000_0");
    Writer writer = OrcFile.createWriter(testFilePath, writerOptions);

    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.close();

    testFilePath = new Path(root, "000000_0_copy_1");

    writer = OrcFile.createWriter(testFilePath, writerOptions);
    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.close();

    testFilePath = new Path(root, "000000_0_copy_2");

    writer = OrcFile.createWriter(testFilePath, writerOptions);
    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.addRow(new DummyOriginalRow(0));
    writer.close();

    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());

    int bucket = 0;

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumWriteId(1)
        .maximumWriteId(1)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);

    int bucketProperty = BucketCodec.V1.encode(options);

    RecordUpdater updater = new OrcRecordUpdater(root, options);

    //delete 1 row from each of the original files
    // Delete the last record in this split to test boundary conditions. It should not be present in the delete event
    // registry for the next split
    updater.delete(options.getMinimumWriteId(), new DummyRow(-1, 2, 0, bucket));
    // Delete the first record in this split to test boundary conditions. It should not be present in the delete event
    // registry for the previous split
    updater.delete(options.getMinimumWriteId(), new DummyRow(-1, 3, 0, bucket));
    updater.delete(options.getMinimumWriteId(), new DummyRow(-1, 7, 0, bucket));
    updater.close(false);

    //HWM is not important - just make sure deltas created above are read as if committed
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, "tbl:2:" + Long.MAX_VALUE + "::");

    // Set vector mode to true int the map work so that we recognize this as a vector mode execution during the split
    // generation. Without this we will not compute the offset for the synthetic row ids.
    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    VectorizedRowBatchCtx vrbContext = new VectorizedRowBatchCtx();
    mapWork.setVectorizedRowBatchCtx(vrbContext);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Utilities.setMapWork(conf, mapWork);

    // now we have 3 delete events total, but for each split we should only
    // load 1 into DeleteRegistry (if filtering is on)
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();

    assertEquals(3, splits.size());
    assertEquals(root.toUri().toString() + File.separator + "000000_0",
        splits.get(0).getPath().toUri().toString());
    assertTrue(splits.get(0).isOriginal());

    assertEquals(root.toUri().toString() + File.separator + "000000_0_copy_1",
        splits.get(1).getPath().toUri().toString());
    assertTrue(splits.get(1).isOriginal());

    assertEquals(root.toUri().toString() + File.separator + "000000_0_copy_2",
        splits.get(2).getPath().toUri().toString());
    assertTrue(splits.get(2).isOriginal());

    VectorizedOrcAcidRowBatchReader vectorizedReader =
        new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL, vrbContext);
    ColumnizedDeleteEventRegistry deleteEventRegistry =
        (ColumnizedDeleteEventRegistry) vectorizedReader.getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 1", filterOn ? 1 : 3, deleteEventRegistry.size());
    OrcRawRecordMerger.KeyInterval keyInterval = vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
              new RecordIdentifier(0, bucketProperty, 0),
              new RecordIdentifier(0, bucketProperty, 2)),
          keyInterval);
    } else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }

    vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(1), conf, Reporter.NULL, vrbContext);
    deleteEventRegistry = (ColumnizedDeleteEventRegistry) vectorizedReader.getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 2", filterOn ? 1 : 3, deleteEventRegistry.size());
    keyInterval = vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
              new RecordIdentifier(0, bucketProperty, 3),
              new RecordIdentifier(0, bucketProperty, 5)),
          keyInterval);
    } else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }

    vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(2), conf, Reporter.NULL, vrbContext);
    deleteEventRegistry = (ColumnizedDeleteEventRegistry) vectorizedReader.getDeleteEventRegistry();
    assertEquals("number of delete events for stripe 3", filterOn ? 1 : 3, deleteEventRegistry.size());
    keyInterval = vectorizedReader.getKeyInterval();
    if(filterOn) {
      assertEquals(new OrcRawRecordMerger.KeyInterval(
          new RecordIdentifier(0, bucketProperty, 6),
          new RecordIdentifier(0, bucketProperty, 8)), keyInterval);
    } else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
    }
  }

  @Test
  public void testDeleteEventOriginalFilteringOff2() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, false);
    testDeleteEventOriginalFiltering2();
  }

  @Test
  public void testDeleteEventOriginalFilteringOn2() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS, true);
    testDeleteEventOriginalFiltering2();
  }

  private void testDeleteEventOriginalFiltering2() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);

    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, false);

    // Need to use a bigger row than DummyRow for the writer to flush the stripes
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, BigRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, BigRow.getColumnTypesProperty());

    Properties properties = new Properties();

    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(properties, conf);
    writerOptions.inspector(bigOriginalRowInspector)
        .stripeSize(1)
        .batchSize(1);

    String originalFile = "000000_0";
    Path originalFilePath = new Path(root, originalFile);

    byte[] data = new byte[1000];
    Writer writer = OrcFile.createWriter(originalFilePath, writerOptions);
    writer.addRow(new BigOriginalRow(data));
    writer.addRow(new BigOriginalRow(data));
    writer.addRow(new BigOriginalRow(data));
    writer.close();

    Reader reader = OrcFile.createReader(originalFilePath, OrcFile.readerOptions(conf));

    List<StripeInformation> stripes = reader.getStripes();

    // Make sure 3 stripes are created
    assertEquals(3, stripes.size());

    FileStatus fileStatus = fs.getFileStatus(originalFilePath);
    long fileLength = fileStatus.getLen();

    // Set vector mode to true in the map work so that we can generate the syntheticProps
    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    VectorizedRowBatchCtx vrbContext = new VectorizedRowBatchCtx();
    mapWork.setVectorizedRowBatchCtx(vrbContext);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Utilities.setMapWork(conf, mapWork);

    OrcSplit.OffsetAndBucketProperty syntheticProps = VectorizedOrcAcidRowBatchReader.computeOffsetAndBucket(
        fileStatus, root, true, true, conf);

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .bucket(0);
    int bucketProperty = BucketCodec.V1.encode(options);

    // 1. Splits within a stripe
    // A split that's completely within the 2nd stripe
    StripeInformation stripe = stripes.get(1);
    OrcSplit split = new OrcSplit(originalFilePath, null,
        stripe.getOffset() + 50,
        stripe.getLength() - 100,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 2),
        new RecordIdentifier(0, bucketProperty, 1), filterOn);

    // A split that's completely within the last stripe
    stripe = stripes.get(2);
    split = new OrcSplit(originalFilePath, null,
        stripe.getOffset() + 50,
        stripe.getLength() - 100,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 3),
        new RecordIdentifier(0, bucketProperty, 2), filterOn);

    // 2. Splits starting at a stripe boundary
    // A split that starts where the 1st stripe starts and ends before the 1st stripe ends
    stripe = stripes.get(0);
    split = new OrcSplit(originalFilePath, null,
        stripe.getOffset(),
        stripe.getLength() - 50,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    // The key interval for the 1st stripe
    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 0),
        new RecordIdentifier(0, bucketProperty, 0), filterOn);

    // A split that starts where the 2nd stripe starts and ends after the 2nd stripe ends
    stripe = stripes.get(1);
    split = new OrcSplit(originalFilePath, null,
        stripe.getOffset(),
        stripe.getLength() + 50,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    // The key interval for the last 2 stripes
    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 1),
        new RecordIdentifier(0, bucketProperty, 2), filterOn);

    // 3. Splits ending at a stripe boundary
    // A split that starts before the last stripe starts and ends at the last stripe boundary
    stripe = stripes.get(2);
    split = new OrcSplit(originalFilePath, null,
        stripe.getOffset() - 50,
        stripe.getLength() + 50,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    // The key interval for the last stripe
    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 2),
        new RecordIdentifier(0, bucketProperty, 2), filterOn);

    // A split that starts after the 1st stripe starts and ends where the last stripe ends
    split = new OrcSplit(originalFilePath, null,
        stripes.get(0).getOffset() + 50,
        reader.getContentLength() - 50,
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    // The key interval for the last 2 stripes
    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 1),
        new RecordIdentifier(0, bucketProperty, 2), filterOn);

    // A split that starts where the 1st stripe starts and ends where the last stripe ends
    split = new OrcSplit(originalFilePath, null,
        stripes.get(0).getOffset(),
        reader.getContentLength(),
        new String[] {"localhost"}, null, true, true, getDeltaMetaDataWithBucketFile(0),
        fileLength, fileLength, root, syntheticProps);

    // The key interval for all 3 stripes
    validateKeyInterval(split, new RecordIdentifier(0, bucketProperty, 0),
        new RecordIdentifier(0, bucketProperty, 2), filterOn);
  }

  @Test
  public void testVectorizedOrcAcidRowBatchReader() throws Exception {
    setupTestData();

    testVectorizedOrcAcidRowBatchReader(ColumnizedDeleteEventRegistry.class.getName());

    // To test the SortMergedDeleteEventRegistry, we need to explicitly set the
    // HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY constant to a smaller value.
    int oldValue = conf.getInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000000);
    conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000);
    testVectorizedOrcAcidRowBatchReader(SortMergedDeleteEventRegistry.class.getName());

    // Restore the old value.
    conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, oldValue);
  }

  private void setupTestData() throws IOException {
    conf.set("bucket_count", "1");
      conf.set(ValidTxnList.VALID_TXNS_KEY,
          new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());

    int bucket = 0;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumWriteId(1)
        .maximumWriteId(NUM_OWID)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    // Create a single insert delta with 150,000 rows, with 15000 rowIds per original transaction id.
    for (long i = 1; i <= NUM_OWID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OWID; ++j) {
        long payload = (i-1) * NUM_ROWID_PER_OWID + j;
        updater.insert(i, new DummyRow(payload, j, i, bucket));
      }
    }
    updater.close(false);

    // Now create three types of delete deltas- first has rowIds divisible by 2 but not by 3,
    // second has rowIds divisible by 3 but not by 2, and the third has rowIds divisible by
    // both 2 and 3. This should produce delete deltas that will thoroughly test the sort-merge
    // logic when the delete events in the delete delta files interleave in the sort order.

    // Create a delete delta that has rowIds divisible by 2 but not by 3. This will produce
    // a delete delta file with 50,000 delete events.
    long currTxnId = NUM_OWID + 1;
    options.minimumWriteId(currTxnId).maximumWriteId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OWID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OWID; j += 1) {
        if (j % 2 == 0 && j % 3 != 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
    // Now, create a delete delta that has rowIds divisible by 3 but not by 2. This will produce
    // a delete delta file with 25,000 delete events.
    currTxnId = NUM_OWID + 2;
    options.minimumWriteId(currTxnId).maximumWriteId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OWID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OWID; j += 1) {
        if (j % 2 != 0 && j % 3 == 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
    // Now, create a delete delta that has rowIds divisible by both 3 and 2. This will produce
    // a delete delta file with 25,000 delete events.
    currTxnId = NUM_OWID + 3;
    options.minimumWriteId(currTxnId).maximumWriteId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OWID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OWID; j += 1) {
        if (j % 2 == 0 && j % 3 == 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
  }


  private void testVectorizedOrcAcidRowBatchReader(String deleteEventRegistry) throws Exception {
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)
        splitStrategies.get(0)).getSplits();
    assertEquals(1, splits.size());
    assertEquals(root.toUri().toString() + File.separator +
            "delta_0000001_0000010_0000/bucket_00000",
        splits.get(0).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());

    // Mark one of the transactions as an exception to test that invalid transactions
    // are being handled properly.
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, "tbl:14:1:1:5"); // Exclude transaction 5

    VectorizedOrcAcidRowBatchReader vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL, new VectorizedRowBatchCtx());
    if (deleteEventRegistry.equals(ColumnizedDeleteEventRegistry.class.getName())) {
      assertTrue(vectorizedReader.getDeleteEventRegistry() instanceof ColumnizedDeleteEventRegistry);
    }
    if (deleteEventRegistry.equals(SortMergedDeleteEventRegistry.class.getName())) {
      assertTrue(vectorizedReader.getDeleteEventRegistry() instanceof SortMergedDeleteEventRegistry);
    }
    TypeDescription schema = OrcInputFormat.getDesiredRowTypeDescr(conf, true, Integer.MAX_VALUE);
    VectorizedRowBatch vectorizedRowBatch = schema.createRowBatchV2();
    vectorizedRowBatch.setPartitionInfo(1, 0); // set data column count as 1.
    long previousPayload = Long.MIN_VALUE;
    while (vectorizedReader.next(null, vectorizedRowBatch)) {
      assertTrue(vectorizedRowBatch.selectedInUse);
      LongColumnVector col = (LongColumnVector) vectorizedRowBatch.cols[0];
      for (int i = 0; i < vectorizedRowBatch.size; ++i) {
        int idx = vectorizedRowBatch.selected[i];
        long payload = col.vector[idx];
        long owid = (payload / NUM_ROWID_PER_OWID) + 1;
        long rowId = payload % NUM_ROWID_PER_OWID;
        assertFalse(rowId % 2 == 0 || rowId % 3 == 0);
        assertTrue(owid != 5); // Check that writeid#5 has been excluded.
        assertTrue(payload > previousPayload); // Check that the data is in sorted order.
        previousPayload = payload;
      }
    }
  }

  @Test
  public void testFetchDeletedRowsUsingColumnizedDeleteEventRegistry() throws Exception {
    setupTestData();
    testFetchDeletedRows();
  }

  @Test
  public void testFetchDeletedRowsUsingSortMergedDeleteEventRegistry() throws Exception {
    setupTestData();

    // To test the SortMergedDeleteEventRegistry, we need to explicitly set the
    // HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY constant to a smaller value.
    int oldValue = conf.getInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000000);
    try {
      conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000);
      testFetchDeletedRows();
    }
    finally {
      // Restore the old value.
      conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, oldValue);
    }
  }

  private void testFetchDeletedRows() throws Exception {
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy) splitStrategies.get(0)).getSplits();

    // Mark one of the transactions as an exception to test that invalid transactions
    // are being handled properly.
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, "tbl:14:1:1:5"); // Exclude transaction 5

    // enable fetching deleted rows
    conf.set(Constants.ACID_FETCH_DELETED_ROWS, "true");

    // Project ROW__IS__DELETED
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx(
            new String[] { "payload", VirtualColumn.ROWISDELETED.getName() },
            new TypeInfo[] { TypeInfoFactory.longTypeInfo, VirtualColumn.ROWISDELETED.getTypeInfo() },
            new DataTypePhysicalVariation[] { DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE },
            new int[] { 0 }, 0, 1,
            new VirtualColumn[] { VirtualColumn.ROWISDELETED },
            new String[0],
            new DataTypePhysicalVariation[] { DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE });
    VectorizedOrcAcidRowBatchReader vectorizedReader =
            new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL, rbCtx);
    VectorizedRowBatch vectorizedRowBatch = rbCtx.createVectorizedRowBatch();
    vectorizedRowBatch.setPartitionInfo(1, 0); // set data column count as 1.
    long previousPayload = Long.MIN_VALUE;
    while (vectorizedReader.next(null, vectorizedRowBatch)) {
      LongColumnVector col = (LongColumnVector) vectorizedRowBatch.cols[0];
      LongColumnVector rowIsDeletedColumnVector = (LongColumnVector) vectorizedRowBatch.cols[1];
      for (int i = 0; i < vectorizedRowBatch.size; ++i) {
        int idx = vectorizedRowBatch.selected[i];
        long payload = col.vector[idx];
        long owid = (payload / NUM_ROWID_PER_OWID) + 1;
        long rowId = payload % NUM_ROWID_PER_OWID;
        if (rowId % 2 == 0 || rowId % 3 == 0) {
          assertEquals(1, rowIsDeletedColumnVector.vector[idx]);
        } else {
          assertEquals(0, rowIsDeletedColumnVector.vector[idx]);
        }
        assertTrue(owid != 5); // Check that writeid#5 has been excluded.
        assertTrue(payload >= previousPayload); // Check that the data is in sorted order.
        previousPayload = payload;
      }
    }
  }

  private List<OrcInputFormat.SplitStrategy<?>> getSplitStrategies() throws Exception {
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(
        context, () -> fs, root, false, null);
    Directory adi = gen.call();
    return OrcInputFormat.determineSplitStrategies(
        null, context, adi.getFs(), adi.getPath(), adi.getFiles(), adi.getDeleteDeltas(),
        null, null, true);

  }

  @Test
  public void testIsQualifiedDeleteDeltaForSplit() {
    // Original file
    checkPath("00000_0", "delete_delta_000012_000012_0000", true);
    checkPath("00000_0", "delete_delta_000001_000001", true);

    // Original copy
    checkPath("00000_0_copy", "delete_delta_0000012_0000012_0000", true);
    checkPath("00000_0_copy", "delete_delta_0000001_0000001", true);

    // Base file
    checkPath("base_00000002/bucket_0000001", "delete_delta_0000012_0000012_0000", true);

    // Compacted base file
    checkPath("base_0000002_v123/bucket_00000_0", "delete_delta_0000012_0000012_0000", true);

    // Delta file
    checkPath("delta_00000002_0000002/bucket_00001_1", "delete_delta_0000012_0000012_0000", true);
    checkPath("delta_00000002_0000002/bucket_00001_1", "delete_delta_0000002_0000002", false);
    checkPath("delta_00000002_0000002/bucket_00001_1", "delete_delta_0000001_0000001_0001", false);

    // Delta with statement id
    checkPath("delta_0000002_0000002_124/bucket_00001", "delete_delta_000012_000012_0000", true);
    checkPath("delta_0000002_0000002_124/bucket_00001", "delete_delta_000002_000002", false);
    checkPath("delta_0000002_0000002_124/bucket_00001", "delete_delta_000001_000001_0001", false);

    // Delta file with data loaded by LOAD DATA command
    checkPath("delta_0000002_0000002_0000/000000_0", "delete_delta_0000012_0000012_0000", true);
    checkPath("delta_0000002_0000002_0000/000000_0", "delete_delta_0000002_0000002", false);
    checkPath("delta_0000002_0000002_0000/000000_0", "delete_delta_0000001_0000001_0001", false);

    // Compacted delta
    checkPath("delta_0000002_0000005_124/bucket_00001", "delete_delta_0000012_0000012_0000", true);
    checkPath("delta_0000002_0000005_124/bucket_00001", "delete_delta_0000003_0000003", true);
    checkPath("delta_0000002_0000005_124/bucket_00001", "delete_delta_0000002_0000005", true);
    checkPath("delta_0000002_0000005_124/bucket_00001", "delete_delta_0000002_0000002", false);
    checkPath("delta_0000002_0000005_124/bucket_00001", "delete_delta_0000001_0000001_0001", false);

    // Multi statement transaction check
    checkPath("delta_0000002_0000002_0000/bucket_00001", "delete_delta_0000002_0000002_0000", false);
    checkPath("delta_0000002_0000002_0001/bucket_00001", "delete_delta_0000002_0000002_0000", false);
    checkPath("delta_0000002_0000002_0001/bucket_00001", "delete_delta_0000002_0000002_0002", true);
    checkPath("delta_0000002_0000002_0001/bucket_00001", "delete_delta_0000002_0000002", false);
    checkPath("delta_0000002_0000002/bucket_00001", "delete_delta_0000002_0000002", false);
    checkPath("delta_0000002_0000002/bucket_00001", "delete_delta_0000002_0000002_0001", true);
  }

  private void checkPath(String splitPath, String deleteDeltaPath, boolean expected) {
    String tableDir = "";//hdfs://localhost:59316/base/warehouse/acid_test/";
    AcidOutputFormat.Options ao = AcidUtils.parseBaseOrDeltaBucketFilename(new Path(tableDir + splitPath), conf);
    ParsedDeltaLight parsedDelta = ParsedDeltaLight.parse(new Path(tableDir + deleteDeltaPath));
    AcidInputFormat.DeltaMetaData deltaMetaData =
        new AcidInputFormat.DeltaMetaData(parsedDelta.getMinWriteId(), parsedDelta.getMaxWriteId(), new ArrayList<>(),
            parsedDelta.getVisibilityTxnId(), new ArrayList<>());
    Integer stmtId = null;
    if (parsedDelta.getStatementId() > -1) {
      deltaMetaData.getStmtIds().add(parsedDelta.getStatementId());
      stmtId = parsedDelta.getStatementId();
    }
    boolean result = VectorizedOrcAcidRowBatchReader.ColumnizedDeleteEventRegistry.isQualifiedDeleteDeltaForSplit(ao,
        deltaMetaData, stmtId);
    assertTrue(expected == result);

  }

  private List<AcidInputFormat.DeltaMetaData> getDeltaMetaDataWithBucketFile(int bucketId) {
    AcidInputFormat.DeltaFileMetaData file = new AcidInputFormat.DeltaFileMetaData(0, 0, null, null, null, bucketId);
    return Collections
        .singletonList(new AcidInputFormat.DeltaMetaData(0, 0, new ArrayList<>(), 0, Collections.singletonList(file)));
  }
}
