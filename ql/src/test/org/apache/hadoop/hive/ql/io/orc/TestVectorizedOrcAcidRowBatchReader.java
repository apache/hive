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
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader.ColumnizedDeleteEventRegistry;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader.SortMergedDeleteEventRegistry;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
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

    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    root = new Path(workDir, "TestVectorizedOrcAcidRowBatch.testDump");
    fs = root.getFileSystem(conf);
    root = fs.makeQualified(root);
    fs.delete(root, true);
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (DummyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(1, 0, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(2, 1, options.getMinimumWriteId(), bucket));
    updater.insert(options.getMinimumWriteId(),
        new DummyRow(3, 2, options.getMinimumWriteId(), bucket));
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
          new RecordIdentifier(1, bucketProperty, 2)),
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

  private void testDeleteEventFiltering2() throws Exception {
    boolean filterOn =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.FILTER_DELETE_EVENTS);
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
      assertEquals(new OrcRawRecordMerger.KeyInterval(
              new RecordIdentifier(0, bucketProperty, 0),
              new RecordIdentifier(10000001, bucketProperty, 0)),
          keyInterval);
      //key point is that in leaf-5 is (rowId <= 2) even though maxKey has
      //rowId 0.  more in VectorizedOrcAcidRowBatchReader.findMinMaxKeys
      assertEquals( "leaf-0 = (LESS_THAN originalTransaction 0)," +
          " leaf-1 = (LESS_THAN bucket 536936448)," +
          " leaf-2 = (LESS_THAN rowId 0)," +
          " leaf-3 = (LESS_THAN_EQUALS originalTransaction 10000001)," +
          " leaf-4 = (LESS_THAN_EQUALS bucket 536936448)," +
          " leaf-5 = (LESS_THAN_EQUALS rowId 2)," +
          " expr = (and (not leaf-0) (not leaf-1) " +
          "(not leaf-2) leaf-3 leaf-4 leaf-5)", sarg.toString());
    }
    else {
      assertEquals(new OrcRawRecordMerger.KeyInterval(null, null), keyInterval);
      assertNull(sarg);
    }

  }
    @Test
  public void testVectorizedOrcAcidRowBatchReader() throws Exception {
    conf.set("bucket_count", "1");

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

    testVectorizedOrcAcidRowBatchReader(ColumnizedDeleteEventRegistry.class.getName());

    // To test the SortMergedDeleteEventRegistry, we need to explicitly set the
    // HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY constant to a smaller value.
    int oldValue = conf.getInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000000);
    conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, 1000);
    testVectorizedOrcAcidRowBatchReader(SortMergedDeleteEventRegistry.class.getName());

    // Restore the old value.
    conf.setInt(HiveConf.ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname, oldValue);
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
  private List<OrcInputFormat.SplitStrategy<?>> getSplitStrategies() throws Exception {
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(
        context, fs, root, false, null);
    OrcInputFormat.AcidDirInfo adi = gen.call();
    return OrcInputFormat.determineSplitStrategies(
        null, context, adi.fs, adi.splitPath, adi.baseFiles, adi.deleteEvents,
        null, null, true);

  }
}
