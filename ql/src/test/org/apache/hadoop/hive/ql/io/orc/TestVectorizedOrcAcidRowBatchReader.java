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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the VectorizedOrcAcidRowBatchReader by creating an actual split and a set
 * of delete delta files. The split is on an insert delta and there are multiple delete deltas
 * with interleaving list of record ids that get deleted. Correctness is tested by validating
 * that the correct set of record ids are returned in sorted order for valid transactions only.
 */
public class TestVectorizedOrcAcidRowBatchReader {

  private static final long NUM_ROWID_PER_OTID = 15000L;
  private static final long NUM_OTID = 10L;
  private JobConf conf;
  private FileSystem fs;
  private Path root;

  static class DummyRow {
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
    conf.set("bucket_count", "1");
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
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (DummyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bucket = 0;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumTransactionId(1)
        .maximumTransactionId(NUM_OTID)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    // Create a single insert delta with 150,000 rows, with 15000 rowIds per original transaction id.
    for (long i = 1; i <= NUM_OTID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OTID; ++j) {
        long payload = (i-1) * NUM_ROWID_PER_OTID + j;
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
    long currTxnId = NUM_OTID + 1;
    options.minimumTransactionId(currTxnId).maximumTransactionId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OTID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OTID; j += 1) {
        if (j % 2 == 0 && j % 3 != 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
    // Now, create a delete delta that has rowIds divisible by 3 but not by 2. This will produce
    // a delete delta file with 25,000 delete events.
    currTxnId = NUM_OTID + 2;
    options.minimumTransactionId(currTxnId).maximumTransactionId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OTID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OTID; j += 1) {
        if (j % 2 != 0 && j % 3 == 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
    // Now, create a delete delta that has rowIds divisible by both 3 and 2. This will produce
    // a delete delta file with 25,000 delete events.
    currTxnId = NUM_OTID + 3;
    options.minimumTransactionId(currTxnId).maximumTransactionId(currTxnId);
    updater = new OrcRecordUpdater(root, options);
    for (long i = 1; i <= NUM_OTID; ++i) {
      for (long j = 0; j < NUM_ROWID_PER_OTID; j += 1) {
        if (j % 2 == 0 && j % 3 == 0) {
          updater.delete(currTxnId, new DummyRow(-1, j, i, bucket));
        }
      }
    }
    updater.close(false);
  }

  private List<OrcSplit> getSplits() throws Exception {
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(context, fs, root, false, null);
    OrcInputFormat.AcidDirInfo adi = gen.call();
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = OrcInputFormat.determineSplitStrategies(
        null, context, adi.fs, adi.splitPath, adi.baseFiles, adi.deleteEvents,
        null, null, true);
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(1, splits.size());
    assertEquals(root.toUri().toString() + File.separator + "delta_0000001_0000010_0000/bucket_00000",
        splits.get(0).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());
    return splits;
  }

  @Test
  public void testVectorizedOrcAcidRowBatchReader() throws Exception {
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
    List<OrcSplit> splits = getSplits();
    // Mark one of the transactions as an exception to test that invalid transactions
    // are being handled properly.
    conf.set(ValidTxnList.VALID_TXNS_KEY, "14:1:1:5"); // Exclude transaction 5

    VectorizedOrcAcidRowBatchReader vectorizedReader = new VectorizedOrcAcidRowBatchReader(splits.get(0), conf, Reporter.NULL, new VectorizedRowBatchCtx());
    if (deleteEventRegistry.equals(ColumnizedDeleteEventRegistry.class.getName())) {
      assertTrue(vectorizedReader.getDeleteEventRegistry() instanceof ColumnizedDeleteEventRegistry);
    }
    if (deleteEventRegistry.equals(SortMergedDeleteEventRegistry.class.getName())) {
      assertTrue(vectorizedReader.getDeleteEventRegistry() instanceof SortMergedDeleteEventRegistry);
    }
    TypeDescription schema = OrcInputFormat.getDesiredRowTypeDescr(conf, true, Integer.MAX_VALUE);
    VectorizedRowBatch vectorizedRowBatch = schema.createRowBatch();
    vectorizedRowBatch.setPartitionInfo(1, 0); // set data column count as 1.
    long previousPayload = Long.MIN_VALUE;
    while (vectorizedReader.next(null, vectorizedRowBatch)) {
      assertTrue(vectorizedRowBatch.selectedInUse);
      LongColumnVector col = (LongColumnVector) vectorizedRowBatch.cols[0];
      for (int i = 0; i < vectorizedRowBatch.size; ++i) {
        int idx = vectorizedRowBatch.selected[i];
        long payload = col.vector[idx];
        long otid = (payload / NUM_ROWID_PER_OTID) + 1;
        long rowId = payload % NUM_ROWID_PER_OTID;
        assertFalse(rowId % 2 == 0 || rowId % 3 == 0);
        assertTrue(otid != 5); // Check that txn#5 has been excluded.
        assertTrue(payload > previousPayload); // Check that the data is in sorted order.
        previousPayload = payload;
      }
    }
  }
}
