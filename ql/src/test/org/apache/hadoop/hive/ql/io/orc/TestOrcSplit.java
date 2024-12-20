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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for OrcSplit class
 */
public class TestOrcSplit {

  private JobConf conf;
  private FileSystem fs;
  private Path root;
  private ObjectInspector inspector;
  public static class DummyRow {
    LongWritable field;
    RecordIdentifier ROW__ID;

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
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(conf, 1);

    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    root = new Path(workDir, "TestOrcSplit.testDump");
    fs = root.getFileSystem(conf);
    root = fs.makeQualified(root);
    fs.delete(root, true);
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (DummyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

  /**
   * This test checks that a split filters out delete_delta folders which only have transactions that happened
   * in the past relative to the current split
   */
  @Test
  public void testDeleteDeltasFiltering() throws Exception {

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

    RecordUpdater updater = new OrcRecordUpdater(root, options);

    // Inserting a new record and then deleting it, 3 times.
    // Every insertion/deletion is in a separate transaction.
    // When reading, this will generate 3 splits, one per insert event.
    for (int i = 1; i <= 5; i = i + 2) {
      // Transaction that inserts one row with value i
      options.minimumWriteId(i)
              .maximumWriteId(i);
      updater = new OrcRecordUpdater(root, options);
      updater.insert(options.getMinimumWriteId(),
              new DummyRow(i, 0, options.getMinimumWriteId(), bucket));
      updater.close(false);

      // Transaction that deletes previously inserted row
      options.minimumWriteId(i+1)
              .maximumWriteId(i+1);
      updater = new OrcRecordUpdater(root, options);
      updater.delete(options.getMinimumWriteId(),
              new DummyRow(-1, 0, i, bucket));
      updater.close(false);
    }

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY,
            "tbl:6:" + Long.MAX_VALUE + "::");

    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = getSplitStrategies();
    assertEquals(1, splitStrategies.size());
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();

    assertEquals(3, splits.size());

    // Split for transaction #1 should include all 3 delete deltas
    assertEquals(3, splits.get(0).getDeltas().size());
    assertEquals(2, splits.get(0).getDeltas().get(0).getMinWriteId());
    assertEquals(2, splits.get(0).getDeltas().get(0).getMaxWriteId());
    assertEquals(4, splits.get(0).getDeltas().get(1).getMinWriteId());
    assertEquals(4, splits.get(0).getDeltas().get(1).getMaxWriteId());
    assertEquals(6, splits.get(0).getDeltas().get(2).getMinWriteId());
    assertEquals(6, splits.get(0).getDeltas().get(2).getMaxWriteId());

    // Split for transaction #3 should include only 2 deltas with transactions #4 & #6
    assertEquals(2, splits.get(1).getDeltas().size());
    assertEquals(4, splits.get(1).getDeltas().get(0).getMinWriteId());
    assertEquals(4, splits.get(1).getDeltas().get(0).getMaxWriteId());
    assertEquals(6, splits.get(1).getDeltas().get(1).getMinWriteId());
    assertEquals(6, splits.get(1).getDeltas().get(1).getMaxWriteId());

    // Split for transaction #5 should include only 1 deltas with transactions #6
    assertEquals(1, splits.get(2).getDeltas().size());
    assertEquals(6, splits.get(2).getDeltas().get(0).getMinWriteId());
    assertEquals(6, splits.get(2).getDeltas().get(0).getMaxWriteId());
  }
}
